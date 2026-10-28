package mx.com.leenustechs.ciaState.business.outbox;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.SendResult;

import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.AtomicStateUpdate;
import mx.com.leenustechs.ciaState.models.records.OutboxEvent;
import mx.com.leenustechs.ciaState.models.types.OperationType;

class OutboxPublisherTest {
    private final EventStateRepository repository = mock(EventStateRepository.class);
    private final KafkaProducerAdapter producer = mock(KafkaProducerAdapter.class);
    private final AtomicReference<EventStateEntity> durableState = new AtomicReference<>();
    private OutboxPublisher publisher;
    private OutboxEvent event;

    @BeforeEach
    void setup() {
        event = new OutboxEvent("stable-id", "worker-topic", "tx", new CommonModel("tx", "CIA_STATE", OperationType.LOGIN, null));
        EventStateEntity state = new EventStateEntity();
        state.setTransactionId("tx");
        state.setPendingOutbox(new LinkedHashMap<>(java.util.Map.of(event.id(), event)));
        state.setOutboxPending(true);
        durableState.set(state);
        when(repository.findWithPendingOutbox(anyInt(), nullable(String.class))).thenAnswer(call -> List.of(durableState.get()));
        when(repository.updateAtomically(eq("tx"), any())).thenAnswer(call -> {
            Function<Optional<EventStateEntity>, Optional<EventStateEntity>> mutation = call.getArgument(1);
            Optional<EventStateEntity> desired = mutation.apply(Optional.ofNullable(durableState.get()));
            desired.ifPresent(durableState::set);
            return new AtomicStateUpdate(desired.isPresent(), Optional.ofNullable(durableState.get()));
        });
        publisher = new OutboxPublisher(repository, producer, 10, 1);
    }

    @Test
    void acknowledgedSendRemovesOnlyItsOwnEvent() {
        OutboxEvent another = new OutboxEvent("other-id", "other", "tx", event.event());
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenAnswer(call -> {
            // Simulates a concurrent workflow transition while Kafka send is in flight.
            var updated = new LinkedHashMap<>(durableState.get().getPendingOutbox());
            updated.put(another.id(), another);
            durableState.get().setPendingOutbox(updated);
            return CompletableFuture.<SendResult<String, Object>>completedFuture(null);
        });
        publisher.publishPending();
        assertEquals(java.util.Set.of("other-id"), durableState.get().getPendingOutbox().keySet());
        assertTrue(durableState.get().isOutboxPending());
        verify(producer).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void brokerFailureSurvivesPublisherRestartAndRetriesSameIdentity() {
        when(producer.publish(anyString(), anyString(), any(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker down")))
                .thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        assertEquals(java.util.Set.of("stable-id"), durableState.get().getPendingOutbox().keySet());
        verify(repository, never()).updateAtomically(anyString(), any());
        new OutboxPublisher(repository, producer, 10, 1).publishPending();
        assertTrue(durableState.get().getPendingOutbox().isEmpty());
        assertFalse(durableState.get().isOutboxPending());
        verify(producer, times(2)).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void lostAcknowledgementAfterKafkaAcceptanceRepublishesSameIdentity() {
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        doThrow(new IllegalStateException("database unavailable")).when(repository).updateAtomically(anyString(), any());
        publisher.publishPending();
        assertTrue(durableState.get().isOutboxPending());
        publisher.publishPending();
        verify(producer, times(2)).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void sendTimeoutKeepsDurableMessage() {
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(new CompletableFuture<>());
        publisher.publishPending();
        assertTrue(durableState.get().isOutboxPending());
        verify(repository, never()).updateAtomically(anyString(), any());
    }

    @Test
    void interruptionPreservesInterruptAndDoesNotAcknowledge() {
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();
        try {
            publisher.publishPending();
            assertTrue(Thread.currentThread().isInterrupted());
            verify(repository, never()).updateAtomically(anyString(), any());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void scanFailureCanRecoverOnNextPoll() {
        when(repository.findWithPendingOutbox(anyInt(), nullable(String.class))).thenThrow(new IllegalStateException("database down"));
        assertDoesNotThrow(publisher::publishPending);
        verifyNoInteractions(producer);
    }

    @Test
    void emptyAndAlreadyAcknowledgedSnapshotsAreHarmless() {
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenAnswer(call -> {
            durableState.get().setPendingOutbox(new LinkedHashMap<>());
            durableState.get().setOutboxPending(false);
            return CompletableFuture.completedFuture(null);
        });
        publisher.publishPending();
        assertFalse(durableState.get().isOutboxPending());
        durableState.get().setPendingOutbox(null);
        publisher.publishPending();
        verify(producer, times(1)).publish(anyString(), anyString(), any(), anyString());
    }

    @Test
    void failingFirstPageDoesNotStarveLaterWorkflows() {
        EventStateEntity later = new EventStateEntity();
        later.setTransactionId("zz");
        later.setPendingOutbox(java.util.Map.of("later", new OutboxEvent("later", "other", "zz", event.event())));
        when(repository.findWithPendingOutbox(10, null)).thenReturn(List.of(durableState.get()));
        when(repository.findWithPendingOutbox(10, "tx")).thenReturn(List.of(later));
        when(repository.findWithPendingOutbox(10, "zz")).thenReturn(List.of());
        when(producer.publish(anyString(), anyString(), any(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")));
        publisher.publishPending();
        publisher.publishPending();
        publisher.publishPending();
        publisher.publishPending();
        verify(producer).publish("other", "zz", event.event(), "later");
        verify(producer, times(2)).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void persistedWorkflowResumesDeliveryWithNewPublisherInstance() {
        var memory = new mx.com.leenustechs.ciaState.support.MemoryEventStateRepository();
        var mapper = tools.jackson.databind.json.JsonMapper.builder().findAndAddModules().build();
        var service = new mx.com.leenustechs.ciaState.business.services.impl.EventStateServiceImpl(memory, mapper);
        var workflow = java.util.List.of(mx.com.leenustechs.ciaState.models.records.WorkflowStage.single(
                mx.com.leenustechs.ciaState.models.types.StepType.SECURITY));
        service.process(new CommonModel("tx", "CIA_API", OperationType.LOGIN, mapper.readTree("{}")), workflow);
        String persistedId = memory.findById("tx").orElseThrow().getPendingOutbox().keySet().iterator().next();
        when(producer.publish(anyString(), anyString(), any(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")))
                .thenReturn(CompletableFuture.completedFuture(null));
        new OutboxPublisher(memory, producer, 10, 1).publishPending();
        assertEquals(java.util.Set.of(persistedId), memory.findById("tx").orElseThrow().getPendingOutbox().keySet());
        new OutboxPublisher(memory, producer, 10, 1).publishPending();
        assertFalse(memory.findById("tx").orElseThrow().isOutboxPending());
        verify(producer, times(2)).publish(anyString(), eq("tx"), any(), eq(persistedId));
    }

    @Test
    void invalidLimitsFailBeforeScheduling() {
        assertThrows(IllegalArgumentException.class, () -> new OutboxPublisher(repository, producer, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new OutboxPublisher(repository, producer, 1, 0));
    }
}
