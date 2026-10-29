package mx.com.leenustechs.ciaState.business.outbox;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;
import org.springframework.kafka.support.SendResult;

import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.*;
import mx.com.leenustechs.ciaState.models.types.*;
import tools.jackson.databind.json.JsonMapper;

class OutboxPublisherTest {
    private final EventStateRepository repository = mock(EventStateRepository.class);
    private final KafkaProducerAdapter producer = mock(KafkaProducerAdapter.class);
    private final AtomicReference<EventStateEntity> durableState = new AtomicReference<>();
    private final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private OutboxPublisher publisher;
    private OutboxEvent event;
    private boolean failWrites;

    @BeforeEach
    void setup() {
        CommonModel command = new CommonModel("tx", "CIA_STATE", OperationType.LOGIN, mapper.createObjectNode());
        command.setStageIndex(0);
        event = new OutboxEvent("stable-id", "worker-topic", "tx", command);
        EventStateEntity state = new EventStateEntity();
        state.setTransactionId("tx");
        state.setStatus(TransactionStatus.PROCESSING);
        state.setCurrentStage(0);
        state.setPendingOutbox(new LinkedHashMap<>(Map.of(event.id(), event)));
        state.setOutboxPending(true);
        durableState.set(copy(state));
        when(repository.scanPendingOutbox(anyInt(), nullable(String.class)))
                .thenAnswer(call -> page(List.of(copy(durableState.get())), "tx"));
        when(repository.updateAtomically(anyString(), any())).thenAnswer(this::applyMutation);
        publisher = new OutboxPublisher(repository, producer, 10, 1);
    }

    private EventStateEntity copy(EventStateEntity state) {
        return state == null ? null : mapper.convertValue(state, EventStateEntity.class);
    }

    private AtomicStateUpdate applyMutation(InvocationOnMock call) {
        Function<Optional<EventStateEntity>, Optional<EventStateEntity>> mutation = call.getArgument(1);
        Optional<EventStateEntity> desired = mutation.apply(Optional.ofNullable(copy(durableState.get())));
        if (desired.isPresent() && failWrites) throw new IllegalStateException("database unavailable");
        desired.ifPresent(durableState::set);
        return new AtomicStateUpdate(desired.isPresent(), Optional.ofNullable(copy(durableState.get())));
    }

    private OutboxPage page(List<EventStateEntity> rows, String cursor) {
        return new OutboxPage(rows, cursor, List.of());
    }

    @Test
    void acknowledgedSendRemovesOnlyItsOwnEvent() {
        OutboxEvent another = new OutboxEvent("other-id", "other", "tx", event.event());
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenAnswer(call -> {
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
        EventStateEntity before = copy(durableState.get());
        publisher.publishPending();
        assertEquals(before, durableState.get());
        new OutboxPublisher(repository, producer, 10, 1).publishPending();
        assertTrue(durableState.get().getPendingOutbox().isEmpty());
        assertFalse(durableState.get().isOutboxPending());
        verify(producer, times(2)).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void lostAcknowledgementAfterKafkaAcceptanceRepublishesSameIdentity() {
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        failWrites = true;
        publisher.publishPending();
        assertTrue(durableState.get().isOutboxPending());
        publisher.publishPending();
        verify(producer, times(2)).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void sendTimeoutKeepsDurableMessage() {
        var meters = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(repository, producer, 10, 1, meters, Clock.systemUTC());
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(new CompletableFuture<>());
        EventStateEntity before = copy(durableState.get());
        publisher.publishPending();
        assertEquals(before, durableState.get());
        assertEquals(1, meters.get("cia.outbox.delivery.failures").counter().count());
        assertEquals(1, meters.get("cia.outbox.delivery.timeouts").counter().count());
        assertEquals(1, meters.get("cia.outbox.delivery.duration").timer().count());
        assertEquals(1, meters.get("cia.outbox.poll.duration").timer().count());
    }

    @Test
    void interruptionPreservesInterruptAndDoesNotAcknowledge() {
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(new CompletableFuture<>());
        Thread.currentThread().interrupt();
        try {
            publisher.publishPending();
            assertTrue(Thread.currentThread().isInterrupted());
            assertTrue(durableState.get().isOutboxPending());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void scanFailureCanRecoverOnNextPoll() {
        when(repository.scanPendingOutbox(anyInt(), nullable(String.class))).thenThrow(new IllegalStateException("database down"));
        assertDoesNotThrow(publisher::publishPending);
        verifyNoInteractions(producer);
    }

    @Test
    void confirmationFailureDoesNotSendAnUnverifiedCommand() {
        doThrow(new IllegalStateException("database unavailable"))
                .when(repository).updateAtomically(anyString(), any());
        publisher.publishPending();
        verifyNoInteractions(producer);
        assertTrue(durableState.get().isOutboxPending());
    }

    @Test
    void staleSnapshotDoesNotRepublishAnAlreadyAcknowledgedOrMissingIntent() {
        var stale = page(List.of(copy(durableState.get())), "tx");
        when(repository.scanPendingOutbox(anyInt(), nullable(String.class))).thenReturn(stale);
        durableState.get().setPendingOutbox(new LinkedHashMap<>());
        publisher.publishPending();
        durableState.get().setPendingOutbox(null);
        publisher.publishPending();
        durableState.set(null);
        publisher.publishPending();
        verifyNoInteractions(producer);
    }

    @Test
    void emptyOutboxSnapshotsAreHarmless() {
        durableState.get().setPendingOutbox(null);
        publisher.publishPending();
        verifyNoInteractions(producer);
    }

    @Test
    void failingFirstPageDoesNotStarveLaterWorkflows() {
        EventStateEntity later = copy(durableState.get());
        later.setTransactionId("zz");
        later.setPendingOutbox(Map.of("later", new OutboxEvent("later", "other", "zz", event.event())));
        when(repository.scanPendingOutbox(10, null)).thenReturn(page(List.of(copy(durableState.get())), "tx"));
        when(repository.scanPendingOutbox(10, "tx")).thenAnswer(call -> { durableState.set(copy(later)); return page(List.of(later), "zz"); });
        when(repository.scanPendingOutbox(10, "zz")).thenReturn(page(List.of(), null));
        when(producer.publish(anyString(), anyString(), any(), anyString()))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("unavailable")));
        publisher.publishPending();
        publisher.publishPending();
        publisher.publishPending();
        verify(producer).publish("other", "zz", event.event(), "later");
        verify(producer).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void allCorruptPageAdvancesToHealthyWorkflowsAndWrapsAtEnd() {
        when(repository.scanPendingOutbox(10, null)).thenReturn(new OutboxPage(List.of(), "bad", List.of("bad")));
        when(repository.scanPendingOutbox(10, "bad")).thenReturn(page(List.of(copy(durableState.get())), "tx"));
        when(repository.scanPendingOutbox(10, "tx")).thenReturn(page(List.of(), null));
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        publisher.publishPending();
        publisher.publishPending();
        publisher.publishPending();
        verify(repository, times(2)).scanPendingOutbox(10, null);
        verify(producer).publish("worker-topic", "tx", event.event(), "stable-id");
    }

    @Test
    void terminalStateCancelsLegacyCommandsBeforeSendingFinalNotification() {
        durableState.get().setStatus(TransactionStatus.FAILED);
        OutboxEvent terminal = new OutboxEvent("final", KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE, "tx", event.event());
        durableState.get().getPendingOutbox().put(terminal.id(), terminal);
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        verify(producer).publish(terminal.topic(), "tx", terminal.event(), "final");
        verify(producer, never()).publish(eq("worker-topic"), anyString(), any(), anyString());
        assertFalse(durableState.get().isOutboxPending());
    }

    @Test
    void failureAfterScanCancelsEveryCommandInTheStaleSnapshot() {
        var stale = page(List.of(copy(durableState.get())), "tx");
        when(repository.scanPendingOutbox(anyInt(), nullable(String.class))).thenAnswer(call -> {
            durableState.get().setStatus(TransactionStatus.FAILED);
            durableState.get().setPendingOutbox(new LinkedHashMap<>());
            return stale;
        });
        publisher.publishPending();
        verifyNoInteractions(producer);
    }

    @Test
    void failureDuringSendDoesNotStartTheNextParallelCommand() {
        OutboxEvent sibling = new OutboxEvent("sibling", "sibling-topic", "tx", event.event());
        durableState.get().getPendingOutbox().put(sibling.id(), sibling);
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenAnswer(call -> {
            durableState.get().setStatus(TransactionStatus.FAILED);
            durableState.get().setPendingOutbox(new LinkedHashMap<>());
            return CompletableFuture.completedFuture(null);
        });
        publisher.publishPending();
        verify(producer, times(1)).publish(anyString(), anyString(), any(), anyString());
    }

    @Test
    void obsoleteStageCommandsAreRemovedButFutureAndPrematureFinalEventsAreNotSent() {
        durableState.get().setCurrentStage(1);
        publisher.publishPending();
        assertFalse(durableState.get().isOutboxPending());
        event.event().setStageIndex(2);
        durableState.get().setPendingOutbox(new LinkedHashMap<>(Map.of(event.id(), event)));
        publisher.publishPending();
        OutboxEvent premature = new OutboxEvent("final", KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE, "tx", event.event());
        durableState.get().setPendingOutbox(new LinkedHashMap<>(Map.of("final", premature)));
        publisher.publishPending();
        verifyNoInteractions(producer);
    }

    @Test
    void persistedWorkflowResumesDeliveryWithNewPublisherInstance() {
        var memory = new mx.com.leenustechs.ciaState.support.MemoryEventStateRepository();
        var service = new mx.com.leenustechs.ciaState.business.services.impl.EventStateServiceImpl(memory, mapper);
        var workflow = List.of(WorkflowStage.single(StepType.SECURITY));
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
    void successfulDeliveryReportsPageScopedBacklogAgeAndBoundedCardinalityMetrics() {
        Instant now = Instant.parse("2026-10-28T12:00:00Z");
        OutboxEvent timestamped = new OutboxEvent(event.id(), event.topic(), event.key(), event.event(), now.minusSeconds(120));
        durableState.get().setPendingOutbox(new LinkedHashMap<>(Map.of(event.id(), timestamped)));
        var meters = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(repository, producer, 10, 1, meters, Clock.fixed(now, ZoneOffset.UTC));
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        assertEquals(1, meters.get("cia.outbox.delivery.success").counter().count());
        assertEquals(0, meters.get("cia.outbox.delivery.failures").counter().count());
        assertEquals(1, meters.get("cia.outbox.delivery.duration").timer().count());
        assertEquals(1, meters.get("cia.outbox.poll.duration").timer().count());
        assertEquals(1, meters.get("cia.outbox.page.pending.events").tag("scope", "last_scan_page").gauge().value());
        assertEquals(1, meters.get("cia.outbox.page.pending.workflows").gauge().value());
        assertEquals(120, meters.get("cia.outbox.page.oldest.age").gauge().value());
        assertEquals(0, meters.get("cia.outbox.page.unknown.timestamps").gauge().value());
        assertTrue(durableState.get().getPendingOutbox().isEmpty());
        assertTrue(meters.getMeters().stream().flatMap(m -> m.getId().getTags().stream())
                .noneMatch(tag -> tag.getKey().matches(".*(transaction|event|topic).*")));
    }

    @Test
    void legacyTimestampsAreExplicitlyUnknownAndEmptyNextPageResetsSnapshotGauges() {
        var meters = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(repository, producer, 10, 1, meters, Clock.systemUTC());
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        assertEquals(1, meters.get("cia.outbox.page.unknown.timestamps").gauge().value());
        assertEquals(0, meters.get("cia.outbox.page.oldest.age").gauge().value());
        when(repository.scanPendingOutbox(anyInt(), nullable(String.class))).thenReturn(page(List.of(), null));
        publisher.publishPending();
        assertEquals(0, meters.get("cia.outbox.page.pending.events").gauge().value());
        assertEquals(0, meters.get("cia.outbox.page.pending.workflows").gauge().value());
        assertEquals(0, meters.get("cia.outbox.page.unknown.timestamps").gauge().value());
    }

    @Test
    void corruptRowsAndInfrastructureFailuresHaveSeparateCounters() {
        var meters = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(repository, producer, 10, 1, meters, Clock.systemUTC());
        when(repository.scanPendingOutbox(anyInt(), nullable(String.class)))
                .thenReturn(new OutboxPage(List.of(), "bad", List.of("bad-a", "bad-b")))
                .thenThrow(new IllegalStateException("database unavailable"));
        publisher.publishPending();
        publisher.publishPending();
        assertEquals(2, meters.get("cia.outbox.scan.rejected").counter().count());
        assertEquals(1, meters.get("cia.outbox.scan.failures").counter().count());
        assertEquals(0, meters.get("cia.outbox.delivery.failures").counter().count());
        assertEquals(2, meters.get("cia.outbox.poll.duration").timer().count());
    }

    @Test
    void wrappedBrokerTimeoutIsRecordedAsFailureAndTimeoutWithoutAcknowledgement() {
        var meters = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(repository, producer, 10, 1, meters, Clock.systemUTC());
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(
                CompletableFuture.failedFuture(new org.apache.kafka.common.errors.TimeoutException("unavailable")));
        publisher.publishPending();
        assertEquals(1, meters.get("cia.outbox.delivery.failures").counter().count());
        assertEquals(1, meters.get("cia.outbox.delivery.timeouts").counter().count());
        assertEquals(0, meters.get("cia.outbox.delivery.success").counter().count());
        assertTrue(durableState.get().isOutboxPending());
    }

    @Test
    void expiredStageCannotStartACommandButFinalNotificationRemainsDeliverable() {
        Instant now = Instant.parse("2026-10-28T12:00:00Z");
        durableState.get().setStageDeadlineAt(now);
        var meters = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(repository, producer, 10, 1, meters, Clock.fixed(now, ZoneOffset.UTC));
        publisher.publishPending();
        verifyNoInteractions(producer);
        assertTrue(durableState.get().isOutboxPending());
        assertEquals(0, meters.get("cia.outbox.delivery.failures").counter().count());
        durableState.get().setStatus(TransactionStatus.FAILED);
        OutboxEvent terminal = new OutboxEvent("final", KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE, "tx", event.event());
        durableState.get().setPendingOutbox(new LinkedHashMap<>(Map.of("final", terminal)));
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        verify(producer).publish(terminal.topic(), "tx", terminal.event(), "final");
    }

    @Test
    void unexpiredStageStillPublishesAndFutureTimestampAgeNeverBecomesNegative() {
        Instant now = Instant.parse("2026-10-28T12:00:00Z");
        durableState.get().setStageDeadlineAt(now.plusSeconds(1));
        OutboxEvent future = new OutboxEvent(event.id(), event.topic(), event.key(), event.event(), now.plusSeconds(30));
        durableState.get().setPendingOutbox(new LinkedHashMap<>(Map.of(event.id(), future)));
        var meters = new SimpleMeterRegistry();
        publisher = new OutboxPublisher(repository, producer, 10, 1, meters, Clock.fixed(now, ZoneOffset.UTC));
        when(producer.publish(anyString(), anyString(), any(), anyString())).thenReturn(CompletableFuture.completedFuture(null));
        publisher.publishPending();
        verify(producer).publish(event.topic(), "tx", event.event(), event.id());
        assertEquals(0, meters.get("cia.outbox.page.oldest.age").gauge().value());
    }

    @Test
    void invalidLimitsFailBeforeScheduling() {
        assertThrows(IllegalArgumentException.class, () -> new OutboxPublisher(repository, producer, 0, 1));
        assertThrows(IllegalArgumentException.class, () -> new OutboxPublisher(repository, producer, 1, 0));
    }
}
