package mx.com.leenustechs.ciaState.business.services.impl;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.*;
import mx.com.leenustechs.ciaState.support.MemoryEventStateRepository;
import tools.jackson.databind.json.JsonMapper;

class WorkflowDeadlineTest {
    private static final Instant START = Instant.parse("2026-10-28T12:00:00Z");
    private final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private MemoryEventStateRepository repository;
    private MutableClock clock;
    private EventStateServiceImpl service;
    private final List<WorkflowStage> workflow = List.of(WorkflowStage.single(StepType.SECURITY),
            WorkflowStage.parallel(StepType.ASSETS, StepType.ROLES));

    @BeforeEach void setup() {
        repository = new MemoryEventStateRepository();
        clock = new MutableClock();
        service = new EventStateServiceImpl(repository, mapper, clock, 10);
    }
    private CommonModel event(String producer) {
        return new CommonModel("tx", producer, OperationType.LOGIN, mapper.createObjectNode());
    }
    private EventStateEntity state() { return repository.findById("tx").orElseThrow(); }
    private void start() { service.process(event("CIA_API"), workflow); }

    @Test void eachStageHasItsOwnDeadlineAndPartialResultsNeverExtendIt() {
        start();
        assertEquals(START.plusSeconds(10), state().getStageDeadlineAt());
        assertEquals(START, state().getPendingOutbox().values().iterator().next().enqueuedAt());
        clock.now = START.plusSeconds(9);
        service.process(event("SECURITY"), workflow);
        assertEquals(START.plusSeconds(19), state().getStageDeadlineAt());
        clock.now = START.plusSeconds(10);
        service.process(event("ASSETS"), workflow);
        assertEquals(START.plusSeconds(19), state().getStageDeadlineAt());
        clock.now = START.plusSeconds(18);
        assertFalse(service.reconcile("tx"));
        clock.now = START.plusSeconds(19);
        assertTrue(service.reconcile("tx"));
        EventStateEntity expired = state();
        assertEquals(TransactionStatus.FAILED, expired.getStatus());
        assertEquals(StepStatus.COMPLETE, expired.getSteps().get(StepType.ASSETS));
        assertEquals(StepStatus.ERROR, expired.getSteps().get(StepType.ROLES));
        assertNull(expired.getStageDeadlineAt());
        assertEquals("STAGE_TIMEOUT", expired.getResult().get("error_code").asText());
        assertEquals(1, expired.getPendingOutbox().size());
        var terminal = expired.getPendingOutbox().values().iterator().next();
        assertEquals(KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE, terminal.topic());
        assertEquals(EventOutcome.FAILURE, terminal.event().getOutcome());
        assertEquals(clock.now, terminal.enqueuedAt());
        assertFalse(service.reconcile("tx"));
        service.process(event("ROLES"), workflow);
        assertEquals(expired, state());
    }

    @Test void lateWorkerAndReplayedInitialRequestsCannotBeatAnExpiredDeadline() {
        start();
        clock.now = START.plusSeconds(10);
        assertEquals(TransactionStatus.FAILED, service.process(event("SECURITY"), workflow).getStatus());
        var expired = state();
        service.process(event("CIA_API"), workflow);
        assertEquals(expired, state());
        repository = new MemoryEventStateRepository();
        service = new EventStateServiceImpl(repository, mapper, clock, 10);
        start();
        clock.now = START.plusSeconds(20);
        assertEquals(TransactionStatus.FAILED, service.process(event("CIA_API"), workflow).getStatus());
    }

    @Test void legacyWorkflowsReceiveGraceFromDiscoveryInsteadOfFailingImmediately() {
        start();
        repository.updateAtomically("tx", current -> {
            var state = current.orElseThrow();
            state.setStageDeadlineAt(null);
            state.setUpdatedAt(Instant.EPOCH);
            return current;
        });
        assertTrue(service.reconcile("tx"));
        assertEquals(START.plusSeconds(10), state().getStageDeadlineAt());
        assertEquals(TransactionStatus.PROCESSING, state().getStatus());
        assertEquals(Instant.EPOCH, state().getUpdatedAt());
        assertFalse(service.reconcile("tx"));
        clock.now = START.plusSeconds(10);
        assertTrue(service.reconcile("tx"));
        assertEquals(TransactionStatus.FAILED, state().getStatus());
    }

    @Test void terminalAndMissingWorkflowsAreSafeAndStaleIndexEntriesAreCleared() {
        assertFalse(service.reconcile("missing"));
        start();
        service.process(event("SECURITY"), workflow);
        service.process(event("ASSETS"), workflow);
        service.process(event("ROLES"), workflow);
        assertEquals(TransactionStatus.COMPLETED, state().getStatus());
        assertNull(state().getStageDeadlineAt());
        assertFalse(service.reconcile("tx"));
        repository.updateAtomically("tx", current -> {
            current.orElseThrow().setStageDeadlineAt(START);
            return current;
        });
        assertTrue(service.reconcile("tx"));
        assertEquals(TransactionStatus.COMPLETED, state().getStatus());
        assertNull(state().getStageDeadlineAt());
    }

    @Test void multipleReconcilersExpireTheSameWorkflowOnlyOnce() throws Exception {
        start();
        clock.now = START.plusSeconds(10);
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            var tasks = IntStream.range(0, 20).<java.util.concurrent.Callable<Boolean>>mapToObj(
                    i -> () -> service.reconcile("tx")).toList();
            int changed = 0;
            for (var future : executor.invokeAll(tasks)) if (future.get()) changed++;
            assertEquals(1, changed);
        }
        assertEquals(1, state().getPendingOutbox().size());
        assertEquals(TransactionStatus.FAILED, state().getStatus());
    }

    @Test void invalidTimeoutsFailBeforeAnyWorkStarts() {
        assertThrows(IllegalArgumentException.class, () -> new EventStateServiceImpl(repository, mapper, clock, 0));
        assertThrows(IllegalArgumentException.class, () -> new EventStateServiceImpl(repository, mapper, clock, -1));
        assertThrows(IllegalArgumentException.class, () -> new EventStateServiceImpl(repository, mapper, clock, Long.MAX_VALUE));
    }

    private static final class MutableClock extends Clock {
        volatile Instant now = START;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
}
