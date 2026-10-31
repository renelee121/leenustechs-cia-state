package mx.com.leenustechs.ciaState.business.services.impl;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.*;
import java.util.List;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.*;
import mx.com.leenustechs.ciaState.models.types.*;
import mx.com.leenustechs.ciaState.support.MemoryEventStateRepository;
import tools.jackson.databind.json.JsonMapper;

class WorkflowReconcilerTest {
    private static final Instant NOW = Instant.parse("2026-10-28T12:00:00Z");
    private EventStateRepository repository;
    private EventStateService service;
    private MutableClock clock;
    private SimpleMeterRegistry metrics;
    private WorkflowReconciler reconciler;

    @BeforeEach void setup() {
        repository = mock(EventStateRepository.class);
        service = mock(EventStateService.class);
        clock = new MutableClock();
        metrics = new SimpleMeterRegistry();
        reconciler = new WorkflowReconciler(repository, service, clock, metrics, 10);
    }
    private EventStateEntity state(String id, Instant deadline) {
        var state = new EventStateEntity();
        state.setTransactionId(id);
        state.setStageDeadlineAt(deadline);
        return state;
    }

    @Test void dueScanAdvancesOverRejectedRowsWrapsAndContinuesAfterOneWorkflowFails() {
        DeadlineCursor cursor = new DeadlineCursor(NOW.toEpochMilli(), "bad");
        when(repository.scanExpiredWorkflows(NOW, 10, null)).thenReturn(
                new WorkflowDeadlinePage(List.of(state("failing", NOW), state("healthy", NOW), state("changed", NOW)), cursor, List.of("bad")));
        when(repository.scanExpiredWorkflows(NOW, 10, cursor)).thenReturn(new WorkflowDeadlinePage(List.of(), null, List.of()));
        when(service.reconcile("failing")).thenThrow(new IllegalStateException("unavailable"));
        when(service.reconcile("healthy")).thenReturn(false);
        when(service.reconcile("changed")).thenReturn(true);
        reconciler.expireStages();
        reconciler.expireStages();
        reconciler.expireStages();
        verify(service, times(2)).reconcile("healthy");
        verify(repository, times(2)).scanExpiredWorkflows(NOW, 10, null);
        assertEquals(2, metrics.get("cia.workflows.reconciliation").tags("scan", "expired", "result", "failed").counter().count());
        assertEquals(2, metrics.get("cia.workflows.reconciliation").tags("scan", "expired", "result", "changed").counter().count());
        assertEquals(2, metrics.get("cia.workflows.reconciliation.scan.rejected").tag("scan", "expired").counter().count());
    }

    @Test void backwardsClockMovementResetsTheDueCursor() {
        DeadlineCursor cursor = new DeadlineCursor(NOW.toEpochMilli(), "tx");
        when(repository.scanExpiredWorkflows(NOW, 10, null)).thenReturn(new WorkflowDeadlinePage(List.of(), cursor, List.of()));
        reconciler.expireStages();
        clock.now = NOW.minusSeconds(60);
        when(repository.scanExpiredWorkflows(clock.now, 10, null)).thenReturn(new WorkflowDeadlinePage(List.of(), null, List.of()));
        reconciler.expireStages();
        verify(repository).scanExpiredWorkflows(clock.now, 10, null);
    }

    @Test void legacyDiscoveryAssignsOnlyMissingDeadlinesAndAdvancesCorruptPages() {
        when(repository.scanProcessingWorkflows(10, null)).thenReturn(new WorkflowPage(List.of(), "bad", List.of("bad")));
        when(repository.scanProcessingWorkflows(10, "bad")).thenReturn(new WorkflowPage(
                List.of(state("legacy", null), state("modern", NOW.plusSeconds(10))), "modern", List.of()));
        when(repository.scanProcessingWorkflows(10, "modern")).thenReturn(new WorkflowPage(List.of(), null, List.of()));
        when(service.reconcile("legacy")).thenReturn(true);
        reconciler.initializeLegacyDeadlines();
        reconciler.initializeLegacyDeadlines();
        reconciler.initializeLegacyDeadlines();
        verify(service).reconcile("legacy");
        verify(service, never()).reconcile("modern");
        assertEquals(1, metrics.get("cia.workflows.reconciliation").tags("scan", "legacy", "result", "changed").counter().count());
        assertEquals(1, metrics.get("cia.workflows.reconciliation.scan.rejected").tag("scan", "legacy").counter().count());
    }

    @Test void unavailableScansAreVisibleAndDoNotPretendToReconcileAnything() {
        when(repository.scanExpiredWorkflows(any(), anyInt(), nullable(DeadlineCursor.class))).thenThrow(new IllegalStateException("offline"));
        when(repository.scanProcessingWorkflows(anyInt(), nullable(String.class))).thenThrow(new IllegalStateException("offline"));
        assertDoesNotThrow(reconciler::expireStages);
        assertDoesNotThrow(reconciler::initializeLegacyDeadlines);
        verifyNoInteractions(service);
        assertEquals(1, metrics.get("cia.workflows.reconciliation.scan.failures").tag("scan", "expired").counter().count());
        assertEquals(1, metrics.get("cia.workflows.reconciliation.scan.failures").tag("scan", "legacy").counter().count());
    }

    @Test void interruptedReconciliationDoesNotStartMoreWorkflowTransactions() {
        when(repository.scanExpiredWorkflows(NOW, 10, null)).thenReturn(new WorkflowDeadlinePage(List.of(state("due", NOW)), null, List.of()));
        when(repository.scanProcessingWorkflows(10, null)).thenReturn(new WorkflowPage(List.of(state("legacy", null)), null, List.of()));
        Thread.currentThread().interrupt();
        try {
            reconciler.expireStages();
            reconciler.initializeLegacyDeadlines();
            verifyNoInteractions(service);
            assertTrue(Thread.currentThread().isInterrupted());
        } finally { Thread.interrupted(); }
    }

    @Test void legacyGraceAndExpirySurviveAReconcilerRestart() {
        var memory = new MemoryEventStateRepository();
        var mapper = JsonMapper.builder().findAndAddModules().build();
        var engine = new EventStateServiceImpl(memory, mapper, clock, 10);
        engine.process(new CommonModel("tx", "CIA_API", OperationType.LOGIN, mapper.createObjectNode()),
                List.of(WorkflowStage.single(StepType.SECURITY)));
        memory.updateAtomically("tx", current -> { current.orElseThrow().setStageDeadlineAt(null); return current; });
        new WorkflowReconciler(memory, engine, clock, metrics, 10).initializeLegacyDeadlines();
        assertEquals(NOW.plusSeconds(10), memory.findById("tx").orElseThrow().getStageDeadlineAt());
        clock.now = NOW.plusSeconds(10);
        new WorkflowReconciler(memory, engine, clock, metrics, 10).expireStages();
        var failed = memory.findById("tx").orElseThrow();
        assertEquals(TransactionStatus.FAILED, failed.getStatus());
        assertEquals("STAGE_TIMEOUT", failed.getResult().get("error_code").asText());
        assertEquals(1, failed.getPendingOutbox().size());
    }

    @Test void invalidBatchSizeFailsAtStartup() {
        assertThrows(IllegalArgumentException.class, () -> new WorkflowReconciler(repository, service, clock, metrics, 0));
    }

    private static final class MutableClock extends Clock {
        Instant now = NOW;
        @Override public ZoneId getZone() { return ZoneOffset.UTC; }
        @Override public Clock withZone(ZoneId zone) { return this; }
        @Override public Instant instant() { return now; }
    }
}
