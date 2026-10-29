package mx.com.leenustechs.ciaState.business.services.impl;

import java.time.Clock;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.DeadlineCursor;

/** Indexed, bounded reconciliation; all decisions are rechecked in a state transaction. */
@Slf4j
@Component
@ConditionalOnProperty(name = "workflows.reconciliation.enabled", havingValue = "true", matchIfMissing = true)
public class WorkflowReconciler {
    private final EventStateRepository repository;
    private final EventStateService service;
    private final Clock clock;
    private final MeterRegistry metrics;
    private final int batchSize;
    private DeadlineCursor dueCursor;
    private String legacyCursor;

    public WorkflowReconciler(EventStateRepository repository, EventStateService service, Clock clock,
            MeterRegistry metrics, @Value("${workflows.reconciliation.batch-size:100}") int batchSize) {
        if (batchSize <= 0) throw new IllegalArgumentException("Reconciliation batch size must be positive");
        this.repository = repository;
        this.service = service;
        this.clock = clock;
        this.metrics = metrics;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${workflows.reconciliation.poll-interval-ms:1000}",
            initialDelayString = "${workflows.reconciliation.poll-interval-ms:1000}")
    public void expireStages() {
        try {
            var now = clock.instant();
            if (dueCursor != null && dueCursor.deadlineMillis() > now.toEpochMilli()) dueCursor = null;
            var page = repository.scanExpiredWorkflows(now, batchSize, dueCursor);
            dueCursor = page.nextCursor();
            reportRejected("expired", page.rejectedWorkflowIds());
            for (EventStateEntity state : page.workflows()) {
                if (Thread.currentThread().isInterrupted()) return;
                reconcile(state.getTransactionId(), "expired");
            }
        } catch (RuntimeException failure) {
            scanFailure("expired", failure);
        }
    }

    @Scheduled(fixedDelayString = "${workflows.reconciliation.legacy-poll-interval-ms:30000}",
            initialDelayString = "${workflows.reconciliation.poll-interval-ms:1000}")
    public void initializeLegacyDeadlines() {
        try {
            var page = repository.scanProcessingWorkflows(batchSize, legacyCursor);
            legacyCursor = page.nextCursor();
            reportRejected("legacy", page.rejectedWorkflowIds());
            for (EventStateEntity state : page.workflows()) {
                if (Thread.currentThread().isInterrupted()) return;
                if (state.getStageDeadlineAt() == null) reconcile(state.getTransactionId(), "legacy");
            }
        } catch (RuntimeException failure) {
            scanFailure("legacy", failure);
        }
    }

    private void reconcile(String transactionId, String scan) {
        try {
            boolean changed = service.reconcile(transactionId);
            metrics.counter("cia.workflows.reconciliation", "scan", scan,
                    "result", changed ? "changed" : "unchanged").increment();
        } catch (RuntimeException failure) {
            metrics.counter("cia.workflows.reconciliation", "scan", scan, "result", "failed").increment();
            log.warn("Workflow reconciliation pending. transactionId={}, failureType={}",
                    transactionId, failure.getClass().getSimpleName());
        }
    }

    private void reportRejected(String scan, List<String> rejected) {
        metrics.counter("cia.workflows.reconciliation.scan.rejected", "scan", scan).increment(rejected.size());
        for (String id : rejected) log.warn("Skipping invalid workflow in reconciliation. transactionId={}", id);
    }

    private void scanFailure(String scan, RuntimeException failure) {
        metrics.counter("cia.workflows.reconciliation.scan.failures", "scan", scan).increment();
        log.warn("Workflow reconciliation scan unavailable. scan={}, failureType={}",
                scan, failure.getClass().getSimpleName());
    }
}
