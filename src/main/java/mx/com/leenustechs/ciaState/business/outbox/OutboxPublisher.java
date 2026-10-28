package mx.com.leenustechs.ciaState.business.outbox;

import java.util.LinkedHashMap;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import mx.com.leenustechs.ciaState.models.records.OutboxEvent;

/**
 * Durable at-least-once delivery. An acknowledgement may be lost after Kafka has
 * accepted a send; consumers must deduplicate the stable event-id, including
 * when more than one application instance publishes the same pending event.
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "outbox.enabled", havingValue = "true", matchIfMissing = true)
public class OutboxPublisher {
    private final EventStateRepository repository;
    private final KafkaProducerAdapter producer;
    private final int batchSize;
    private final long sendTimeoutSeconds;
    private String cursor;

    public OutboxPublisher(EventStateRepository repository, KafkaProducerAdapter producer,
            @Value("${outbox.batch-size:100}") int batchSize,
            @Value("${outbox.send-timeout-seconds:15}") long sendTimeoutSeconds) {
        if (batchSize <= 0 || sendTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("Outbox batch size and send timeout must be positive");
        }
        this.repository = repository;
        this.producer = producer;
        this.batchSize = batchSize;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}", initialDelayString = "${outbox.poll-interval-ms:1000}")
    public synchronized void publishPending() {
        try {
            var page = repository.scanPendingOutbox(batchSize, cursor);
            // Advance by raw rows, even when every row on this page is corrupt.
            cursor = page.nextCursor();
            for (String rejectedId : page.rejectedWorkflowIds()) {
                log.warn("Skipping invalid pending workflow. transactionId={}", rejectedId);
            }
            for (EventStateEntity workflow : page.workflows()) {
                if (workflow.getPendingOutbox() == null) {
                    continue;
                }
                for (OutboxEvent event : workflow.getPendingOutbox().values()) {
                    try {
                        // A scan is only a snapshot. Confirm the intent still exists before
                        // starting a send; a sibling worker may have failed since the scan.
                        Optional<OutboxEvent> pending = currentIntent(workflow.getTransactionId(), event.id());
                        if (pending.isEmpty()) continue;
                        OutboxEvent authorized = pending.get();
                        producer.publish(authorized.topic(), authorized.key(), authorized.event(), authorized.id())
                                .get(sendTimeoutSeconds, TimeUnit.SECONDS);
                        acknowledge(workflow.getTransactionId(), authorized.id());
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception failure) {
                        // Do not log exception messages or payloads: client errors may contain secrets.
                        log.warn("Outbox delivery pending. transactionId={}, eventId={}, failureType={}",
                                workflow.getTransactionId(), event.id(), failure.getClass().getSimpleName());
                    }
                }
            }
        } catch (Exception failure) {
            log.warn("Unable to scan pending outbox. failureType={}", failure.getClass().getSimpleName());
        }
    }

    private Optional<OutboxEvent> currentIntent(String transactionId, String eventId) {
        // A no-op RTDB transaction confirms server state; a local read can be stale.
        var confirmed = repository.updateAtomically(transactionId, current -> current.flatMap(state -> {
            if (state.getPendingOutbox() == null) return Optional.empty();
            var remaining = new LinkedHashMap<>(state.getPendingOutbox());
            boolean removed = remaining.values().removeIf(event -> obsolete(state, event));
            if (!removed) return Optional.empty();
            state.setPendingOutbox(remaining);
            state.setOutboxPending(!remaining.isEmpty());
            return Optional.of(state);
        })).state();
        return confirmed.flatMap(state -> {
            if (state.getPendingOutbox() == null) return Optional.empty();
            OutboxEvent event = state.getPendingOutbox().get(eventId);
            if (event == null) return Optional.empty();
            if (KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE.equals(event.topic())) {
                return terminal(state) ? Optional.of(event) : Optional.empty();
            }
            if (state.getStatus() != TransactionStatus.PROCESSING
                    || !java.util.Objects.equals(state.getCurrentStage(), event.event().getStageIndex())) {
                return Optional.empty();
            }
            return Optional.of(event);
        });
    }

    private boolean obsolete(EventStateEntity state, OutboxEvent event) {
        if (KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE.equals(event.topic())) return false;
        return terminal(state) || (state.getStatus() == TransactionStatus.PROCESSING
                && state.getCurrentStage() != null && event.event().getStageIndex() != null
                && event.event().getStageIndex() < state.getCurrentStage());
    }

    private boolean terminal(EventStateEntity state) {
        return state.getStatus() == TransactionStatus.COMPLETED || state.getStatus() == TransactionStatus.FAILED;
    }

    private void acknowledge(String transactionId, String eventId) {
        repository.updateAtomically(transactionId, current -> current.flatMap(workflow -> {
            if (workflow.getPendingOutbox() == null || !workflow.getPendingOutbox().containsKey(eventId)) {
                return Optional.empty();
            }
            var remaining = new LinkedHashMap<>(workflow.getPendingOutbox());
            remaining.remove(eventId);
            workflow.setPendingOutbox(remaining);
            workflow.setOutboxPending(!remaining.isEmpty());
            return Optional.of(workflow);
        }));
    }
}
