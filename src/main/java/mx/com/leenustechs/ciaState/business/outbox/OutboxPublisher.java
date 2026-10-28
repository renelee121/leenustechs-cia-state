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
            var batch = repository.findWithPendingOutbox(batchSize, cursor);
            if (batch.isEmpty()) {
                cursor = null;
                return;
            }
            // Rotate across polls: a failing first page must not starve later workflows.
            cursor = batch.getLast().getTransactionId();
            for (EventStateEntity workflow : batch) {
                if (workflow.getPendingOutbox() == null) {
                    continue;
                }
                for (OutboxEvent event : workflow.getPendingOutbox().values()) {
                    try {
                        producer.publish(event.topic(), event.key(), event.event(), event.id())
                                .get(sendTimeoutSeconds, TimeUnit.SECONDS);
                        acknowledge(workflow.getTransactionId(), event.id());
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
