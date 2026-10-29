package mx.com.leenustechs.ciaState.business.outbox;

import java.time.Clock;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import mx.com.leenustechs.ciaState.models.records.OutboxEvent;
import mx.com.leenustechs.ciaState.models.records.OutboxPage;

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
    private final Clock clock;
    private final MeterRegistry meters;
    private final Counter successes;
    private final Counter failures;
    private final Counter timeouts;
    private final Counter rejectedWorkflows;
    private final Counter scanFailures;
    private final Timer deliveryDuration;
    private final Timer pollDuration;
    private final AtomicLong pageWorkflows = new AtomicLong();
    private final AtomicLong pageEvents = new AtomicLong();
    private final AtomicLong pageUnknownTimestamps = new AtomicLong();
    private final AtomicReference<Instant> oldestPendingAt = new AtomicReference<>();
    private String cursor;

    public OutboxPublisher(EventStateRepository repository, KafkaProducerAdapter producer,
            int batchSize, long sendTimeoutSeconds) {
        this(repository, producer, batchSize, sendTimeoutSeconds, new SimpleMeterRegistry(), Clock.systemUTC());
    }

    @Autowired
    public OutboxPublisher(EventStateRepository repository, KafkaProducerAdapter producer,
            @Value("${outbox.batch-size:100}") int batchSize,
            @Value("${outbox.send-timeout-seconds:15}") long sendTimeoutSeconds,
            MeterRegistry meters, Clock clock) {
        if (batchSize <= 0 || sendTimeoutSeconds <= 0) {
            throw new IllegalArgumentException("Outbox batch size and send timeout must be positive");
        }
        this.repository = repository;
        this.producer = producer;
        this.batchSize = batchSize;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
        this.clock = Objects.requireNonNull(clock);
        this.meters = Objects.requireNonNull(meters);
        successes = meters.counter("cia.outbox.delivery.success");
        failures = meters.counter("cia.outbox.delivery.failures");
        timeouts = meters.counter("cia.outbox.delivery.timeouts");
        rejectedWorkflows = meters.counter("cia.outbox.scan.rejected");
        scanFailures = meters.counter("cia.outbox.scan.failures");
        deliveryDuration = meters.timer("cia.outbox.delivery.duration");
        pollDuration = meters.timer("cia.outbox.poll.duration");
        Gauge.builder("cia.outbox.page.pending.workflows", pageWorkflows, AtomicLong::doubleValue)
                .tag("scope", "last_scan_page").register(meters);
        Gauge.builder("cia.outbox.page.pending.events", pageEvents, AtomicLong::doubleValue)
                .tag("scope", "last_scan_page").register(meters);
        Gauge.builder("cia.outbox.page.unknown.timestamps", pageUnknownTimestamps, AtomicLong::doubleValue)
                .tag("scope", "last_scan_page").register(meters);
        Gauge.builder("cia.outbox.page.oldest.age", this, OutboxPublisher::oldestPendingAgeSeconds)
                .baseUnit("seconds").tag("scope", "last_scan_page").register(meters);
    }

    @Scheduled(fixedDelayString = "${outbox.poll-interval-ms:1000}", initialDelayString = "${outbox.poll-interval-ms:1000}")
    public synchronized void publishPending() {
        Timer.Sample poll = Timer.start(meters);
        try {
            var page = repository.scanPendingOutbox(batchSize, cursor);
            // Advance by raw rows, even when every row on this page is corrupt.
            cursor = page.nextCursor();
            observePage(page);
            rejectedWorkflows.increment(page.rejectedWorkflowIds().size());
            for (String rejectedId : page.rejectedWorkflowIds()) {
                log.warn("Skipping invalid pending workflow. transactionId={}", rejectedId);
            }
            for (EventStateEntity workflow : page.workflows()) {
                if (workflow.getPendingOutbox() == null) continue;
                for (OutboxEvent event : workflow.getPendingOutbox().values()) {
                    Timer.Sample delivery = null;
                    try {
                        // A scan is only a snapshot. Confirm the intent still exists before
                        // starting a send; a sibling worker may have failed since the scan.
                        Optional<OutboxEvent> pending = currentIntent(workflow.getTransactionId(), event.id());
                        if (pending.isEmpty()) continue;
                        OutboxEvent authorized = pending.get();
                        delivery = Timer.start(meters);
                        producer.publish(authorized.topic(), authorized.key(), authorized.event(), authorized.id())
                                .get(sendTimeoutSeconds, TimeUnit.SECONDS);
                        acknowledge(workflow.getTransactionId(), authorized.id());
                        successes.increment();
                    } catch (InterruptedException interrupted) {
                        failures.increment();
                        Thread.currentThread().interrupt();
                        return;
                    } catch (Exception failure) {
                        failures.increment();
                        if (isTimeout(failure)) timeouts.increment();
                        // Do not log exception messages or payloads: client errors may contain secrets.
                        log.warn("Outbox delivery pending. transactionId={}, eventId={}, failureType={}",
                                workflow.getTransactionId(), event.id(), failure.getClass().getSimpleName());
                    } finally {
                        if (delivery != null) delivery.stop(deliveryDuration);
                    }
                }
            }
        } catch (Exception failure) {
            scanFailures.increment();
            log.warn("Unable to scan pending outbox. failureType={}", failure.getClass().getSimpleName());
        } finally {
            poll.stop(pollDuration);
        }
    }

    private void observePage(OutboxPage page) {
        long workflows = 0, events = 0, missing = 0;
        Instant oldest = null;
        for (EventStateEntity workflow : page.workflows()) {
            if (workflow.getPendingOutbox() == null || workflow.getPendingOutbox().isEmpty()) continue;
            workflows++;
            for (OutboxEvent event : workflow.getPendingOutbox().values()) {
                events++;
                if (event.enqueuedAt() == null) {
                    missing++;
                } else if (oldest == null || event.enqueuedAt().isBefore(oldest)) {
                    oldest = event.enqueuedAt();
                }
            }
        }
        pageWorkflows.set(workflows);
        pageEvents.set(events);
        pageUnknownTimestamps.set(missing);
        oldestPendingAt.set(oldest);
    }

    private double oldestPendingAgeSeconds() {
        Instant oldest = oldestPendingAt.get();
        return oldest == null ? 0 : Math.max(0,
                (double) clock.instant().getEpochSecond() - oldest.getEpochSecond());
    }

    private boolean isTimeout(Throwable failure) {
        for (int depth = 0; failure != null && depth < 16; depth++, failure = failure.getCause()) {
            if (failure instanceof TimeoutException || failure instanceof org.apache.kafka.common.errors.TimeoutException) return true;
        }
        return false;
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
            if (state.getStatus() != TransactionStatus.PROCESSING || expired(state)
                    || !Objects.equals(state.getCurrentStage(), event.event().getStageIndex())) {
                return Optional.empty();
            }
            return Optional.of(event);
        });
    }

    private boolean expired(EventStateEntity state) {
        return state.getStageDeadlineAt() != null && !clock.instant().isBefore(state.getStageDeadlineAt());
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
