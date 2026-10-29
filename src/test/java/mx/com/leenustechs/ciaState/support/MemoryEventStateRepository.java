package mx.com.leenustechs.ciaState.support;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.AtomicStateUpdate;
import mx.com.leenustechs.ciaState.models.records.OutboxPage;
import mx.com.leenustechs.ciaState.models.records.DeadlineCursor;
import mx.com.leenustechs.ciaState.models.records.WorkflowDeadlinePage;
import mx.com.leenustechs.ciaState.models.records.WorkflowPage;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.json.JsonMapper;

/** Isolated snapshots and a lock model the atomic repository contract for workflow tests. */
public final class MemoryEventStateRepository implements EventStateRepository {
    private final Map<String, EventStateEntity> states = new LinkedHashMap<>();
    private final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    private EventStateEntity copy(EventStateEntity entity) {
        return mapper.convertValue(entity, EventStateEntity.class);
    }
    @Override
    public synchronized Optional<EventStateEntity> findById(String id) {
        return Optional.ofNullable(states.get(id)).map(this::copy);
    }
    @Override
    public synchronized OutboxPage scanPendingOutbox(int limit, String afterTransactionId) {
        var page = states.values().stream().filter(e -> e.getPendingOutbox() != null && !e.getPendingOutbox().isEmpty())
                .filter(e -> afterTransactionId == null || e.getTransactionId().compareTo(afterTransactionId) > 0)
                .sorted(java.util.Comparator.comparing(EventStateEntity::getTransactionId)).limit(limit).map(this::copy).toList();
        return new OutboxPage(page, page.isEmpty() ? null : page.getLast().getTransactionId(), List.of());
    }
    @Override
    public synchronized WorkflowDeadlinePage scanExpiredWorkflows(Instant cutoff, int limit, DeadlineCursor after) {
        var page = states.values().stream().filter(e -> e.getStageDeadlineAt() != null
                        && !e.getStageDeadlineAt().isAfter(cutoff))
                .filter(e -> after == null || e.getStageDeadlineAt().toEpochMilli() > after.deadlineMillis()
                        || e.getStageDeadlineAt().toEpochMilli() == after.deadlineMillis()
                        && e.getTransactionId().compareTo(after.transactionId()) > 0)
                .sorted(java.util.Comparator.comparing(EventStateEntity::getStageDeadlineAt)
                        .thenComparing(EventStateEntity::getTransactionId))
                .limit(limit).map(this::copy).toList();
        DeadlineCursor cursor = page.isEmpty() ? null : new DeadlineCursor(
                page.getLast().getStageDeadlineAt().toEpochMilli(), page.getLast().getTransactionId());
        return new WorkflowDeadlinePage(page, cursor, List.of());
    }
    @Override
    public synchronized WorkflowPage scanProcessingWorkflows(int limit, String afterTransactionId) {
        var page = states.values().stream().filter(e -> e.getStatus() == TransactionStatus.PROCESSING)
                .filter(e -> afterTransactionId == null || e.getTransactionId().compareTo(afterTransactionId) > 0)
                .sorted(java.util.Comparator.comparing(EventStateEntity::getTransactionId))
                .limit(limit).map(this::copy).toList();
        return new WorkflowPage(page, page.isEmpty() ? null : page.getLast().getTransactionId(), List.of());
    }
    @Override
    public synchronized AtomicStateUpdate updateAtomically(String id,
            Function<Optional<EventStateEntity>, Optional<EventStateEntity>> mutation) {
        Optional<EventStateEntity> desired = mutation.apply(findById(id));
        desired.ifPresent(entity -> states.put(id, copy(entity)));
        return new AtomicStateUpdate(desired.isPresent(), findById(id));
    }
}
