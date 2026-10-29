package mx.com.leenustechs.ciaState.business.repositories;

import java.time.Instant;
import java.util.Optional;
import java.util.List;
import java.util.function.Function;

import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.AtomicStateUpdate;
import mx.com.leenustechs.ciaState.models.records.OutboxPage;
import mx.com.leenustechs.ciaState.models.records.DeadlineCursor;
import mx.com.leenustechs.ciaState.models.records.WorkflowDeadlinePage;
import mx.com.leenustechs.ciaState.models.records.WorkflowPage;

/**
 * Persistence port for workflow state.
 *
 * <p>The atomic update callback must be side-effect free because a realtime
 * database transaction may invoke it more than once when concurrent writers
 * modify the same workflow.</p>
 */
public interface EventStateRepository {

    Optional<EventStateEntity> findById(String transactionId);

    default List<EventStateEntity> findWithPendingOutbox(int limit) {
        return scanPendingOutbox(limit, null).workflows();
    }

    default List<EventStateEntity> findWithPendingOutbox(int limit, String afterTransactionId) {
        return scanPendingOutbox(limit, afterTransactionId).workflows();
    }

    /**
     * Scans at most {@code limit} raw workflows. The cursor includes rejected
     * rows so a corrupt workflow cannot prevent later workflows from publishing.
     */
    OutboxPage scanPendingOutbox(int limit, String afterTransactionId);

    /** Scans the indexed numeric deadline range up to cutoff, excluding null deadlines. */
    WorkflowDeadlinePage scanExpiredWorkflows(Instant cutoff, int limit, DeadlineCursor after);

    /** Scans active workflows so restored legacy states can receive a grace deadline. */
    WorkflowPage scanProcessingWorkflows(int limit, String afterTransactionId);

    AtomicStateUpdate updateAtomically(
            String transactionId,
            Function<Optional<EventStateEntity>, Optional<EventStateEntity>> mutation);
}
