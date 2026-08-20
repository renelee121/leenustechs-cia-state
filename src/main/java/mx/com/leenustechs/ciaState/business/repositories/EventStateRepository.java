package mx.com.leenustechs.ciaState.business.repositories;

import java.util.Optional;
import java.util.function.Function;

import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.AtomicStateUpdate;

/**
 * Persistence port for workflow state.
 *
 * <p>The atomic update callback must be side-effect free because a realtime
 * database transaction may invoke it more than once when concurrent writers
 * modify the same workflow.</p>
 */
public interface EventStateRepository {

    Optional<EventStateEntity> findById(String transactionId);

    AtomicStateUpdate updateAtomically(
            String transactionId,
            Function<Optional<EventStateEntity>, Optional<EventStateEntity>> mutation);
}
