package mx.com.leenustechs.ciaState.models.records;

import java.util.Optional;

import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;

public record AtomicStateUpdate(
        boolean applied,
        Optional<EventStateEntity> state) {
}
