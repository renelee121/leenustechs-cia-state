package mx.com.leenustechs.ciaState.business.repositories;

import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;

import java.util.Optional;

import org.springframework.data.repository.CrudRepository;

public interface EventStateRepository extends CrudRepository<EventStateEntity, String>{
    Optional<EventStateEntity> findByTransactionId(String transactionId);
}
