package mx.com.leenustechs.ciaState.business.services.impl;

import java.util.Optional;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.exceptions.TransactionNotFoundException;
import mx.com.leenustechs.ciaState.business.utils.mappers.EventStateModelMapper;
import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;

@Service
@RequiredArgsConstructor
public class EventStateServiceImpl implements EventStateService{

    private final EventStateRepository eventStateRepository;
    private final EventStateModelMapper eventStateModelMapper;

    @Override
    public EventStateResponse findByTransactionId(String transactionId) {
        Optional<EventStateEntity> entity =
                eventStateRepository.findByTransactionId(transactionId);

        if (entity.isEmpty()) {
            throw new TransactionNotFoundException(transactionId);
        }

        return eventStateModelMapper.toResponse(
            eventStateModelMapper.toModel(
                entity.get()
            )
        );
    }

    @Override
    public EventStateResponse save(EventStateModel eventStateModel) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'save'");
    }
    
}
