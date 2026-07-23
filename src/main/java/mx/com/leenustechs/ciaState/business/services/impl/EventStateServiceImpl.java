package mx.com.leenustechs.ciaState.business.services.impl;

import java.util.List;
import java.util.Optional;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
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
    public List<EventStateResponse> findAllByTransactionId(String transactionId) {
        Optional<List<EventStateEntity>> op = eventStateRepository.findAllByTransactionId(transactionId);
        if(op.isEmpty()){
            throw new IllegalAccessError("No hay registros con ese id de transaccion ::"+transactionId);
        }
        List<EventStateModel> models = eventStateModelMapper.toModels(op.get());
        return eventStateModelMapper.toResponses(models);
    }

    @Override
    public EventStateResponse save(EventStateModel eventStateModel) {
        // TODO Auto-generated method stub
        throw new UnsupportedOperationException("Unimplemented method 'save'");
    }
    
}
