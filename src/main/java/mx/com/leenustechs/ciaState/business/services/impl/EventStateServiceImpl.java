package mx.com.leenustechs.ciaState.business.services.impl;

import java.time.Instant;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.exceptions.TransactionNotFoundException;
import mx.com.leenustechs.ciaState.business.utils.mappers.EventStateInputMapper;
import mx.com.leenustechs.ciaState.business.utils.mappers.EventStateModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

@Service
@RequiredArgsConstructor
public class EventStateServiceImpl implements EventStateService{

    private final EventStateRepository eventStateRepository;
    private final EventStateModelMapper eventStateModelMapper;
    private final EventStateInputMapper eventStateInputMapper;

    @Override
    public EventStateResponse findByTransactionId(String transactionId) {
        EventStateEntity entity = eventStateRepository
                .findById(transactionId)
                .orElseThrow(() ->
                        new TransactionNotFoundException(transactionId)
                );
        EventStateModel model = eventStateModelMapper.toModel(
            entity
        );
        EventStateResponse response = eventStateModelMapper.toResponse(
            model
        );
        return response;
    }

    @Override
    public EventStateResponse save(CommonModel event, TransactionStatus status,
            String currentStep, String nextStep, JsonNode result) {
        EventStateModel model = eventStateInputMapper.toEventStateModel(event, status, currentStep, nextStep, result, Instant.now(), Instant.now());
        throw new UnsupportedOperationException("Unimplemented method 'save'");
    }

    
    
}
