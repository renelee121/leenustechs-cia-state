package mx.com.leenustechs.ciaState.business.services;

import java.util.List;

import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;

public interface EventStateService {
    public EventStateResponse save(EventStateModel eventStateModel);
    public List<EventStateResponse> findAllByTransactionId(String transactionId);
}
