package mx.com.leenustechs.ciaState.business.services;

import java.util.Map;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

public interface EventStateService {
    public EventStateResponse save(CommonModel event, TransactionStatus status, Integer currentStage, Map<StepType, StepStatus> steps, JsonNode result);
    public EventStateResponse findByTransactionId(String transactionId);
}
