package mx.com.leenustechs.ciaState.models;

import java.time.Instant;
import java.util.Map;

import lombok.Data;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

@Data
public class EventStateModel{
    private String transactionId;
    private String producer;
    private OperationType command;
    private TransactionStatus status;
    private Integer currentStage;
    private Map<StepType, StepStatus> steps;
    private JsonNode payload;
    private JsonNode result;
    private Instant createdAt;
    private Instant updatedAt;
}
