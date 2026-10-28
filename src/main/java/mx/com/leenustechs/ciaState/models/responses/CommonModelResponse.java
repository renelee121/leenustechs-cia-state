package mx.com.leenustechs.ciaState.models.responses;

import lombok.Data;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

@Data
public class CommonModelResponse {
    private String transactionId;
    private String producer;
    private OperationType command;
    @lombok.ToString.Exclude
    private JsonNode payload;
    private String eventId;
    private String executionId;
    private Integer stageIndex;
    private EventOutcome outcome;
    private TransactionStatus status;
}
