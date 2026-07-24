package mx.com.leenustechs.ciaState.models.responses;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

@Slf4j
@Data
public class EventStateResponse {
    private String transactionId;
    private OperationType command;
    private TransactionStatus status;
    private Integer currentStage;
    private JsonNode result;
}
