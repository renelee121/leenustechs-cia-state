package mx.com.leenustechs.ciaState.models.entities;

import java.time.Instant;
import java.util.Map;

import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;

import org.springframework.data.annotation.Id;
import lombok.Data;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

@Data
@RedisHash("eventState")
public class EventStateEntity {
    @Id
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
    @TimeToLive
    private Long ttl;
}
