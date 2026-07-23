package mx.com.leenustechs.ciaState.models.entities;

import java.time.Instant;

import org.springframework.data.redis.core.RedisHash;
import org.springframework.data.redis.core.TimeToLive;

import org.springframework.data.annotation.Id;
import lombok.Data;
import mx.com.leenustechs.ciaState.models.types.OperationType;
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
    private String currentStep;
    private String nextStep;
    private JsonNode payload;
    private JsonNode result;
    private Instant createdAt;
    private Instant updatedAt;
    @TimeToLive
    private Long ttl;
}
