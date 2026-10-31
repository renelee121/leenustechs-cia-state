package mx.com.leenustechs.ciaState.models;

import java.nio.charset.StandardCharsets;

import lombok.Data;
import lombok.NoArgsConstructor;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import tools.jackson.databind.JsonNode;

/** Event envelope shared by the orchestrator and its workers. */
@Data
@NoArgsConstructor
public class CommonModel implements Event {
    private String transactionId;
    private String producer;
    private OperationType command;
    @lombok.ToString.Exclude
    private JsonNode payload;
    private String eventId;
    private String executionId;
    private Integer stageIndex;
    private EventOutcome outcome;

    /** Retains the original envelope constructor for existing callers. */
    public CommonModel(String transactionId, String producer, OperationType command, JsonNode payload) {
        this.transactionId = transactionId;
        this.producer = producer;
        this.command = command;
        this.payload = payload;
    }

    public void validate() {
        validateIdentifier(transactionId, "transactionId");
        if (producer == null || producer.isBlank() || producer.length() > 128) {
            throw new IllegalArgumentException("producer is required and must not exceed 128 characters");
        }
        if (command == null) {
            throw new IllegalArgumentException("command is required");
        }
        if (eventId != null) {
            validateIdentifier(eventId, "eventId");
        }
        if (executionId != null) {
            validateIdentifier(executionId, "executionId");
        }
        if (stageIndex != null && stageIndex < 0) {
            throw new IllegalArgumentException("stageIndex must not be negative");
        }
    }

    private static void validateIdentifier(String value, String field) {
        if (value == null || value.isBlank()
                || value.getBytes(StandardCharsets.UTF_8).length > 768
                || value.chars().anyMatch(c -> c < 32 || c == 127 || ".#$[]/".indexOf(c) >= 0)) {
            throw new IllegalArgumentException(field + " must be a nonblank Firebase-safe identifier of at most 768 bytes");
        }
    }
}
