package mx.com.leenustechs.ciaState.models.records;

import java.util.Map;

import mx.com.leenustechs.ciaState.models.types.StepType;
import tools.jackson.databind.JsonNode;

public record StageResultSnapshot(
        JsonNode inputPayload,
        Map<StepType, JsonNode> stepResults) {
}
