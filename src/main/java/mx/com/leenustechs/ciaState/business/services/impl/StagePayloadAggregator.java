package mx.com.leenustechs.ciaState.business.services.impl;

import java.util.HashMap;
import java.util.Map;
import mx.com.leenustechs.ciaState.models.records.StageResultSnapshot;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.StepType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

/** Deterministic three-way merge against the immutable stage input. */
public final class StagePayloadAggregator {
    public JsonNode aggregate(
            StageResultSnapshot snapshot,
            WorkflowStage stage) {

        JsonNode inputPayload = snapshot.inputPayload();

        if (!stage.isParallel()) {
            StepType step = stage.steps().getFirst();
            JsonNode stepResult = snapshot.stepResults().get(step);

            if (stepResult == null) {
                throw new IllegalStateException(
                        "Missing result for completed step: " + step);
            }

            return mergeSingleResult(inputPayload, stepResult, step);
        }

        if (inputPayload != null
                && !inputPayload.isNull()
                && !inputPayload.isObject()) {
            throw new IllegalStateException(
                    "Parallel stage input payload must be a JSON object");
        }

        ObjectNode baseline =
                inputPayload == null || inputPayload.isNull()
                        ? JsonNodeFactory.instance.objectNode()
                        : ((ObjectNode) inputPayload).deepCopy();

        ObjectNode merged = baseline.deepCopy();
        Map<String, StepType> owners = new HashMap<>();

        /*
         * Iterate the workflow declaration, never Map iteration order. That
         * makes aggregation independent from response arrival order.
         */
        for (StepType step : stage.steps()) {
            JsonNode stepResult = snapshot.stepResults().get(step);

            if (stepResult == null) {
                throw new IllegalStateException(
                        "Missing result for completed parallel step: " + step);
            }

            if (stepResult.isNull()) {
                continue;
            }

            if (!stepResult.isObject()) {
                throw new IllegalStateException(
                        "Parallel step result must be a JSON object. step="
                                + step);
            }

            mergeObject(
                    merged,
                    baseline,
                    (ObjectNode) stepResult,
                    owners,
                    step,
                    "");
        }

        return merged;
    }

    private JsonNode mergeSingleResult(
            JsonNode inputPayload,
            JsonNode stepResult,
            StepType step) {

        if (stepResult.isNull()) {
            return inputPayload == null
                    ? null
                    : inputPayload.deepCopy();
        }

        if (inputPayload == null || inputPayload.isNull()) {
            return stepResult.deepCopy();
        }

        if (inputPayload.isObject() && stepResult.isObject()) {
            ObjectNode baseline = ((ObjectNode) inputPayload).deepCopy();
            ObjectNode merged = baseline.deepCopy();

            mergeObject(
                    merged,
                    baseline,
                    (ObjectNode) stepResult,
                    new HashMap<>(),
                    step,
                    "");

            return merged;
        }

        return stepResult.deepCopy();
    }

    private void mergeObject(
            ObjectNode target,
            ObjectNode baseline,
            ObjectNode incoming,
            Map<String, StepType> owners,
            StepType sourceStep,
            String prefix) {

        for (Map.Entry<String, JsonNode> property : incoming.properties()) {
            String name = property.getKey();
            String path = prefix.isEmpty() ? name : prefix + "." + name;

            JsonNode baseValue = baseline.get(name);
            JsonNode incomingValue = property.getValue();

            /* Branch merely repeats the stage input: no write to merge. */
            if (baseValue != null && baseValue.equals(incomingValue)) {
                continue;
            }

            JsonNode currentValue = target.get(name);

            if (incomingValue.isObject()
                    && (baseValue == null || baseValue.isObject())) {

                ObjectNode baseObject =
                        baseValue == null
                                ? JsonNodeFactory.instance.objectNode()
                                : (ObjectNode) baseValue;

                ObjectNode targetObject;

                if (currentValue == null
                        || (baseValue != null
                                && currentValue.equals(baseValue))) {
                    targetObject = baseObject.deepCopy();
                    target.set(name, targetObject);
                } else if (currentValue.isObject()) {
                    targetObject = (ObjectNode) currentValue;
                } else {
                    throw mergeConflict(path, owners.get(path), sourceStep);
                }

                mergeObject(
                        targetObject,
                        baseObject,
                        (ObjectNode) incomingValue,
                        owners,
                        sourceStep,
                        path);
                continue;
            }

            if (currentValue == null
                    || (baseValue != null
                            && currentValue.equals(baseValue))) {
                target.set(name, incomingValue.deepCopy());
                owners.put(path, sourceStep);
                continue;
            }

            if (currentValue.equals(incomingValue)) {
                continue;
            }

            throw mergeConflict(path, owners.get(path), sourceStep);
        }
    }

    private IllegalStateException mergeConflict(
            String path,
            StepType previousStep,
            StepType incomingStep) {

        String previous =
                previousStep == null
                        ? "another parallel step"
                        : previousStep.name();

        return new IllegalStateException(
                "Conflicting parallel results at path '%s': %s vs %s"
                        .formatted(path, previous, incomingStep.name()));
    }

}
