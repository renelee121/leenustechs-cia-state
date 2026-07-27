package mx.com.leenustechs.ciaState.models.records;

import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;

public record StepTransitionResult(
        EventStateResponse state,
        boolean applied) {
}
