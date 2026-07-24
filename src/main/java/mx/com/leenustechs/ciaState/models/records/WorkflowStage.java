package mx.com.leenustechs.ciaState.models.records;

import java.util.List;

import mx.com.leenustechs.ciaState.models.types.StepType;

public record WorkflowStage(
        List<StepType> steps) {
    public static WorkflowStage single(StepType step) {
        return new WorkflowStage(List.of(step));
    }

    public static WorkflowStage parallel(StepType... steps) {
        return new WorkflowStage(List.of(steps));
    }

    public boolean isParallel() {
        return steps.size() > 1;
    }
}