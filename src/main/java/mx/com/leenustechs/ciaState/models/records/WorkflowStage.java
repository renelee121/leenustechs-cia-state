package mx.com.leenustechs.ciaState.models.records;

import java.util.HashSet;
import java.util.List;
import mx.com.leenustechs.ciaState.models.types.StepType;

public record WorkflowStage(List<StepType> steps) {
    public WorkflowStage {
        if (steps == null || steps.isEmpty() || steps.stream().anyMatch(java.util.Objects::isNull)
                || new HashSet<>(steps).size() != steps.size()) {
            throw new IllegalArgumentException("A workflow stage needs distinct nonnull steps");
        }
        steps = List.copyOf(steps);
    }

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
