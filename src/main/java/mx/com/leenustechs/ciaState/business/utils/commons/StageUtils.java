package mx.com.leenustechs.ciaState.business.utils.commons;

import java.util.List;

import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.StepType;

public class StageUtils {
    public static int findStageIndex(StepType step, List<WorkflowStage> sequence) {
        for (int i = 0; i < sequence.size(); i++) {
            if (sequence.get(i).steps().contains(step)) {
                return i;
            }
        }
        return -1;
    }
}
