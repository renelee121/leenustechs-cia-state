package mx.com.leenustechs.ciaState.business.utils.commons;

import static org.assertj.core.api.Assertions.assertThat;
import java.util.List;
import org.junit.jupiter.api.Test;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.StepType;

class StageUtilsTest {
    @Test
    void findsTheDeclaredStageForSequentialAndParallelWorkers() {
        List<WorkflowStage> workflow = List.of(
                WorkflowStage.single(StepType.SECURITY),
                WorkflowStage.parallel(StepType.ASSETS, StepType.ROLES),
                WorkflowStage.single(StepType.MODULES));
        assertThat(StageUtils.findStageIndex(StepType.SECURITY, workflow)).isZero();
        assertThat(StageUtils.findStageIndex(StepType.ROLES, workflow)).isEqualTo(1);
        assertThat(StageUtils.findStageIndex(StepType.MODULES, workflow)).isEqualTo(2);
        assertThat(StageUtils.findStageIndex(StepType.STUDENTS, workflow)).isEqualTo(-1);
        assertThat(StageUtils.findStageIndex(StepType.SECURITY, List.of())).isEqualTo(-1);
    }
}
