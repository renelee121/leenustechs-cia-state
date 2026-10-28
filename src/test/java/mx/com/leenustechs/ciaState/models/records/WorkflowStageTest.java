package mx.com.leenustechs.ciaState.models.records;

import static org.assertj.core.api.Assertions.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import mx.com.leenustechs.ciaState.models.types.StepType;

class WorkflowStageTest {
    @Test
    void stagesDefensivelyCopyTheirDeclarations() {
        List<StepType> declared = new ArrayList<>(List.of(StepType.SECURITY));
        WorkflowStage stage = new WorkflowStage(declared);
        declared.add(StepType.MODULES);
        assertThat(stage.steps()).containsExactly(StepType.SECURITY);
        assertThatThrownBy(() -> stage.steps().add(StepType.MODULES)).isInstanceOf(UnsupportedOperationException.class);
        assertThat(stage.isParallel()).isFalse();
        assertThat(WorkflowStage.parallel(StepType.ASSETS, StepType.ROLES).isParallel()).isTrue();
        assertThat(WorkflowStage.single(StepType.SECURITY)).isEqualTo(stage);
    }

    @Test
    void rejectsEmptyNullAndDuplicateSteps() {
        assertThatThrownBy(() -> new WorkflowStage(null)).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkflowStage(List.of())).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> new WorkflowStage(Arrays.asList(StepType.SECURITY, null))).isInstanceOf(IllegalArgumentException.class);
        assertThatThrownBy(() -> WorkflowStage.parallel(StepType.ASSETS, StepType.ASSETS)).isInstanceOf(IllegalArgumentException.class);
    }
}
