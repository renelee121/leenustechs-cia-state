package mx.com.leenustechs.ciaState.business.useCases;

import static org.assertj.core.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;

class LoginUseCaseTest {
    @Test
    void declaresLoginStagesAndDelegatesAtomicallyWithoutDirectPublishing() {
        EventStateService service = mock(EventStateService.class);
        LoginUseCase login = new LoginUseCase(Mappers.getMapper(CommonModelMapper.class), service);
        CommonModel event = new CommonModel("tx", "MODULES", OperationType.LOGIN, null);
        event.setEventId("event-1");
        event.setExecutionId("run-1");
        event.setStageIndex(2);
        event.setOutcome(EventOutcome.SUCCESS);
        List<WorkflowStage> expectedStages = List.of(
                WorkflowStage.single(StepType.SECURITY),
                WorkflowStage.parallel(StepType.STUDENTS, StepType.ASSETS, StepType.PREFERENCES, StepType.ROLES),
                WorkflowStage.single(StepType.MODULES));
        EventStateResponse state = new EventStateResponse();
        state.setStatus(TransactionStatus.COMPLETED);
        when(service.process(event, expectedStages)).thenReturn(state);
        CommonModelResponse response = login.execute(event);
        verify(service).process(event, expectedStages);
        verifyNoMoreInteractions(service);
        assertThat(login.getEventTypes()).containsExactly(OperationType.LOGIN);
        assertThat(response.getTransactionId()).isEqualTo("tx");
        assertThat(response.getEventId()).isEqualTo("event-1");
        assertThat(response.getExecutionId()).isEqualTo("run-1");
        assertThat(response.getStageIndex()).isEqualTo(2);
        assertThat(response.getOutcome()).isEqualTo(EventOutcome.SUCCESS);
        assertThat(response.getStatus()).isEqualTo(TransactionStatus.COMPLETED);
    }
}
