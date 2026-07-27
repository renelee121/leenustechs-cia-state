package mx.com.leenustechs.ciaState.business.useCases;

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepType;

@Component
public class LoginUseCase extends AbstractWorkflowUseCase {

    private static final List<WorkflowStage> WORKFLOW = List.of(
            WorkflowStage.single(StepType.SECURITY),
            WorkflowStage.parallel(
                    StepType.STUDENTS,
                    StepType.ASSETS,
                    StepType.PREFERENCES,
                    StepType.ROLES),
            WorkflowStage.single(StepType.MODULES));

    public LoginUseCase(
            CommonModelMapper commonModelMapper,
            KafkaProducerAdapter kafkaProducerAdapter,
            EventStateService eventStateService) {

        super(commonModelMapper, kafkaProducerAdapter, eventStateService);
    }

    @Override
    protected List<WorkflowStage> workflow() {
        return WORKFLOW;
    }

    @Override
    public Set<OperationType> getEventTypes() {
        return Set.of(OperationType.LOGIN);
    }
}
