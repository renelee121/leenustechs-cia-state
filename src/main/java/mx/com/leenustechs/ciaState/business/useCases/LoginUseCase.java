package mx.com.leenustechs.ciaState.business.useCases;

import java.util.List;
import java.util.Set;

import org.springframework.stereotype.Component;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.commons.EventOperation;
import mx.com.leenustechs.ciaState.business.utils.commons.StageUtils;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepType;

@Slf4j
@Component
@RequiredArgsConstructor
public class LoginUseCase implements EventOperation {
    private static final List<WorkflowStage> sequence = List.of(
            WorkflowStage.single(StepType.SECURITY),
            WorkflowStage.parallel(
                    StepType.STUDENTS,
                    StepType.ASSETS,
                    StepType.PREFERENCES,
                    StepType.ROLES),
            WorkflowStage.single(StepType.MODULES));
            
    private final CommonModelMapper commonModelMapper;
    private final KafkaProducerAdapter kafkaProducerAdapter;
    private final EventStateService eventStateService;

    @Override
    public CommonModelResponse execute(CommonModel event) {

        log.info(
                "Executing login use case. transactionId={}, producer={}",
                event.getTransactionId(),
                event.getProducer());
        
        if ("API".equals(event.getProducer())) {
            publishStage(event, sequence.getFirst());
            return commonModelMapper.toResponse(event);
        }

        StepType producerStep = StepType.valueOf(event.getProducer());

        int currentStageIndex = StageUtils.findStageIndex(producerStep, sequence);

        if (currentStageIndex == -1) {
            throw new IllegalStateException(
                    "Producer not found in LOGIN workflow: " + event.getProducer());
        }

        int nextStageIndex = currentStageIndex + 1;

        if (nextStageIndex >= sequence.size()) {
            return complete(event);
        }

        WorkflowStage nextStage = sequence.get(nextStageIndex);

        publishStage(event, nextStage);

        return commonModelMapper.toResponse(event);
    }

    private void publishStage(
            CommonModel event,
            WorkflowStage stage) {

        stage.steps().forEach(step -> {
            kafkaProducerAdapter.publish(
                    step.getTopic(),
                    event.getTransactionId(),
                    event);
        });
    }

    @Override
    public Set<OperationType> getEventTypes() {
        return Set.of(OperationType.LOGIN);
    }

    private CommonModelResponse complete(CommonModel event) {

        log.info(
                "Completing LOGIN workflow. transactionId={}",
                event.getTransactionId());

        CommonModelResponse response = commonModelMapper.toResponse(event);

        kafkaProducerAdapter.publish(
                KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE,
                event.getTransactionId(),
                response);

        return response;
    }
}
