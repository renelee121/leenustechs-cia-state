package mx.com.leenustechs.ciaState.business.useCases;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;

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

            eventStateService.save(
                    event,
                    TransactionStatus.PROCESSING,
                    0,
                    Map.of(
                            StepType.SECURITY,
                            StepStatus.PROCESSING
                    ),
                    null
            );

            publishStage(event, sequence.getFirst());
            return commonModelMapper.toResponse(event);
        }

        StepType producerStep = StepType.valueOf(event.getProducer());

        int currentStageIndex = StageUtils.findStageIndex(producerStep, sequence);

        if (currentStageIndex == -1) {
            throw new IllegalStateException(
                    "Producer not found in LOGIN workflow: " + event.getProducer());
        }

        EventStateResponse state = eventStateService.save(
                event,
                TransactionStatus.PROCESSING,
                currentStageIndex,
                Map.of(
                        producerStep,
                        StepStatus.COMPLETE),
                null);

        WorkflowStage currentStage = sequence.get(currentStageIndex);

        boolean stageComplete = currentStage.steps()
                .stream()
                .allMatch(step ->
                        state.getSteps().get(step) == StepStatus.COMPLETE
                );

        if (!stageComplete) {
            return commonModelMapper.toResponse(event);
        }

        int nextStageIndex = currentStageIndex + 1;

        if (nextStageIndex >= sequence.size()) {
            return complete(event,currentStageIndex);
        }

        WorkflowStage nextStage = sequence.get(nextStageIndex);

        Map<StepType, StepStatus> nextSteps = nextStage.steps()
                .stream()
                .collect(Collectors.toMap(
                        step -> step,
                        step -> StepStatus.PROCESSING
                ));

        eventStateService.save(
                event,
                TransactionStatus.PROCESSING,
                nextStageIndex,
                nextSteps,
                null
        );

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

    private CommonModelResponse complete(
        CommonModel event,
        int currentStageIndex) {

        log.info(
                "Completing LOGIN workflow. transactionId={}",
                event.getTransactionId());

        eventStateService.save(
                event,
                TransactionStatus.COMPLETED,
                currentStageIndex,
                Map.of(
                        StepType.MODULES,
                        StepStatus.COMPLETE
                ),
                event.getPayload()
        );

        CommonModelResponse response = commonModelMapper.toResponse(event);

        kafkaProducerAdapter.publish(
                KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE,
                event.getTransactionId(),
                response);

        return response;
    }
}
