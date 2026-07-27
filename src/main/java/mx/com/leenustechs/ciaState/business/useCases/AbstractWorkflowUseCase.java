package mx.com.leenustechs.ciaState.business.useCases;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.commons.EventOperation;
import mx.com.leenustechs.ciaState.business.utils.commons.StageUtils;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.records.StepTransitionResult;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;

@Slf4j
public abstract class AbstractWorkflowUseCase implements EventOperation {

    private static final String INITIAL_PRODUCER = "API";
    private static final String ERROR_CODE = "error_code";
    private static final String ERROR_MESSAGE = "error_message";
    private static final String EXCEPTION = "exception";

    private final CommonModelMapper commonModelMapper;
    private final KafkaProducerAdapter kafkaProducerAdapter;
    private final EventStateService eventStateService;

    protected AbstractWorkflowUseCase(
            CommonModelMapper commonModelMapper,
            KafkaProducerAdapter kafkaProducerAdapter,
            EventStateService eventStateService) {

        this.commonModelMapper = commonModelMapper;
        this.kafkaProducerAdapter = kafkaProducerAdapter;
        this.eventStateService = eventStateService;
    }

    protected abstract List<WorkflowStage> workflow();

    @Override
    public final CommonModelResponse execute(CommonModel event) {
        List<WorkflowStage> stages = workflow();
        validateWorkflow(stages);

        log.info(
                "Executing workflow. command={}, transactionId={}, producer={}",
                event.getCommand(),
                event.getTransactionId(),
                event.getProducer());

        if (INITIAL_PRODUCER.equals(event.getProducer())) {
            return start(event, stages.getFirst());
        }

        StepType producerStep = parseProducerStep(event);
        int currentStageIndex = StageUtils.findStageIndex(producerStep, stages);

        if (currentStageIndex == -1) {
            throw new IllegalStateException(
                    "Producer not found in %s workflow: %s"
                            .formatted(event.getCommand(), event.getProducer()));
        }

        if (isFailure(event)) {
            return fail(event, producerStep, currentStageIndex);
        }

        StepTransitionResult transition = eventStateService.transitionStep(
                event,
                TransactionStatus.PROCESSING,
                currentStageIndex,
                producerStep,
                StepStatus.COMPLETE,
                null);

        if (!transition.applied()) {
            return commonModelMapper.toResponse(event);
        }

        WorkflowStage currentStage = stages.get(currentStageIndex);
        boolean stageClaimed = eventStateService.claimStageCompletion(
                event.getTransactionId(),
                currentStageIndex,
                currentStage.steps());

        if (!stageClaimed) {
            return commonModelMapper.toResponse(event);
        }

        int nextStageIndex = currentStageIndex + 1;

        if (nextStageIndex >= stages.size()) {
            return complete(event, currentStageIndex);
        }

        WorkflowStage nextStage = stages.get(nextStageIndex);

        eventStateService.save(
                event,
                TransactionStatus.PROCESSING,
                nextStageIndex,
                processingSteps(nextStage),
                null);

        publishStage(event, nextStage);

        return commonModelMapper.toResponse(event);
    }

    protected void publishCompleted(
            CommonModel event,
            CommonModelResponse response) {

        kafkaProducerAdapter.publish(
                KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE,
                event.getTransactionId(),
                response);
    }

    private CommonModelResponse start(
            CommonModel event,
            WorkflowStage firstStage) {

        eventStateService.save(
                event,
                TransactionStatus.PROCESSING,
                0,
                processingSteps(firstStage),
                null);

        publishStage(event, firstStage);

        return commonModelMapper.toResponse(event);
    }

    private CommonModelResponse complete(
            CommonModel event,
            int currentStageIndex) {

        log.info(
                "Completing workflow. command={}, transactionId={}",
                event.getCommand(),
                event.getTransactionId());

        eventStateService.save(
                event,
                TransactionStatus.COMPLETED,
                currentStageIndex,
                Map.of(),
                event.getPayload());

        CommonModelResponse response = commonModelMapper.toResponse(event);
        publishCompleted(event, response);

        return response;
    }

    private CommonModelResponse fail(
            CommonModel event,
            StepType producerStep,
            int currentStageIndex) {

        log.warn(
                "Failing workflow. command={}, transactionId={}, step={}, errorType={}",
                event.getCommand(),
                event.getTransactionId(),
                producerStep,
                failureType(event));

        StepTransitionResult transition = eventStateService.transitionStep(
                event,
                TransactionStatus.FAILED,
                currentStageIndex,
                producerStep,
                StepStatus.ERROR,
                event.getPayload());

        if (!transition.applied()) {
            return commonModelMapper.toResponse(event);
        }

        CommonModelResponse response = commonModelMapper.toResponse(event);
        publishCompleted(event, response);

        return response;
    }

    private void publishStage(
            CommonModel event,
            WorkflowStage stage) {

        stage.steps().forEach(step ->
                kafkaProducerAdapter.publish(
                        step.getTopic(),
                        event.getTransactionId(),
                        event));
    }

    private Map<StepType, StepStatus> processingSteps(
            WorkflowStage stage) {

        return stage.steps().stream()
                .collect(Collectors.toMap(
                        step -> step,
                        step -> StepStatus.PROCESSING));
    }

    private boolean isFailure(CommonModel event) {
        return isControlledFailure(event) || isDeadLetter(event);
    }

    private boolean isControlledFailure(CommonModel event) {
        return event.getPayload() != null
                && event.getPayload().has(ERROR_CODE)
                && event.getPayload().has(ERROR_MESSAGE);
    }

    private boolean isDeadLetter(CommonModel event) {
        return event.getPayload() != null
                && event.getPayload().has(EXCEPTION);
    }

    private String failureType(CommonModel event) {
        return isDeadLetter(event) ? "DEADLETTER" : "FAILURE_RESPONSE";
    }

    private StepType parseProducerStep(CommonModel event) {
        try {
            return StepType.valueOf(event.getProducer());
        } catch (IllegalArgumentException exception) {
            throw new IllegalStateException(
                    "Invalid producer in %s workflow: %s"
                            .formatted(event.getCommand(), event.getProducer()),
                    exception);
        }
    }

    private void validateWorkflow(List<WorkflowStage> stages) {
        if (stages == null || stages.isEmpty()) {
            throw new IllegalStateException(
                    "Workflow must contain at least one stage");
        }

        if (stages.stream().anyMatch(stage ->
                stage == null
                        || stage.steps() == null
                        || stage.steps().isEmpty())) {

            throw new IllegalStateException(
                    "Workflow stages must contain at least one step");
        }
    }
}
