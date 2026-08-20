package mx.com.leenustechs.ciaState.business.useCases;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.commons.EventOperation;
import mx.com.leenustechs.ciaState.business.utils.commons.StageUtils;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.records.StageResultSnapshot;
import mx.com.leenustechs.ciaState.models.records.StepTransitionResult;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

@Slf4j
public abstract class AbstractWorkflowUseCase implements EventOperation {

    private static final String INITIAL_PRODUCER = "CIA_API";
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

        /*
         * A stage is not eligible to advance until every COMPLETE step also
         * has a persisted result. transitionStep guarantees that property
         * atomically; this snapshot makes it explicit at the orchestration
         * boundary.
         */
        Optional<StageResultSnapshot> completedStage =
                eventStateService.completedStageSnapshot(
                        event.getTransactionId(),
                        currentStageIndex,
                        currentStage.steps());

        if (completedStage.isEmpty()) {
            return commonModelMapper.toResponse(event);
        }

        /*
         * Aggregate before claiming the stage. If the parallel results are
         * semantically incompatible, fail here without permanently claiming a
         * stage whose output cannot be constructed.
         */
        JsonNode aggregatedPayload = aggregateStagePayload(
                completedStage.get(),
                currentStage);

        boolean stageClaimed = eventStateService.claimStageCompletion(
                event.getTransactionId(),
                currentStageIndex,
                currentStage.steps());

        if (!stageClaimed) {
            return commonModelMapper.toResponse(event);
        }

        CommonModel stageEvent = withPayload(event, aggregatedPayload);

        int nextStageIndex = currentStageIndex + 1;

        if (nextStageIndex >= stages.size()) {
            return complete(stageEvent, currentStageIndex);
        }

        WorkflowStage nextStage = stages.get(nextStageIndex);

        eventStateService.save(
                stageEvent,
                TransactionStatus.PROCESSING,
                nextStageIndex,
                processingSteps(nextStage),
                null);

        publishStage(stageEvent, nextStage);

        return commonModelMapper.toResponse(stageEvent);
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

    /**
     * Converts a completed stage into the context for the next stage.
     *
     * <p>For a parallel stage, the input payload is the immutable baseline.
     * Each worker result is treated as a branch derived from that baseline.
     * Unchanged baseline fields are ignored; independent additions are merged;
     * conflicting writes to the same path are rejected deterministically.</p>
     */
    private JsonNode aggregateStagePayload(
            StageResultSnapshot snapshot,
            WorkflowStage stage) {

        JsonNode inputPayload = snapshot.inputPayload();

        if (!stage.isParallel()) {
            StepType step = stage.steps().getFirst();
            JsonNode stepResult = snapshot.stepResults().get(step);

            if (stepResult == null) {
                throw new IllegalStateException(
                        "Missing result for completed step: " + step);
            }

            return mergeSingleResult(inputPayload, stepResult, step);
        }

        if (inputPayload != null
                && !inputPayload.isNull()
                && !inputPayload.isObject()) {
            throw new IllegalStateException(
                    "Parallel stage input payload must be a JSON object");
        }

        ObjectNode baseline =
                inputPayload == null || inputPayload.isNull()
                        ? JsonNodeFactory.instance.objectNode()
                        : ((ObjectNode) inputPayload).deepCopy();

        ObjectNode merged = baseline.deepCopy();
        Map<String, StepType> owners = new HashMap<>();

        /*
         * Iterate the workflow declaration, never Map iteration order. That
         * makes aggregation independent from response arrival order.
         */
        for (StepType step : stage.steps()) {
            JsonNode stepResult = snapshot.stepResults().get(step);

            if (stepResult == null) {
                throw new IllegalStateException(
                        "Missing result for completed parallel step: " + step);
            }

            if (stepResult.isNull()) {
                continue;
            }

            if (!stepResult.isObject()) {
                throw new IllegalStateException(
                        "Parallel step result must be a JSON object. step="
                                + step);
            }

            mergeObject(
                    merged,
                    baseline,
                    (ObjectNode) stepResult,
                    owners,
                    step,
                    "");
        }

        return merged;
    }

    private JsonNode mergeSingleResult(
            JsonNode inputPayload,
            JsonNode stepResult,
            StepType step) {

        if (stepResult.isNull()) {
            return inputPayload == null
                    ? null
                    : inputPayload.deepCopy();
        }

        if (inputPayload == null || inputPayload.isNull()) {
            return stepResult.deepCopy();
        }

        if (inputPayload.isObject() && stepResult.isObject()) {
            ObjectNode baseline = ((ObjectNode) inputPayload).deepCopy();
            ObjectNode merged = baseline.deepCopy();

            mergeObject(
                    merged,
                    baseline,
                    (ObjectNode) stepResult,
                    new HashMap<>(),
                    step,
                    "");

            return merged;
        }

        return stepResult.deepCopy();
    }

    private void mergeObject(
            ObjectNode target,
            ObjectNode baseline,
            ObjectNode incoming,
            Map<String, StepType> owners,
            StepType sourceStep,
            String prefix) {

        for (Map.Entry<String, JsonNode> property : incoming.properties()) {
            String name = property.getKey();
            String path = prefix.isEmpty() ? name : prefix + "." + name;

            JsonNode baseValue = baseline.get(name);
            JsonNode incomingValue = property.getValue();

            /* Branch merely repeats the stage input: no write to merge. */
            if (baseValue != null && baseValue.equals(incomingValue)) {
                continue;
            }

            JsonNode currentValue = target.get(name);

            if (incomingValue.isObject()
                    && (baseValue == null || baseValue.isObject())) {

                ObjectNode baseObject =
                        baseValue == null
                                ? JsonNodeFactory.instance.objectNode()
                                : (ObjectNode) baseValue;

                ObjectNode targetObject;

                if (currentValue == null
                        || (baseValue != null
                                && currentValue.equals(baseValue))) {
                    targetObject = baseObject.deepCopy();
                    target.set(name, targetObject);
                } else if (currentValue.isObject()) {
                    targetObject = (ObjectNode) currentValue;
                } else {
                    throw mergeConflict(path, owners.get(path), sourceStep);
                }

                mergeObject(
                        targetObject,
                        baseObject,
                        (ObjectNode) incomingValue,
                        owners,
                        sourceStep,
                        path);
                continue;
            }

            if (currentValue == null
                    || (baseValue != null
                            && currentValue.equals(baseValue))) {
                target.set(name, incomingValue.deepCopy());
                owners.put(path, sourceStep);
                continue;
            }

            if (currentValue.equals(incomingValue)) {
                continue;
            }

            throw mergeConflict(path, owners.get(path), sourceStep);
        }
    }

    private IllegalStateException mergeConflict(
            String path,
            StepType previousStep,
            StepType incomingStep) {

        String previous =
                previousStep == null
                        ? "another parallel step"
                        : previousStep.name();

        return new IllegalStateException(
                "Conflicting parallel results at path '%s': %s vs %s"
                        .formatted(path, previous, incomingStep.name()));
    }

    private CommonModel withPayload(
            CommonModel event,
            JsonNode payload) {

        if (Objects.equals(event.getPayload(), payload)) {
            return event;
        }

        return new CommonModel(
                event.getTransactionId(),
                event.getProducer(),
                event.getCommand(),
                payload);
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
