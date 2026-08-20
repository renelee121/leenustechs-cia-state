package mx.com.leenustechs.ciaState.business.services.impl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.stereotype.Service;

import lombok.RequiredArgsConstructor;
import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.exceptions.TransactionNotFoundException;
import mx.com.leenustechs.ciaState.business.utils.mappers.EventStateInputMapper;
import mx.com.leenustechs.ciaState.business.utils.mappers.EventStateModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.AtomicStateUpdate;
import mx.com.leenustechs.ciaState.models.records.StageResultSnapshot;
import mx.com.leenustechs.ciaState.models.records.StepTransitionResult;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Service
@RequiredArgsConstructor
public class EventStateServiceImpl implements EventStateService {

    private static final String STEP_RESULT_KEY_TEMPLATE = "%d:%s";

    private final EventStateRepository eventStateRepository;
    private final EventStateModelMapper eventStateModelMapper;
    private final EventStateInputMapper eventStateInputMapper;
    private final ObjectMapper objectMapper;

    @Override
    public EventStateResponse findByTransactionId(String transactionId) {
        EventStateEntity entity = eventStateRepository
                .findById(transactionId)
                .orElseThrow(() ->
                        new TransactionNotFoundException(transactionId));

        return toResponse(entity);
    }

    @Override
    public EventStateResponse save(
            CommonModel event,
            TransactionStatus status,
            Integer currentStage,
            Map<StepType, StepStatus> steps,
            JsonNode result) {

        Instant now = Instant.now();

        EventStateModel model = eventStateInputMapper.toEventStateModel(
                event,
                status,
                currentStage,
                steps,
                result,
                now,
                now);

        AtomicStateUpdate update = eventStateRepository.updateAtomically(
                model.getTransactionId(),
                current -> Optional.of(
                        current.map(existing -> {
                            eventStateModelMapper.updateFromModel(model, existing);
                            return existing;
                        }).orElseGet(() ->
                                eventStateModelMapper.toEntity(model))));

        EventStateEntity saved = update.state()
                .orElseThrow(() -> new IllegalStateException(
                        "Firebase RTDB save completed without workflow state. "
                                + "transactionId=" + model.getTransactionId()));

        return toResponse(saved);
    }

    @Override
    public StepTransitionResult transitionStep(
            CommonModel event,
            TransactionStatus status,
            Integer currentStage,
            StepType step,
            StepStatus stepStatus,
            JsonNode result) {

        validateTransition(status, stepStatus);

        String transactionId = event.getTransactionId();
        String resultKey = stepResultKey(currentStage, step);
        String serializedStepResult = event.getPayload() == null
                ? "null"
                : event.getPayload().toString();
        Instant now = Instant.now();

        AtomicStateUpdate update = eventStateRepository.updateAtomically(
                transactionId,
                current -> current.flatMap(entity -> {
                    if (TransactionStatus.FAILED.equals(entity.getStatus())
                            || TransactionStatus.COMPLETED.equals(entity.getStatus())) {
                        return Optional.empty();
                    }

                    if (!currentStage.equals(entity.getCurrentStage())) {
                        return Optional.empty();
                    }

                    if (entity.getSteps() == null
                            || !StepStatus.PROCESSING.equals(
                                    entity.getSteps().get(step))) {
                        return Optional.empty();
                    }

                    Map<StepType, StepStatus> updatedSteps =
                            new EnumMap<>(StepType.class);
                    updatedSteps.putAll(entity.getSteps());
                    updatedSteps.put(step, stepStatus);
                    entity.setSteps(updatedSteps);

                    Map<String, String> updatedResults =
                            entity.getStepResults() == null
                                    ? new LinkedHashMap<>()
                                    : new LinkedHashMap<>(entity.getStepResults());
                    updatedResults.put(resultKey, serializedStepResult);
                    entity.setStepResults(updatedResults);

                    if (StepStatus.ERROR.equals(stepStatus)) {
                        entity.setStatus(TransactionStatus.FAILED);
                    }

                    if (StepStatus.ERROR.equals(stepStatus) && result != null) {
                        entity.setResult(result.deepCopy());
                    }

                    entity.setUpdatedAt(now);
                    return Optional.of(entity);
                }));

        EventStateEntity state = update.state()
                .orElseThrow(() ->
                        new TransactionNotFoundException(transactionId));

        return new StepTransitionResult(
                toResponse(state),
                update.applied());
    }

    @Override
    public Optional<StageResultSnapshot> completedStageSnapshot(
            String transactionId,
            Integer stageIndex,
            Iterable<StepType> steps) {

        EventStateEntity entity = eventStateRepository
                .findById(transactionId)
                .orElseThrow(() ->
                        new TransactionNotFoundException(transactionId));

        if (!TransactionStatus.PROCESSING.equals(entity.getStatus())
                || !stageIndex.equals(entity.getCurrentStage())) {
            return Optional.empty();
        }

        List<StepType> requestedSteps = new ArrayList<>();

        for (StepType step : steps) {
            requestedSteps.add(step);

            if (entity.getSteps() == null
                    || !StepStatus.COMPLETE.equals(
                            entity.getSteps().get(step))) {
                return Optional.empty();
            }
        }

        if (requestedSteps.isEmpty()) {
            return Optional.empty();
        }

        Map<String, String> persistedResults = entity.getStepResults();

        if (persistedResults == null) {
            throw new IllegalStateException(
                    "Completed stage has no persisted step results. "
                            + "transactionId=" + transactionId
                            + ", stageIndex=" + stageIndex);
        }

        Map<StepType, JsonNode> parsedResults =
                new EnumMap<>(StepType.class);

        for (StepType step : requestedSteps) {
            String resultKey = stepResultKey(stageIndex, step);
            String serializedResult = persistedResults.get(resultKey);

            if (serializedResult == null) {
                throw new IllegalStateException(
                        "Step is COMPLETE but its result is missing. "
                                + "transactionId=%s, stageIndex=%d, step=%s"
                                        .formatted(
                                                transactionId,
                                                stageIndex,
                                                step));
            }

            parsedResults.put(
                    step,
                    parseStepResult(
                            transactionId,
                            stageIndex,
                            step,
                            serializedResult));
        }

        JsonNode inputPayload = entity.getPayload() == null
                ? null
                : entity.getPayload().deepCopy();

        return Optional.of(
                new StageResultSnapshot(
                        inputPayload,
                        Map.copyOf(parsedResults)));
    }

    @Override
    public boolean claimStageCompletion(
            String transactionId,
            Integer stageIndex,
            Iterable<StepType> steps) {

        List<StepType> requestedSteps = new ArrayList<>();
        steps.forEach(requestedSteps::add);

        if (requestedSteps.isEmpty()) {
            return false;
        }

        Instant now = Instant.now();

        AtomicStateUpdate update = eventStateRepository.updateAtomically(
                transactionId,
                current -> current.flatMap(entity -> {
                    if (!TransactionStatus.PROCESSING.equals(entity.getStatus())
                            || !stageIndex.equals(entity.getCurrentStage())) {
                        return Optional.empty();
                    }

                    for (StepType step : requestedSteps) {
                        if (entity.getSteps() == null
                                || !StepStatus.COMPLETE.equals(
                                        entity.getSteps().get(step))) {
                            return Optional.empty();
                        }
                    }

                    Map<Integer, StepStatus> completedStages =
                            entity.getCompletedStages() == null
                                    ? new HashMap<>()
                                    : new HashMap<>(entity.getCompletedStages());

                    if (completedStages.containsKey(stageIndex)) {
                        return Optional.empty();
                    }

                    completedStages.put(stageIndex, StepStatus.COMPLETE);
                    entity.setCompletedStages(completedStages);
                    entity.setUpdatedAt(now);
                    return Optional.of(entity);
                }));

        return update.applied();
    }

    private EventStateResponse toResponse(EventStateEntity entity) {
        EventStateModel model = eventStateModelMapper.toModel(entity);
        return eventStateModelMapper.toResponse(model);
    }

    private String stepResultKey(
            Integer stageIndex,
            StepType step) {

        return STEP_RESULT_KEY_TEMPLATE.formatted(
                stageIndex,
                step.name());
    }

    private JsonNode parseStepResult(
            String transactionId,
            Integer stageIndex,
            StepType step,
            String serializedResult) {

        try {
            return objectMapper.readTree(serializedResult);
        } catch (Exception exception) {
            throw new IllegalStateException(
                    "Unable to deserialize persisted step result. "
                            + "transactionId=%s, stageIndex=%d, step=%s"
                                    .formatted(
                                            transactionId,
                                            stageIndex,
                                            step),
                    exception);
        }
    }

    private void validateTransition(
            TransactionStatus status,
            StepStatus stepStatus) {

        boolean validCompletion =
                StepStatus.COMPLETE.equals(stepStatus)
                        && TransactionStatus.PROCESSING.equals(status);

        boolean validFailure =
                StepStatus.ERROR.equals(stepStatus)
                        && TransactionStatus.FAILED.equals(status);

        if (!validCompletion && !validFailure) {
            throw new IllegalArgumentException(
                    "Invalid terminal step transition. "
                            + "transactionStatus=%s, stepStatus=%s"
                                    .formatted(status, stepStatus));
        }
    }
}
