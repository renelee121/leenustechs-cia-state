package mx.com.leenustechs.ciaState.business.services.impl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.PartialUpdate;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.data.redis.serializer.GenericToStringSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;
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

    private static final String KEY_PREFIX = "eventState:";
    private static final String STEP_FIELD_TEMPLATE = "steps.[%s]";
    private static final String STEP_RESULT_FIELD_TEMPLATE = "stepResults.[%s]";
    private static final String STAGE_FIELD_TEMPLATE = "completedStages.[%d]";
    private static final String STEP_RESULT_KEY_TEMPLATE = "%d:%s";

    /**
     * Owns the atomic terminal transition of one worker step.
     *
     * <p>The result and the terminal step status are written in the same Lua
     * execution. Therefore a visible COMPLETE/ERROR step always has its
     * persisted result. The script also rejects responses for an old stage or
     * for an already terminal workflow.</p>
     */
    private static final RedisScript<Long> TRANSITION_STEP_SCRIPT =
            RedisScript.of("""
                    local transactionStatus =
                            redis.call('HGET', KEYS[1], 'status')

                    if transactionStatus == ARGV[1]
                            or transactionStatus == ARGV[2] then
                        return 0
                    end

                    local storedStage =
                            redis.call('HGET', KEYS[1], 'currentStage')

                    if storedStage ~= ARGV[3] then
                        return 0
                    end

                    local currentStep =
                            redis.call('HGET', KEYS[1], ARGV[4])

                    if currentStep ~= ARGV[5] then
                        return 0
                    end

                    redis.call(
                            'HSET',
                            KEYS[1],
                            ARGV[6],
                            ARGV[7])

                    redis.call(
                            'HSET',
                            KEYS[1],
                            ARGV[4],
                            ARGV[8])

                    if ARGV[8] == ARGV[9] then
                        redis.call(
                                'HSET',
                                KEYS[1],
                                'status',
                                ARGV[1])
                    end

                    redis.call(
                            'EXPIRE',
                            KEYS[1],
                            tonumber(ARGV[10]))

                    return 1
                    """, Long.class);

    /**
     * Atomically elects exactly one owner for advancing a completed stage.
     * The claim is valid only while the workflow is PROCESSING and still on
     * the expected stage.
     */
    private static final RedisScript<Long> CLAIM_STAGE_SCRIPT =
            RedisScript.of("""
                    local transactionStatus =
                            redis.call('HGET', KEYS[1], 'status')

                    if transactionStatus ~= ARGV[1] then
                        return 0
                    end

                    local storedStage =
                            redis.call('HGET', KEYS[1], 'currentStage')

                    if storedStage ~= ARGV[2] then
                        return 0
                    end

                    for index = 5, #ARGV do
                        if redis.call(
                                'HGET',
                                KEYS[1],
                                ARGV[index]) ~= ARGV[3] then
                            return 0
                        end
                    end

                    return redis.call(
                            'HSETNX',
                            KEYS[1],
                            ARGV[4],
                            ARGV[3])
                    """, Long.class);

    private final EventStateRepository eventStateRepository;
    private final EventStateModelMapper eventStateModelMapper;
    private final EventStateInputMapper eventStateInputMapper;
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisKeyValueTemplate redisKeyValueTemplate;
    private final ObjectMapper objectMapper;

    @Value("${redis.ttl}")
    private Long ttl;

    @Override
    public EventStateResponse findByTransactionId(String transactionId) {
        EventStateEntity entity = eventStateRepository
                .findById(transactionId)
                .orElseThrow(() ->
                        new TransactionNotFoundException(transactionId));

        EventStateModel model = eventStateModelMapper.toModel(entity);
        return eventStateModelMapper.toResponse(model);
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

        EventStateEntity entity = eventStateRepository
                .findById(model.getTransactionId())
                .map(existing -> {
                    eventStateModelMapper.updateFromModel(model, existing);
                    return existing;
                })
                .orElseGet(() -> {
                    EventStateEntity created =
                            eventStateModelMapper.toEntity(model);
                    created.setTtl(ttl);
                    return created;
                });

        EventStateEntity saved = eventStateRepository.save(entity);

        return eventStateModelMapper.toResponse(
                eventStateModelMapper.toModel(saved));
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

        String key = KEY_PREFIX + event.getTransactionId();
        String stepField = STEP_FIELD_TEMPLATE.formatted(step.name());
        String resultKey = stepResultKey(currentStage, step);
        String stepResultField =
                STEP_RESULT_FIELD_TEMPLATE.formatted(resultKey);

        String serializedStepResult =
                event.getPayload() == null
                        ? "null"
                        : event.getPayload().toString();

        Long applied = redisTemplate.execute(
                TRANSITION_STEP_SCRIPT,
                new StringRedisSerializer(),
                new GenericToStringSerializer<>(Long.class),
                List.of(key),
                TransactionStatus.FAILED.name(),
                TransactionStatus.COMPLETED.name(),
                String.valueOf(currentStage),
                stepField,
                StepStatus.PROCESSING.name(),
                stepResultField,
                serializedStepResult,
                stepStatus.name(),
                StepStatus.ERROR.name(),
                String.valueOf(ttl));

        if (!Long.valueOf(1L).equals(applied)) {
            return new StepTransitionResult(
                    findByTransactionId(event.getTransactionId()),
                    false);
        }

        /*
         * Important: this partial update intentionally does NOT touch payload,
         * producer, command, currentStage, or PROCESSING status. Those are
         * workflow/stage-owned fields. A worker owns only its terminal status
         * and its own stepResults slot (written atomically by Lua above).
         */
        PartialUpdate<EventStateEntity> update =
                new PartialUpdate<>(
                        event.getTransactionId(),
                        EventStateEntity.class)
                        .set("updatedAt", Instant.now());

        if (StepStatus.ERROR.equals(stepStatus) && result != null) {
            update.set("result", result);
        }

        redisKeyValueTemplate.update(update);

        return new StepTransitionResult(
                findByTransactionId(event.getTransactionId()),
                true);
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

        JsonNode inputPayload =
                entity.getPayload() == null
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

        List<Object> arguments = new ArrayList<>();
        arguments.add(TransactionStatus.PROCESSING.name());
        arguments.add(String.valueOf(stageIndex));
        arguments.add(StepStatus.COMPLETE.name());
        arguments.add(STAGE_FIELD_TEMPLATE.formatted(stageIndex));

        boolean hasSteps = false;

        for (StepType step : steps) {
            hasSteps = true;
            arguments.add(STEP_FIELD_TEMPLATE.formatted(step.name()));
        }

        if (!hasSteps) {
            return false;
        }

        Long claimed = redisTemplate.execute(
                CLAIM_STAGE_SCRIPT,
                new StringRedisSerializer(),
                new GenericToStringSerializer<>(Long.class),
                List.of(KEY_PREFIX + transactionId),
                arguments.toArray());

        return Long.valueOf(1L).equals(claimed);
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
