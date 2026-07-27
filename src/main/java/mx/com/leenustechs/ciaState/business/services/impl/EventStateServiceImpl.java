package mx.com.leenustechs.ciaState.business.services.impl;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import mx.com.leenustechs.ciaState.models.records.StepTransitionResult;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;

@Service
@RequiredArgsConstructor
public class EventStateServiceImpl implements EventStateService{

    private static final String KEY_PREFIX = "eventState:";
    private static final String STEP_FIELD_TEMPLATE = "steps.[%s]";
    private static final String STAGE_FIELD_TEMPLATE = "completedStages.[%d]";

    private static final RedisScript<Long> TRANSITION_STEP_SCRIPT =
            RedisScript.of("""
                    local current = redis.call('HGET', KEYS[1], ARGV[1])
                    if current == ARGV[2] or current == ARGV[3] then
                        return 0
                    end
                    redis.call('HSET', KEYS[1], ARGV[1], ARGV[4])
                    return 1
                    """, Long.class);

    private static final RedisScript<Long> CLAIM_STAGE_SCRIPT =
            RedisScript.of("""
                    for index = 3, #ARGV do
                        if redis.call('HGET', KEYS[1], ARGV[index]) ~= ARGV[1] then
                            return 0
                        end
                    end
                    return redis.call('HSETNX', KEYS[1], ARGV[2], ARGV[1])
                    """, Long.class);

    private final EventStateRepository eventStateRepository;
    private final EventStateModelMapper eventStateModelMapper;
    private final EventStateInputMapper eventStateInputMapper;
    private final RedisTemplate<String, Object> redisTemplate;
    private final RedisKeyValueTemplate redisKeyValueTemplate;
    @Value("${redis.ttl}") private Long ttl;

    @Override
    public EventStateResponse findByTransactionId(String transactionId) {
        EventStateEntity entity = eventStateRepository
                .findById(transactionId)
                .orElseThrow(() ->
                        new TransactionNotFoundException(transactionId)
                );
        EventStateModel model = eventStateModelMapper.toModel(
            entity
        );
        EventStateResponse response = eventStateModelMapper.toResponse(
            model
        );
        return response;
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
                now
        );

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

        EventStateEntity saved =
                eventStateRepository.save(entity);

        return eventStateModelMapper.toResponse(
                eventStateModelMapper.toModel(saved)
        );
    }

    @Override
    public StepTransitionResult transitionStep(
            CommonModel event,
            TransactionStatus status,
            Integer currentStage,
            StepType step,
            StepStatus stepStatus,
            JsonNode result) {

        String key = KEY_PREFIX + event.getTransactionId();
        String stepField = STEP_FIELD_TEMPLATE.formatted(step.name());

        Long applied = redisTemplate.execute(
                TRANSITION_STEP_SCRIPT,
                new StringRedisSerializer(),
                new GenericToStringSerializer<>(Long.class),
                List.of(key),
                stepField,
                StepStatus.COMPLETE.name(),
                StepStatus.ERROR.name(),
                stepStatus.name());

        if (!Long.valueOf(1L).equals(applied)) {
            return new StepTransitionResult(
                    findByTransactionId(event.getTransactionId()),
                    false);
        }

        PartialUpdate<EventStateEntity> update =
                new PartialUpdate<>(
                        event.getTransactionId(),
                        EventStateEntity.class)
                        .set("producer", event.getProducer())
                        .set("command", event.getCommand())
                        .set("status", status)
                        .set("currentStage", currentStage)
                        .set("updatedAt", Instant.now())
                        .set("ttl", ttl)
                        .refreshTtl(true);

        if (event.getPayload() != null) {
            update.set("payload", event.getPayload());
        }

        if (result != null) {
            update.set("result", result);
        }

        redisKeyValueTemplate.update(update);

        return new StepTransitionResult(
                findByTransactionId(event.getTransactionId()),
                true);
    }

    @Override
    public boolean claimStageCompletion(
            String transactionId,
            Integer stageIndex,
            Iterable<StepType> steps) {

        List<Object> arguments = new ArrayList<>();
        arguments.add(StepStatus.COMPLETE.name());
        arguments.add(STAGE_FIELD_TEMPLATE.formatted(stageIndex));

        for (StepType step : steps) {
            arguments.add(STEP_FIELD_TEMPLATE.formatted(step.name()));
        }

        Long claimed = redisTemplate.execute(
                CLAIM_STAGE_SCRIPT,
                new StringRedisSerializer(),
                new GenericToStringSerializer<>(Long.class),
                List.of(KEY_PREFIX + transactionId),
                arguments.toArray());

        return Long.valueOf(1L).equals(claimed);
    }
}
