package mx.com.leenustechs.ciaState.business.services.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.data.redis.core.RedisKeyValueTemplate;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.test.util.ReflectionTestUtils;

import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
import mx.com.leenustechs.ciaState.business.utils.exceptions.TransactionNotFoundException;
import mx.com.leenustechs.ciaState.business.utils.mappers.EventStateInputMapper;
import mx.com.leenustechs.ciaState.business.utils.mappers.EventStateModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.StageResultSnapshot;
import mx.com.leenustechs.ciaState.models.records.StepTransitionResult;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@SuppressWarnings({ "unchecked", "rawtypes" })
class EventStateServiceImplTest {

    private final EventStateRepository repository = mock(EventStateRepository.class);
    private final EventStateModelMapper modelMapper = mock(EventStateModelMapper.class);
    private final EventStateInputMapper inputMapper = mock(EventStateInputMapper.class);
    private final RedisTemplate<String, Object> redisTemplate = mock(RedisTemplate.class);
    private final RedisKeyValueTemplate keyValueTemplate = mock(RedisKeyValueTemplate.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonNode payload = objectMapper.createObjectNode().put("value", 1);
    private EventStateServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new EventStateServiceImpl(
                repository,
                modelMapper,
                inputMapper,
                redisTemplate,
                keyValueTemplate,
                objectMapper);
        ReflectionTestUtils.setField(service, "ttl", 300L);
    }

    @Test
    void findsAndMapsStoredTransactions() {
        EventStateEntity entity = new EventStateEntity();
        EventStateModel model = new EventStateModel();
        EventStateResponse response = new EventStateResponse();
        when(repository.findById("tx")).thenReturn(Optional.of(entity));
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);

        assertSame(response, service.findByTransactionId("tx"));
    }

    @Test
    void reportsMissingTransactions() {
        when(repository.findById("missing")).thenReturn(Optional.empty());

        TransactionNotFoundException exception = assertThrows(
                TransactionNotFoundException.class,
                () -> service.findByTransactionId("missing"));

        assertTrue(exception.getMessage().contains("missing"));
    }

    @Test
    void createsNewStateWithTtl() {
        CommonModel event = event(null);
        EventStateModel model = new EventStateModel();
        model.setTransactionId("tx");
        EventStateEntity entity = new EventStateEntity();
        EventStateResponse response = new EventStateResponse();
        when(inputMapper.toEventStateModel(
                any(), any(), any(), any(), any(), any(), any())).thenReturn(model);
        when(repository.findById("tx")).thenReturn(Optional.empty());
        when(modelMapper.toEntity(model)).thenReturn(entity);
        when(repository.save(entity)).thenReturn(entity);
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);

        assertSame(response, service.save(
                event, TransactionStatus.PROCESSING, 0,
                Map.of(StepType.SECURITY, StepStatus.PROCESSING), null));
        assertEquals(300L, entity.getTtl());
    }

    @Test
    void updatesExistingStateBeforeSaving() {
        CommonModel event = event(payload);
        EventStateModel model = new EventStateModel();
        model.setTransactionId("tx");
        EventStateEntity entity = new EventStateEntity();
        EventStateResponse response = new EventStateResponse();
        when(inputMapper.toEventStateModel(
                any(), any(), any(), any(), any(), any(), any())).thenReturn(model);
        when(repository.findById("tx")).thenReturn(Optional.of(entity));
        when(repository.save(entity)).thenReturn(entity);
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);

        assertSame(response, service.save(
                event, TransactionStatus.PROCESSING, 1,
                Map.of(StepType.ASSETS, StepStatus.PROCESSING), payload));
        verify(modelMapper).updateFromModel(model, entity);
        verify(modelMapper, never()).toEntity(model);
    }

    @Test
    void returnsExistingStateWhenTransitionWasAlreadyTerminal() {
        EventStateResponse response = new EventStateResponse();
        EventStateEntity entity = new EventStateEntity();
        EventStateModel model = new EventStateModel();
        when(redisTemplate.execute(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(0L);
        when(repository.findById("tx")).thenReturn(Optional.of(entity));
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);

        StepTransitionResult result = service.transitionStep(
                event(payload), TransactionStatus.PROCESSING, 0,
                StepType.SECURITY, StepStatus.COMPLETE, null);

        assertFalse(result.applied());
        assertSame(response, result.state());
        verify(keyValueTemplate, never()).update(any());
    }

    @Test
    void appliesTransitionAndPersistsOnlyAuxiliaryPartialUpdate() {
        EventStateResponse response = new EventStateResponse();
        EventStateEntity entity = new EventStateEntity();
        EventStateModel model = new EventStateModel();
        when(redisTemplate.execute(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(1L);
        when(repository.findById("tx")).thenReturn(Optional.of(entity));
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);

        StepTransitionResult result = service.transitionStep(
                event(payload), TransactionStatus.PROCESSING, 0,
                StepType.SECURITY, StepStatus.COMPLETE, null);

        assertTrue(result.applied());
        assertSame(response, result.state());
        verify(keyValueTemplate).update(any());
    }

    @Test
    void rejectsInconsistentTransitionContractBeforeTouchingRedis() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.transitionStep(
                        event(payload),
                        TransactionStatus.COMPLETED,
                        0,
                        StepType.SECURITY,
                        StepStatus.COMPLETE,
                        null));

        verify(redisTemplate, never()).execute(
                any(), any(), any(), any(), any(Object[].class));
    }

    @Test
    void returnsCompletedStageSnapshotWithEveryPersistedResult() {
        JsonNode input = objectMapper.createObjectNode().put("userId", 42);
        JsonNode students = objectMapper.createObjectNode().put("students", 1);
        JsonNode assets = objectMapper.createObjectNode().put("assets", 1);

        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        entity.setStatus(TransactionStatus.PROCESSING);
        entity.setCurrentStage(1);
        entity.setPayload(input);
        entity.setSteps(Map.of(
                StepType.STUDENTS, StepStatus.COMPLETE,
                StepType.ASSETS, StepStatus.COMPLETE));
        entity.setStepResults(Map.of(
                "1:STUDENTS", students.toString(),
                "1:ASSETS", assets.toString()));

        when(repository.findById("tx")).thenReturn(Optional.of(entity));

        StageResultSnapshot snapshot = service.completedStageSnapshot(
                "tx",
                1,
                List.of(StepType.STUDENTS, StepType.ASSETS))
                .orElseThrow();

        assertEquals(input, snapshot.inputPayload());
        assertEquals(students, snapshot.stepResults().get(StepType.STUDENTS));
        assertEquals(assets, snapshot.stepResults().get(StepType.ASSETS));
    }

    @Test
    void doesNotExposeSnapshotUntilEveryStepIsComplete() {
        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        entity.setStatus(TransactionStatus.PROCESSING);
        entity.setCurrentStage(1);
        entity.setSteps(Map.of(
                StepType.STUDENTS, StepStatus.COMPLETE,
                StepType.ASSETS, StepStatus.PROCESSING));

        when(repository.findById("tx")).thenReturn(Optional.of(entity));

        assertTrue(service.completedStageSnapshot(
                "tx",
                1,
                List.of(StepType.STUDENTS, StepType.ASSETS))
                .isEmpty());
    }

    @Test
    void rejectsImpossibleCompleteWithoutPersistedResult() {
        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        entity.setStatus(TransactionStatus.PROCESSING);
        entity.setCurrentStage(1);
        entity.setSteps(Map.of(StepType.ASSETS, StepStatus.COMPLETE));
        entity.setStepResults(Map.of());

        when(repository.findById("tx")).thenReturn(Optional.of(entity));

        IllegalStateException exception = assertThrows(
                IllegalStateException.class,
                () -> service.completedStageSnapshot(
                        "tx",
                        1,
                        List.of(StepType.ASSETS)));

        assertTrue(exception.getMessage().contains("result is missing"));
    }

    @Test
    void claimsStageOnlyWhenRedisReturnsOne() {
        when(redisTemplate.execute(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(1L, 0L);

        assertTrue(service.claimStageCompletion(
                "tx", 1, List.of(StepType.ASSETS, StepType.ROLES)));
        assertFalse(service.claimStageCompletion(
                "tx", 1, List.of(StepType.ASSETS)));
        assertFalse(service.claimStageCompletion("tx", 1, List.of()));
    }

    private CommonModel event(JsonNode eventPayload) {
        return new CommonModel("tx", "SECURITY", OperationType.LOGIN, eventPayload);
    }
}
