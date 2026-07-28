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
    private final JsonNode payload = new ObjectMapper().createObjectNode().put("value", 1);
    private EventStateServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new EventStateServiceImpl(
                repository, modelMapper, inputMapper, redisTemplate, keyValueTemplate);
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
                StepType.SECURITY, StepStatus.COMPLETE, payload);

        assertFalse(result.applied());
        assertSame(response, result.state());
        verify(keyValueTemplate, never()).update(any());
    }

    @Test
    void appliesTransitionAndPersistsPartialUpdate() {
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
                StepType.SECURITY, StepStatus.COMPLETE, payload);

        assertTrue(result.applied());
        assertSame(response, result.state());
        verify(keyValueTemplate).update(any());
    }

    @Test
    void claimsStageOnlyWhenRedisReturnsOne() {
        when(redisTemplate.execute(any(), any(), any(), any(), any(Object[].class)))
                .thenReturn(1L, 0L, null);

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
