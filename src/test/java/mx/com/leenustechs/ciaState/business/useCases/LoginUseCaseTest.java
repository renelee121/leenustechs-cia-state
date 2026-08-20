package mx.com.leenustechs.ciaState.business.useCases;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.records.StageResultSnapshot;
import mx.com.leenustechs.ciaState.models.records.StepTransitionResult;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.JsonNodeFactory;
import tools.jackson.databind.node.ObjectNode;

@ExtendWith(MockitoExtension.class)
class LoginUseCaseTest {

    @Mock
    private CommonModelMapper commonModelMapper;

    @Mock
    private KafkaProducerAdapter kafkaProducerAdapter;

    @Mock
    private EventStateService eventStateService;

    private LoginUseCase useCase;

    @BeforeEach
    void setUp() {
        useCase = new LoginUseCase(
                commonModelMapper,
                kafkaProducerAdapter,
                eventStateService);
    }

    @Test
    void startsWorkflowAndPublishesFirstStage() {
        CommonModel event = eventFrom("CIA_API");

        useCase.execute(event);

        verify(eventStateService).save(
                event,
                TransactionStatus.PROCESSING,
                0,
                Map.of(StepType.SECURITY, StepStatus.PROCESSING),
                null);
        verify(kafkaProducerAdapter).publish(
                StepType.SECURITY.getTopic(),
                event.getTransactionId(),
                event);
    }

    @Test
    void waitsUntilEveryParallelStepAndResultIsComplete() {
        CommonModel event = eventFrom(StepType.STUDENTS.name());
        EventStateResponse state = stateWith(Map.of(
                StepType.STUDENTS, StepStatus.COMPLETE,
                StepType.ASSETS, StepStatus.PROCESSING,
                StepType.PREFERENCES, StepStatus.COMPLETE,
                StepType.ROLES, StepStatus.COMPLETE));

        when(eventStateService.transitionStep(
                eq(event),
                eq(TransactionStatus.PROCESSING),
                eq(1),
                eq(StepType.STUDENTS),
                eq(StepStatus.COMPLETE),
                eq(null)))
                .thenReturn(new StepTransitionResult(state, true));

        when(eventStateService.completedStageSnapshot(
                event.getTransactionId(),
                1,
                parallelSteps()))
                .thenReturn(Optional.empty());

        useCase.execute(event);

        verify(eventStateService, never()).claimStageCompletion(
                any(),
                any(),
                any());
        verify(kafkaProducerAdapter, never())
                .publish(any(), any(), any());
    }

    @Test
    void fansOutWhenPreviousStageIsComplete() {
        CommonModel event = eventFrom(StepType.SECURITY.name());
        EventStateResponse state = stateWith(
                Map.of(StepType.SECURITY, StepStatus.COMPLETE));

        when(eventStateService.transitionStep(
                eq(event),
                eq(TransactionStatus.PROCESSING),
                eq(0),
                eq(StepType.SECURITY),
                eq(StepStatus.COMPLETE),
                eq(null)))
                .thenReturn(new StepTransitionResult(state, true));

        when(eventStateService.completedStageSnapshot(
                event.getTransactionId(),
                0,
                List.of(StepType.SECURITY)))
                .thenReturn(Optional.of(
                        new StageResultSnapshot(
                                null,
                                Map.of(
                                        StepType.SECURITY,
                                        JsonNodeFactory.instance.nullNode()))));

        when(eventStateService.claimStageCompletion(
                event.getTransactionId(),
                0,
                List.of(StepType.SECURITY)))
                .thenReturn(true);

        useCase.execute(event);

        Map<StepType, StepStatus> expectedSteps = new EnumMap<>(StepType.class);
        expectedSteps.put(StepType.STUDENTS, StepStatus.PROCESSING);
        expectedSteps.put(StepType.ASSETS, StepStatus.PROCESSING);
        expectedSteps.put(StepType.PREFERENCES, StepStatus.PROCESSING);
        expectedSteps.put(StepType.ROLES, StepStatus.PROCESSING);

        verify(eventStateService).save(
                event,
                TransactionStatus.PROCESSING,
                1,
                expectedSteps,
                null);
        verify(kafkaProducerAdapter, times(4))
                .publish(any(), eq(event.getTransactionId()), eq(event));
    }

    @Test
    void aggregatesEveryParallelResultBeforePublishingModules() {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode input = mapper.createObjectNode()
                .put("userId", 42)
                .put("security", "ok");

        ObjectNode students = input.deepCopy();
        students.put("students", 1);

        ObjectNode assets = input.deepCopy();
        assets.put("assets", 1);

        ObjectNode preferences = input.deepCopy();
        preferences.put("preferences", 1);

        ObjectNode roles = input.deepCopy();
        roles.put("roles", 1);

        CommonModel event = eventFrom(StepType.ROLES.name(), roles);

        when(eventStateService.transitionStep(
                event,
                TransactionStatus.PROCESSING,
                1,
                StepType.ROLES,
                StepStatus.COMPLETE,
                null))
                .thenReturn(new StepTransitionResult(
                        stateWith(Map.of(
                                StepType.STUDENTS, StepStatus.COMPLETE,
                                StepType.ASSETS, StepStatus.COMPLETE,
                                StepType.PREFERENCES, StepStatus.COMPLETE,
                                StepType.ROLES, StepStatus.COMPLETE)),
                        true));

        when(eventStateService.completedStageSnapshot(
                event.getTransactionId(),
                1,
                parallelSteps()))
                .thenReturn(Optional.of(
                        new StageResultSnapshot(
                                input,
                                Map.of(
                                        StepType.STUDENTS, students,
                                        StepType.ASSETS, assets,
                                        StepType.PREFERENCES, preferences,
                                        StepType.ROLES, roles))));

        when(eventStateService.claimStageCompletion(
                event.getTransactionId(),
                1,
                parallelSteps()))
                .thenReturn(true);

        useCase.execute(event);

        ArgumentCaptor<Object> publishedCaptor =
                ArgumentCaptor.forClass(Object.class);

        verify(kafkaProducerAdapter).publish(
                eq(StepType.MODULES.getTopic()),
                eq(event.getTransactionId()),
                publishedCaptor.capture());

        CommonModel published = (CommonModel) publishedCaptor.getValue();

        assertTrue(published.getPayload().has("userId"));
        assertTrue(published.getPayload().has("security"));
        assertTrue(published.getPayload().has("students"));
        assertTrue(published.getPayload().has("assets"));
        assertTrue(published.getPayload().has("preferences"));
        assertTrue(published.getPayload().has("roles"));

        verify(eventStateService).save(
                eq(published),
                eq(TransactionStatus.PROCESSING),
                eq(2),
                eq(Map.of(StepType.MODULES, StepStatus.PROCESSING)),
                eq(null));
    }

    @Test
    void parallelAggregationRejectsConflictingBranchWrites() {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode input = mapper.createObjectNode()
                .put("shared", "base");

        ObjectNode students = input.deepCopy();
        students.put("shared", "students");

        ObjectNode assets = input.deepCopy();
        assets.put("shared", "assets");

        ObjectNode preferences = input.deepCopy();
        ObjectNode roles = input.deepCopy();

        CommonModel event = eventFrom(StepType.ROLES.name(), roles);

        when(eventStateService.transitionStep(
                event,
                TransactionStatus.PROCESSING,
                1,
                StepType.ROLES,
                StepStatus.COMPLETE,
                null))
                .thenReturn(new StepTransitionResult(
                        new EventStateResponse(),
                        true));

        when(eventStateService.completedStageSnapshot(
                event.getTransactionId(),
                1,
                parallelSteps()))
                .thenReturn(Optional.of(
                        new StageResultSnapshot(
                                input,
                                Map.of(
                                        StepType.STUDENTS, students,
                                        StepType.ASSETS, assets,
                                        StepType.PREFERENCES, preferences,
                                        StepType.ROLES, roles))));

        assertThrows(
                IllegalStateException.class,
                () -> useCase.execute(event));

        verify(eventStateService, never()).claimStageCompletion(
                any(), any(), any());
        verify(kafkaProducerAdapter, never()).publish(
                eq(StepType.MODULES.getTopic()),
                any(),
                any());
    }

    @Test
    void completesWorkflowWithAggregatedLastStageResult() {
        ObjectMapper mapper = new ObjectMapper();

        ObjectNode input = mapper.createObjectNode().put("roles", 1);
        ObjectNode modules = input.deepCopy();
        modules.put("modules", 1);

        CommonModel event = eventFrom(StepType.MODULES.name(), modules);
        CommonModelResponse response = new CommonModelResponse(
                event.getTransactionId(),
                event.getProducer(),
                event.getCommand(),
                event.getPayload());
        EventStateResponse state = stateWith(
                Map.of(StepType.MODULES, StepStatus.COMPLETE));

        when(eventStateService.transitionStep(
                eq(event),
                eq(TransactionStatus.PROCESSING),
                eq(2),
                eq(StepType.MODULES),
                eq(StepStatus.COMPLETE),
                eq(null)))
                .thenReturn(new StepTransitionResult(state, true));

        when(eventStateService.completedStageSnapshot(
                event.getTransactionId(),
                2,
                List.of(StepType.MODULES)))
                .thenReturn(Optional.of(
                        new StageResultSnapshot(
                                input,
                                Map.of(StepType.MODULES, modules))));

        when(eventStateService.claimStageCompletion(
                event.getTransactionId(),
                2,
                List.of(StepType.MODULES)))
                .thenReturn(true);

        when(commonModelMapper.toResponse(event)).thenReturn(response);

        useCase.execute(event);

        verify(eventStateService).save(
                event,
                TransactionStatus.COMPLETED,
                2,
                Map.of(),
                event.getPayload());
        verify(kafkaProducerAdapter).publish(
                KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE,
                event.getTransactionId(),
                response);
    }

    @Test
    void failsWorkflowWhenServiceReturnsControlledError() {
        JsonNode payload = new ObjectMapper().createObjectNode()
                .put("error_code", "INVALID_CREDENTIALS")
                .put("error_message", "Invalid username or password");
        CommonModel event = eventFrom(StepType.SECURITY.name(), payload);
        CommonModelResponse response = new CommonModelResponse(
                event.getTransactionId(),
                event.getProducer(),
                event.getCommand(),
                event.getPayload());

        when(eventStateService.transitionStep(
                event,
                TransactionStatus.FAILED,
                0,
                StepType.SECURITY,
                StepStatus.ERROR,
                payload))
                .thenReturn(new StepTransitionResult(
                        new EventStateResponse(),
                        true));
        when(commonModelMapper.toResponse(event)).thenReturn(response);

        useCase.execute(event);

        verify(eventStateService).transitionStep(
                event,
                TransactionStatus.FAILED,
                0,
                StepType.SECURITY,
                StepStatus.ERROR,
                payload);
        verify(kafkaProducerAdapter).publish(
                KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE,
                event.getTransactionId(),
                response);
        verify(kafkaProducerAdapter, never()).publish(
                eq(StepType.STUDENTS.getTopic()),
                any(),
                any());
    }

    @Test
    void failsWorkflowWhenServiceReturnsDeadLetter() {
        JsonNode payload = new ObjectMapper().createObjectNode()
                .put("exception", "java.lang.NullPointerException");
        CommonModel event = eventFrom(StepType.ASSETS.name(), payload);
        CommonModelResponse response = new CommonModelResponse(
                event.getTransactionId(),
                event.getProducer(),
                event.getCommand(),
                event.getPayload());

        when(eventStateService.transitionStep(
                event,
                TransactionStatus.FAILED,
                1,
                StepType.ASSETS,
                StepStatus.ERROR,
                payload))
                .thenReturn(new StepTransitionResult(
                        new EventStateResponse(),
                        true));
        when(commonModelMapper.toResponse(event)).thenReturn(response);

        useCase.execute(event);

        verify(eventStateService).transitionStep(
                event,
                TransactionStatus.FAILED,
                1,
                StepType.ASSETS,
                StepStatus.ERROR,
                payload);
        verify(kafkaProducerAdapter).publish(
                KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE,
                event.getTransactionId(),
                response);
        verify(kafkaProducerAdapter, never()).publish(
                eq(StepType.MODULES.getTopic()),
                any(),
                any());
    }

    @Test
    void ignoresDuplicateCompletedStepWithoutPublishingAgain() {
        CommonModel event = eventFrom(StepType.SECURITY.name());
        EventStateResponse state = stateWith(
                Map.of(StepType.SECURITY, StepStatus.COMPLETE));

        when(eventStateService.transitionStep(
                event,
                TransactionStatus.PROCESSING,
                0,
                StepType.SECURITY,
                StepStatus.COMPLETE,
                null))
                .thenReturn(new StepTransitionResult(state, false));

        useCase.execute(event);

        verify(eventStateService, never()).completedStageSnapshot(
                any(), any(), any());
        verify(eventStateService, never()).claimStageCompletion(
                any(), any(), any());
        verify(kafkaProducerAdapter, never())
                .publish(any(), any(), any());
    }

    private List<StepType> parallelSteps() {
        return List.of(
                StepType.STUDENTS,
                StepType.ASSETS,
                StepType.PREFERENCES,
                StepType.ROLES);
    }

    private CommonModel eventFrom(String producer) {
        return eventFrom(producer, null);
    }

    private CommonModel eventFrom(
            String producer,
            JsonNode payload) {

        return new CommonModel(
                "transaction-id",
                producer,
                OperationType.LOGIN,
                payload);
    }

    private EventStateResponse stateWith(
            Map<StepType, StepStatus> steps) {

        EventStateResponse response = new EventStateResponse();
        response.setSteps(steps);
        return response;
    }
}
