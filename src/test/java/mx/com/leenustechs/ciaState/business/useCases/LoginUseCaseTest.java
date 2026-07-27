package mx.com.leenustechs.ciaState.business.useCases;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.EnumMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import mx.com.leenustechs.ciaState.business.adapters.out.KafkaProducerAdapter;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import mx.com.leenustechs.ciaState.business.utils.mappers.CommonModelMapper;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.constants.KafkaTopics;
import mx.com.leenustechs.ciaState.models.responses.CommonModelResponse;
import mx.com.leenustechs.ciaState.models.responses.EventStateResponse;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

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
        CommonModel event = eventFrom("API");

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
    void waitsUntilEveryParallelStepIsComplete() {
        CommonModel event = eventFrom(StepType.STUDENTS.name());
        EventStateResponse state = stateWith(Map.of(
                StepType.STUDENTS, StepStatus.COMPLETE,
                StepType.ASSETS, StepStatus.PROCESSING,
                StepType.PREFERENCES, StepStatus.COMPLETE,
                StepType.ROLES, StepStatus.COMPLETE));

        when(eventStateService.save(
                eq(event),
                eq(TransactionStatus.PROCESSING),
                eq(1),
                eq(Map.of(StepType.STUDENTS, StepStatus.COMPLETE)),
                eq(null)))
                .thenReturn(state);

        useCase.execute(event);

        verify(kafkaProducerAdapter, never())
                .publish(any(), any(), any());
    }

    @Test
    void fansOutWhenPreviousStageIsComplete() {
        CommonModel event = eventFrom(StepType.SECURITY.name());
        EventStateResponse state = stateWith(
                Map.of(StepType.SECURITY, StepStatus.COMPLETE));

        when(eventStateService.save(
                eq(event),
                eq(TransactionStatus.PROCESSING),
                eq(0),
                eq(Map.of(StepType.SECURITY, StepStatus.COMPLETE)),
                eq(null)))
                .thenReturn(state);

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
    void completesWorkflowWithoutHardcodingLastStep() {
        CommonModel event = eventFrom(StepType.MODULES.name());
        CommonModelResponse response = new CommonModelResponse(
                event.getTransactionId(),
                event.getProducer(),
                event.getCommand(),
                event.getPayload());
        EventStateResponse state = stateWith(
                Map.of(StepType.MODULES, StepStatus.COMPLETE));

        when(eventStateService.save(
                eq(event),
                eq(TransactionStatus.PROCESSING),
                eq(2),
                eq(Map.of(StepType.MODULES, StepStatus.COMPLETE)),
                eq(null)))
                .thenReturn(state);
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

        when(commonModelMapper.toResponse(event)).thenReturn(response);

        useCase.execute(event);

        verify(eventStateService).save(
                event,
                TransactionStatus.FAILED,
                0,
                Map.of(StepType.SECURITY, StepStatus.ERROR),
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

        when(commonModelMapper.toResponse(event)).thenReturn(response);

        useCase.execute(event);

        verify(eventStateService).save(
                event,
                TransactionStatus.FAILED,
                1,
                Map.of(StepType.ASSETS, StepStatus.ERROR),
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
