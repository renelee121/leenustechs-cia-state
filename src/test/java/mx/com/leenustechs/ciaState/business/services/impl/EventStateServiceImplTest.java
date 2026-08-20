package mx.com.leenustechs.ciaState.business.services.impl;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import mx.com.leenustechs.ciaState.business.repositories.EventStateRepository;
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
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@SuppressWarnings({"unchecked", "rawtypes"})
class EventStateServiceImplTest {

    private final EventStateRepository repository = mock(EventStateRepository.class);
    private final EventStateModelMapper modelMapper = mock(EventStateModelMapper.class);
    private final EventStateInputMapper inputMapper = mock(EventStateInputMapper.class);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JsonNode payload = objectMapper.createObjectNode().put("value", 1);
    private EventStateServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new EventStateServiceImpl(
                repository,
                modelMapper,
                inputMapper,
                objectMapper);
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
    void createsNewDurableStateWithoutTtl() {
        CommonModel event = event(null);
        EventStateModel model = new EventStateModel();
        model.setTransactionId("tx");
        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        EventStateResponse response = new EventStateResponse();

        when(inputMapper.toEventStateModel(
                any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(model);
        when(modelMapper.toEntity(model)).thenReturn(entity);
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);
        mockAtomicUpdate(Optional.empty(), true);

        assertSame(response, service.save(
                event,
                TransactionStatus.PROCESSING,
                0,
                Map.of(StepType.SECURITY, StepStatus.PROCESSING),
                null));

        verify(modelMapper).toEntity(model);
    }

    @Test
    void updatesExistingStateInsideAtomicStoreMutation() {
        CommonModel event = event(payload);
        EventStateModel model = new EventStateModel();
        model.setTransactionId("tx");
        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        EventStateResponse response = new EventStateResponse();

        when(inputMapper.toEventStateModel(
                any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(model);
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);
        mockAtomicUpdate(Optional.of(entity), true);

        assertSame(response, service.save(
                event,
                TransactionStatus.PROCESSING,
                1,
                Map.of(StepType.ASSETS, StepStatus.PROCESSING),
                payload));

        verify(modelMapper).updateFromModel(model, entity);
        verify(modelMapper, never()).toEntity(model);
    }

    @Test
    void appliesCompleteTransitionAndPersistsWorkerResultAtomically() throws Exception {
        EventStateEntity entity = processingEntity(0, StepType.SECURITY);
        EventStateResponse response = mappedResponse(entity);
        mockAtomicUpdate(Optional.of(entity), true);

        StepTransitionResult transition = service.transitionStep(
                event(payload),
                TransactionStatus.PROCESSING,
                0,
                StepType.SECURITY,
                StepStatus.COMPLETE,
                null);

        assertTrue(transition.applied());
        assertSame(response, transition.state());
        assertEquals(StepStatus.COMPLETE, entity.getSteps().get(StepType.SECURITY));
        assertEquals(payload, objectMapper.readTree(entity.getStepResults().get("0:SECURITY")));
    }

    @Test
    void terminalWorkflowRejectsLateWorkerWithoutRegressingState() {
        EventStateEntity entity = processingEntity(0, StepType.SECURITY);
        entity.setStatus(TransactionStatus.FAILED);
        EventStateResponse response = mappedResponse(entity);
        mockAtomicUpdate(Optional.of(entity), false);

        StepTransitionResult transition = service.transitionStep(
                event(payload),
                TransactionStatus.PROCESSING,
                0,
                StepType.SECURITY,
                StepStatus.COMPLETE,
                null);

        assertFalse(transition.applied());
        assertSame(response, transition.state());
        assertEquals(StepStatus.PROCESSING, entity.getSteps().get(StepType.SECURITY));
        assertTrue(entity.getStepResults() == null || entity.getStepResults().isEmpty());
    }

    @Test
    void failureTransitionIsMonotonicAndStoresErrorResult() {
        EventStateEntity entity = processingEntity(1, StepType.ASSETS);
        JsonNode error = objectMapper.createObjectNode().put("code", "ASSET_ERROR");
        mappedResponse(entity);
        mockAtomicUpdate(Optional.of(entity), true);

        StepTransitionResult transition = service.transitionStep(
                event(payload),
                TransactionStatus.FAILED,
                1,
                StepType.ASSETS,
                StepStatus.ERROR,
                error);

        assertTrue(transition.applied());
        assertEquals(TransactionStatus.FAILED, entity.getStatus());
        assertEquals(StepStatus.ERROR, entity.getSteps().get(StepType.ASSETS));
        assertEquals(error, entity.getResult());
    }

    @Test
    void rejectsInconsistentTransitionBeforeTouchingPersistence() {
        assertThrows(
                IllegalArgumentException.class,
                () -> service.transitionStep(
                        event(payload),
                        TransactionStatus.COMPLETED,
                        0,
                        StepType.SECURITY,
                        StepStatus.COMPLETE,
                        null));

        verify(repository, never()).updateAtomically(any(), any());
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
    void claimsCompletedStageExactlyOnceThroughAtomicStoreMutation() {
        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        entity.setStatus(TransactionStatus.PROCESSING);
        entity.setCurrentStage(1);
        entity.setSteps(Map.of(
                StepType.ASSETS, StepStatus.COMPLETE,
                StepType.ROLES, StepStatus.COMPLETE));

        mockAtomicUpdate(Optional.of(entity), true);
        assertTrue(service.claimStageCompletion(
                "tx",
                1,
                List.of(StepType.ASSETS, StepType.ROLES)));
        assertEquals(StepStatus.COMPLETE, entity.getCompletedStages().get(1));

        mockAtomicUpdate(Optional.of(entity), false);
        assertFalse(service.claimStageCompletion(
                "tx",
                1,
                List.of(StepType.ASSETS, StepType.ROLES)));
    }

    @Test
    void refusesEmptyStageClaimWithoutTouchingPersistence() {
        assertFalse(service.claimStageCompletion("tx", 1, List.of()));
        verify(repository, never()).updateAtomically(eq("tx"), any());
    }

    private EventStateEntity processingEntity(int stage, StepType step) {
        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        entity.setStatus(TransactionStatus.PROCESSING);
        entity.setCurrentStage(stage);
        entity.setSteps(Map.of(step, StepStatus.PROCESSING));
        return entity;
    }

    private EventStateResponse mappedResponse(EventStateEntity entity) {
        EventStateModel model = new EventStateModel();
        EventStateResponse response = new EventStateResponse();
        when(modelMapper.toModel(entity)).thenReturn(model);
        when(modelMapper.toResponse(model)).thenReturn(response);
        return response;
    }

    private void mockAtomicUpdate(
            Optional<EventStateEntity> initial,
            boolean expectedApplied) {

        when(repository.updateAtomically(eq("tx"), any()))
                .thenAnswer(invocation -> {
                    Function<Optional<EventStateEntity>, Optional<EventStateEntity>> mutation =
                            invocation.getArgument(1);
                    Optional<EventStateEntity> desired = mutation.apply(initial);
                    boolean applied = desired.isPresent();

                    if (expectedApplied) {
                        assertTrue(applied);
                    } else {
                        assertFalse(applied);
                    }

                    return new AtomicStateUpdate(
                            applied,
                            desired.isPresent() ? desired : initial);
                });
    }

    private CommonModel event(JsonNode eventPayload) {
        return new CommonModel(
                "tx",
                "SECURITY",
                OperationType.LOGIN,
                eventPayload);
    }
}
