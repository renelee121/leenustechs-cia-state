package mx.com.leenustechs.ciaState.business.services.impl;

import static org.junit.jupiter.api.Assertions.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Callable;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import mx.com.leenustechs.ciaState.business.utils.exceptions.TransactionNotFoundException;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.*;
import mx.com.leenustechs.ciaState.support.MemoryEventStateRepository;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class EventStateServiceImplTest {
    final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    MemoryEventStateRepository repository;
    EventStateServiceImpl service;
    final List<WorkflowStage> workflow = List.of(WorkflowStage.single(StepType.SECURITY),
            WorkflowStage.parallel(StepType.ASSETS, StepType.ROLES), WorkflowStage.single(StepType.MODULES));
    JsonNode json(String s) { return mapper.readTree(s); }
    CommonModel event(String producer, String payload) {
        return new CommonModel("tx", producer, OperationType.LOGIN, payload == null ? null : json(payload));
    }
    EventStateEntity state() { return repository.findById("tx").orElseThrow(); }
    @BeforeEach void setup() {
        repository = new MemoryEventStateRepository();
        service = new EventStateServiceImpl(repository, mapper);
    }
    void start() { service.process(event("CIA_API", "{\"base\":1}"), workflow); }
    void first() { start(); service.process(event("SECURITY", "{\"auth\":true}"), workflow); }

    @Test void initialReplayNeverRestartsAndDifferentInputOrCommandIsRejected() {
        start();
        EventStateEntity original = state();
        assertEquals(TransactionStatus.PROCESSING, original.getStatus());
        assertEquals(1, original.getPendingOutbox().size());
        var envelope = original.getPendingOutbox().values().iterator().next();
        assertEquals("tx", envelope.key());
        assertEquals("CIA_STATE", envelope.event().getProducer());
        assertNotNull(envelope.event().getEventId());
        assertEquals("tx", envelope.event().getExecutionId());
        assertEquals(0, envelope.event().getStageIndex());
        service.process(event("CIA_API", "{\"base\":1}"), workflow);
        assertEquals(original, state());
        assertThrows(IllegalArgumentException.class, () -> service.process(event("CIA_API", "{}"), workflow));
        CommonModel conflicting = event("CIA_API", "{\"base\":1}"); conflicting.setCommand(OperationType.LOGOUT);
        assertThrows(IllegalArgumentException.class, () -> service.process(conflicting, workflow));
        service.process(event("SECURITY", "{}"), workflow);
        service.process(event("CIA_API", "{\"base\":1}"), workflow);
        assertEquals(1, state().getCurrentStage());
    }

    @Test void parallelResultsAdvanceOnceAndFinalStateIsImmutable() throws Exception {
        first();
        EventStateEntity before = state();
        assertEquals(1, before.getCurrentStage());
        assertEquals(Map.of(StepType.ASSETS, StepStatus.PROCESSING, StepType.ROLES, StepStatus.PROCESSING), before.getSteps());
        try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
            List<Callable<Void>> tasks = new ArrayList<>();
            for (int i = 0; i < 20; i++) {
                String producer = i % 2 == 0 ? "ASSETS" : "ROLES";
                String payload = i % 2 == 0 ? "{\"assets\":[1]}" : "{\"roles\":[2]}";
                tasks.add(() -> { service.process(event(producer, payload), workflow); return null; });
            }
            for (var result : executor.invokeAll(tasks)) result.get();
        }
        assertEquals(2, state().getCurrentStage());
        assertEquals(1, state().getPendingOutbox().size());
        assertEquals(json("{\"base\":1,\"auth\":true,\"assets\":[1],\"roles\":[2]}"), state().getPayload());
        service = new EventStateServiceImpl(repository, mapper); // restart uses durable state
        var result = service.process(event("MODULES", "{\"modules\":[3]}"), workflow);
        assertEquals(TransactionStatus.COMPLETED, result.getStatus());
        assertTrue(result.getResult().has("modules"));
        EventStateEntity complete = state();
        assertEquals(1, complete.getPendingOutbox().size());
        service.process(event("MODULES", "{\"modules\":[999]}"), workflow);
        service.process(event("CIA_API", "{\"base\":1}"), workflow);
        assertEquals(complete, state());
        assertEquals(complete.getStatus(), service.findByTransactionId("tx").getStatus());
    }

    @Test void conflictingResultsFailAtomicallyAndCreateDurableFinalEvent() {
        first();
        service.process(event("ROLES", "{\"shared\":1}"), workflow);
        var result = service.process(event("ASSETS", "{\"shared\":2}"), workflow);
        assertEquals(TransactionStatus.FAILED, result.getStatus());
        assertEquals("INVALID_STAGE_RESULTS", result.getResult().get("error_code").asText());
        assertFalse(result.getResult().toString().contains("shared"));
        assertEquals(1, state().getPendingOutbox().size());
        assertTrue(state().getPendingOutbox().values().stream().anyMatch(o -> o.event().getOutcome() == EventOutcome.FAILURE));
        EventStateEntity failed = state();
        service.process(event("ROLES", "{}"), workflow);
        assertEquals(failed, state());
    }

    @Test void explicitOutcomeOverridesLegacyHeuristicsAndFailureIsTerminal() {
        start();
        CommonModel success = event("SECURITY", "{\"exception\":\"business field\"}");
        success.setOutcome(EventOutcome.SUCCESS);
        service.process(success, workflow);
        assertEquals(1, state().getCurrentStage());
        CommonModel failure = event("ASSETS", "{\"message\":\"denied\"}");
        failure.setOutcome(EventOutcome.FAILURE);
        service.process(failure, workflow);
        assertEquals(TransactionStatus.FAILED, state().getStatus());
        assertEquals(StepStatus.ERROR, state().getSteps().get(StepType.ASSETS));
        assertEquals(failure.getPayload(), state().getResult());
    }

    @Test void acceptedParallelResultRemovesOnlyItsCommandAndKeepsSiblingPending() {
        first();
        var before = state().getPendingOutbox();
        assertEquals(2, before.size());
        String assetsId = before.values().stream().filter(o -> o.topic().equals(StepType.ASSETS.getTopic()))
                .findFirst().orElseThrow().id();
        String rolesId = before.values().stream().filter(o -> o.topic().equals(StepType.ROLES.getTopic()))
                .findFirst().orElseThrow().id();
        service.process(event("ASSETS", "{\"assets\":[1]}"), workflow);
        assertEquals(java.util.Set.of(rolesId), state().getPendingOutbox().keySet());
        assertFalse(state().getPendingOutbox().containsKey(assetsId));
        assertTrue(state().isOutboxPending());
        EventStateEntity waiting = state();
        service.process(event("ASSETS", "{\"assets\":[999]}"), workflow);
        assertEquals(waiting, state());
    }

    @Test void failureCancelsSiblingCommandsAndKeepsOnlyOneStableFinalNotification() {
        first();
        assertEquals(2, state().getPendingOutbox().size());
        CommonModel failure = event("ASSETS", "{\"message\":\"denied\"}");
        failure.setOutcome(EventOutcome.FAILURE);
        service.process(failure, workflow);
        EventStateEntity failed = state();
        assertEquals(TransactionStatus.FAILED, failed.getStatus());
        assertEquals(1, failed.getPendingOutbox().size());
        var finalEvent = failed.getPendingOutbox().values().iterator().next();
        assertEquals(mx.com.leenustechs.ciaState.models.constants.KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE, finalEvent.topic());
        assertEquals(EventOutcome.FAILURE, finalEvent.event().getOutcome());
        assertTrue(failed.isOutboxPending());
        service.process(failure, workflow);
        service.process(event("ROLES", "{}"), workflow);
        service.process(event("CIA_API", "{\"base\":1}"), workflow);
        assertEquals(failed, state());
        assertEquals(java.util.Set.of(finalEvent.id()), state().getPendingOutbox().keySet());
    }

    @Test void successfulCompletionCancelsLegacyPendingCommandsAndKeepsOnlyFinalNotification() {
        first();
        var obsolete = state().getPendingOutbox().values().iterator().next();
        service.process(event("ASSETS", "{}"), workflow);
        service.process(event("ROLES", "{}"), workflow);
        // Older versions retained commands after their worker response. Simulate such a
        // restored state to verify terminal cleanup does not depend on new per-step cleanup.
        repository.updateAtomically("tx", current -> {
            var persisted = current.orElseThrow();
            var pending = new java.util.LinkedHashMap<>(persisted.getPendingOutbox());
            pending.put(obsolete.id(), obsolete);
            persisted.setPendingOutbox(pending);
            return java.util.Optional.of(persisted);
        });
        assertEquals(2, state().getPendingOutbox().size());
        service.process(event("MODULES", "{}"), workflow);
        EventStateEntity completed = state();
        assertEquals(TransactionStatus.COMPLETED, completed.getStatus());
        assertEquals(1, completed.getPendingOutbox().size());
        var finalEvent = completed.getPendingOutbox().values().iterator().next();
        assertEquals(mx.com.leenustechs.ciaState.models.constants.KafkaTopics.LEENUSTECHS_CIA_FINAL_RESPONSE, finalEvent.topic());
        assertEquals(EventOutcome.SUCCESS, finalEvent.event().getOutcome());
        service.process(event("MODULES", "{}"), workflow);
        assertEquals(completed, state());
    }

    @Test void workerResultsHandleLegacyStateWithoutAnOutbox() {
        start();
        repository.updateAtomically("tx", current -> {
            var persisted = current.orElseThrow();
            persisted.setPendingOutbox(null);
            persisted.setOutboxPending(false);
            return java.util.Optional.of(persisted);
        });
        service.process(event("SECURITY", "{}"), workflow);
        assertEquals(1, state().getCurrentStage());
        assertEquals(2, state().getPendingOutbox().size());
    }

    @Test void legacyFailuresNullPayloadAndExecutionCorrelation() {
        CommonModel initial = event("CIA_API", "{}"); initial.setExecutionId("execution1");
        service.process(initial, workflow);
        assertThrows(IllegalArgumentException.class, () -> service.process(event("SECURITY", null), workflow));
        CommonModel wrong = event("SECURITY", null); wrong.setExecutionId("wrong");
        assertThrows(IllegalArgumentException.class, () -> service.process(wrong, workflow));
        CommonModel correct = event("SECURITY", null); correct.setExecutionId("execution1"); correct.setStageIndex(0);
        service.process(correct, workflow);
        assertEquals(json("{}"), state().getPayload());
        CommonModel fail = event("ROLES", "{\"error_code\":\"E\",\"error_message\":\"denied\"}");
        fail.setExecutionId("execution1");
        service.process(fail, workflow);
        assertEquals(TransactionStatus.FAILED, state().getStatus());
    }

    @Test void invalidOrUnknownEventsCannotMutateState() {
        assertThrows(TransactionNotFoundException.class, () -> service.process(event("SECURITY", "{}"), workflow));
        assertThrows(TransactionNotFoundException.class, () -> service.findByTransactionId("missing"));
        assertThrows(IllegalArgumentException.class, () -> service.process(null, workflow));
        assertThrows(IllegalArgumentException.class, () -> service.process(event("CIA_API", "{}"), List.of()));
        assertThrows(IllegalArgumentException.class, () -> service.process(event("CIA_API", "{}"), List.of(WorkflowStage.single(StepType.SECURITY), WorkflowStage.single(StepType.SECURITY))));
        CommonModel initialFailure = event("CIA_API", "{}"); initialFailure.setOutcome(EventOutcome.FAILURE);
        assertThrows(IllegalArgumentException.class, () -> service.process(initialFailure, workflow));
        CommonModel initialStage = event("CIA_API", "{}"); initialStage.setStageIndex(1);
        assertThrows(IllegalArgumentException.class, () -> service.process(initialStage, workflow));
        start();
        assertThrows(IllegalArgumentException.class, () -> service.process(event("MODULES", "{}"), workflow));
        assertThrows(IllegalArgumentException.class, () -> service.process(event("STUDENTS", "{}"), workflow));
        CommonModel badStage = event("SECURITY", "{}"); badStage.setStageIndex(2);
        assertThrows(IllegalArgumentException.class, () -> service.process(badStage, workflow));
        assertEquals(0, state().getCurrentStage());
    }

    @Test void deterministicEventIdentitiesDoNotCollideForColonDelimitedExecutionTuples() {
        CommonModel first = new CommonModel("a", "CIA_API", OperationType.LOGIN, json("{}"));
        first.setExecutionId("b:c");
        CommonModel second = new CommonModel("a:b", "CIA_API", OperationType.LOGIN, json("{}"));
        second.setExecutionId("c");
        service.process(first, workflow);
        service.process(second, workflow);
        String id1 = repository.findById("a").orElseThrow().getPendingOutbox().keySet().iterator().next();
        String id2 = repository.findById("a:b").orElseThrow().getPendingOutbox().keySet().iterator().next();
        assertNotEquals(id1, id2);
    }

    @Test void scalarAndNullResultsPersistAsValidSingleStepResults() {
        var one = List.of(WorkflowStage.single(StepType.SECURITY));
        service.process(event("CIA_API", null), one);
        service.process(event("SECURITY", "42"), one);
        assertEquals(json("42"), state().getResult());
        assertEquals(TransactionStatus.COMPLETED, state().getStatus());
    }
}
