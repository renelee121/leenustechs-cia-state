package mx.com.leenustechs.ciaState.business.repositories;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.google.firebase.database.*;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.OutboxEvent;
import mx.com.leenustechs.ciaState.models.types.*;
import tools.jackson.databind.json.JsonMapper;

class FirebaseEventStateRepositoryTest {
    final JsonMapper mapper = JsonMapper.builder().findAndAddModules().build();
    FirebaseDatabase database;
    DatabaseReference root;
    DatabaseReference child;
    FirebaseEventStateRepository repository;
    @BeforeEach void setup() {
        database = mock(FirebaseDatabase.class);
        root = mock(DatabaseReference.class);
        child = mock(DatabaseReference.class);
        when(database.getReference("workflows")).thenReturn(root);
        when(root.child("tx")).thenReturn(child);
        repository = new FirebaseEventStateRepository(database, mapper, "workflows", 1);
    }
    EventStateEntity entity() {
        EventStateEntity e = new EventStateEntity();
        e.setTransactionId("tx"); e.setProducer("CIA_API"); e.setCommand(OperationType.LOGIN);
        e.setExecutionId("execution"); e.setStatus(TransactionStatus.PROCESSING); e.setCurrentStage(1);
        e.setPayload(mapper.readTree("{\"a\":[1,2]}")); e.setInitialPayload(mapper.readTree("{\"initial\":true}"));
        e.setResult(mapper.readTree("{\"r\":1}")); e.setCreatedAt(Instant.ofEpochMilli(1000)); e.setUpdatedAt(Instant.ofEpochMilli(2000));
        e.setSteps(Map.of(StepType.SECURITY, StepStatus.COMPLETE));
        e.setCompletedStages(Map.of(0, StepStatus.COMPLETE));
        e.setStepResults(Map.of("0:SECURITY", "null", "1:ASSETS", "{\"native\":true}"));
        CommonModel outgoing = new CommonModel("tx", "CIA_STATE", OperationType.LOGIN, mapper.readTree("{}"));
        outgoing.setEventId("event1"); outgoing.setExecutionId("execution"); outgoing.setStageIndex(1); outgoing.setOutcome(EventOutcome.SUCCESS);
        e.setPendingOutbox(Map.of("event1", new OutboxEvent("event1", "topic", "tx", outgoing)));
        e.setOutboxPending(true);
        return e;
    }
    DataSnapshot snapshot(String key, Object value) {
        DataSnapshot s = mock(DataSnapshot.class);
        when(s.getKey()).thenReturn(key); when(s.exists()).thenReturn(value != null); when(s.getValue()).thenReturn(value);
        return s;
    }
    @Test void roundTripsNativeJsonOutboxNullResultsAndPrefixedStageKeys() {
        EventStateEntity expected = entity();
        Map<String, Object> encoded = repository.toNativeValue(expected);
        assertEquals(Map.of("stage_0", "COMPLETE"), encoded.get("completedStages"));
        assertEquals(true, encoded.get("outboxPending"));
        assertEquals(expected, repository.fromNativeValue(encoded).orElseThrow());
        expected.setPendingOutbox(Map.of()); expected.setOutboxPending(false);
        assertEquals(false, repository.toNativeValue(expected).get("outboxPending"));
        assertTrue(repository.fromNativeValue(null).isEmpty());
    }
    @Test void preservesEmptyContainersNullsAndNumericObjectKeysThroughFirebaseNormalization() {
        EventStateEntity expected = entity();
        expected.setInitialPayload(mapper.readTree("{}"));
        expected.setPayload(mapper.readTree("{\"empty\":{},\"array\":[],\"null\":null,\"numeric\":{\"0\":\"zero\",\"1\":\"one\"}}"));
        expected.setResult(mapper.readTree("null"));
        expected.setStepResults(Map.of("empty", "{}", "array", "[]", "null", "null"));
        var pending = expected.getPendingOutbox().get("event1");
        pending.event().setPayload(mapper.readTree("{\"empty\":{},\"null\":null}"));
        Object firebaseValue = com.google.firebase.database.snapshot.NodeUtilities.NodeFromJSON(repository.toNativeValue(expected)).getValue();
        assertEquals(expected, repository.fromNativeValue(firebaseValue).orElseThrow());
        expected.setPayload(mapper.readTree("{\"not.a.firebase/key\":{}}"));
        var nativeValue = repository.toNativeValue(expected);
        assertFalse(nativeValue.containsKey("payload"));
        assertEquals(expected.getPayload(), repository.fromNativeValue(nativeValue).orElseThrow().getPayload());
    }

    @Test void readsLegacyNumericStagesDirectResultsAndTimestamps() {
        var raw = new LinkedHashMap<>(repository.toNativeValue(entity()));
        raw.remove("pendingOutbox"); raw.remove("executionId"); raw.remove("initialPayload"); raw.remove("initialPayloadJson");
        raw.put("completedStages", List.of("COMPLETE", "ERROR"));
        raw.put("stepResults", Map.of("old", Map.of("nested", 1)));
        raw.put("updatedAt", "2020-01-01T00:00:00Z");
        var decoded = repository.fromNativeValue(raw).orElseThrow();
        assertEquals(Map.of(0, StepStatus.COMPLETE, 1, StepStatus.ERROR), decoded.getCompletedStages());
        assertEquals("{\"nested\":1}", decoded.getStepResults().get("old"));
        assertEquals(Instant.parse("2020-01-01T00:00:00Z"), decoded.getUpdatedAt());
        assertFalse(decoded.isOutboxPending());
        raw.put("completedStages", Map.of("0", "COMPLETE"));
        assertEquals(Map.of(0, StepStatus.COMPLETE), repository.fromNativeValue(raw).orElseThrow().getCompletedStages());
        var sparse = new ArrayList<Object>(); sparse.add(null); sparse.add("COMPLETE"); raw.put("completedStages", sparse);
        assertEquals(Map.of(1, StepStatus.COMPLETE), repository.fromNativeValue(raw).orElseThrow().getCompletedStages());
    }
    @Test void rejectsCorruptNativeShapesAndOutboxIdentity() {
        assertThrows(IllegalStateException.class, () -> repository.fromNativeValue(List.of()));
        var raw = new LinkedHashMap<>(repository.toNativeValue(entity()));
        raw.put("pendingOutbox", List.of());
        assertThrows(IllegalStateException.class, () -> repository.fromNativeValue(raw));
        raw.put("pendingOutbox", Map.of("event1", "bad"));
        assertThrows(IllegalStateException.class, () -> repository.fromNativeValue(raw));
        raw.put("pendingOutbox", Map.of("event1", Map.of("id", "other", "event", Map.of())));
        assertThrows(IllegalStateException.class, () -> repository.fromNativeValue(raw));
        raw.remove("pendingOutbox"); raw.put("completedStages", 2);
        assertThrows(IllegalStateException.class, () -> repository.fromNativeValue(raw));
        EventStateEntity bad = entity(); bad.setStepResults(Map.of("bad", "invalid json"));
        assertThrows(IllegalStateException.class, () -> repository.toNativeValue(bad));
    }
    @Test void readsSnapshotAndPropagatesCancellationAndConversionErrors() {
        doAnswer(a -> { a.getArgument(0, ValueEventListener.class).onDataChange(snapshot("tx", repository.toNativeValue(entity()))); return null; })
                .when(child).addListenerForSingleValueEvent(any());
        assertEquals("execution", repository.findById("tx").orElseThrow().getExecutionId());
        doAnswer(a -> { a.getArgument(0, ValueEventListener.class).onDataChange(snapshot("tx", null)); return null; })
                .when(child).addListenerForSingleValueEvent(any());
        assertTrue(repository.findById("tx").isEmpty());
        doAnswer(a -> { a.getArgument(0, ValueEventListener.class).onCancelled(DatabaseError.fromCode(DatabaseError.PERMISSION_DENIED)); return null; })
                .when(child).addListenerForSingleValueEvent(any());
        assertThrows(IllegalStateException.class, () -> repository.findById("tx"));
        doAnswer(a -> { a.getArgument(0, ValueEventListener.class).onDataChange(snapshot("tx", "invalid")); return null; })
                .when(child).addListenerForSingleValueEvent(any());
        assertThrows(IllegalStateException.class, () -> repository.findById("tx"));
    }
    @Test void queryPagesAreBoundedAndExcludeInclusiveCursor() {
        Query q = mock(Query.class, RETURNS_SELF); when(root.orderByChild("outboxPending")).thenReturn(q);
        DataSnapshot page = mock(DataSnapshot.class);
        DataSnapshot row = snapshot("tx", repository.toNativeValue(entity()));
        when(page.getChildren()).thenReturn(List.of(row));
        doAnswer(a -> { a.getArgument(0, ValueEventListener.class).onDataChange(page); return null; }).when(q).addListenerForSingleValueEvent(any());
        assertEquals(1, repository.findWithPendingOutbox(10).size());
        verify(q).equalTo(true); verify(q).limitToFirst(10);
        assertTrue(repository.findWithPendingOutbox(10, "tx").isEmpty());
        verify(q).startAt(true, "tx"); verify(q).endAt(true); verify(q).limitToFirst(11);
        assertThrows(IllegalArgumentException.class, () -> repository.findWithPendingOutbox(0));
        doAnswer(a -> { a.getArgument(0, ValueEventListener.class).onCancelled(DatabaseError.fromCode(DatabaseError.PERMISSION_DENIED)); return null; }).when(q).addListenerForSingleValueEvent(any());
        assertThrows(IllegalStateException.class, () -> repository.findWithPendingOutbox(1));
    }
    @Test void commitsMutationAndPreservesNoopAppliedFlag() {
        AtomicReference<Object> current = new AtomicReference<>(repository.toNativeValue(entity()));
        simulateTransaction(current, false);
        var update = repository.updateAtomically("tx", c -> { c.orElseThrow().setCurrentStage(2); return c; });
        assertTrue(update.applied()); assertEquals(2, update.state().orElseThrow().getCurrentStage());
        assertFalse(repository.updateAtomically("tx", c -> Optional.empty()).applied());
        assertThrows(IllegalArgumentException.class, () -> repository.updateAtomically("tx", c -> { throw new IllegalArgumentException("domain error"); }));
    }
    @Test void retriesStaleNonemptyLocalCacheBeforeReturningDomainErrorOrDuplicate() {
        EventStateEntity stale = entity(); stale.setCurrentStage(0);
        EventStateEntity actual = entity(); actual.setCurrentStage(1);
        doAnswer(a -> {
            Transaction.Handler handler = a.getArgument(0);
            MutableData local = mock(MutableData.class);
            when(local.getValue()).thenReturn(repository.toNativeValue(stale));
            assertTrue(handler.doTransaction(local).isSuccess());
            MutableData server = mock(MutableData.class);
            when(server.getValue()).thenReturn(repository.toNativeValue(actual));
            assertTrue(handler.doTransaction(server).isSuccess());
            handler.onComplete(null, true, snapshot("tx", repository.toNativeValue(actual)));
            return null;
        }).when(child).runTransaction(any(Transaction.Handler.class), eq(false));
        var result = repository.updateAtomically("tx", c -> {
            if (c.orElseThrow().getCurrentStage() == 0) throw new IllegalArgumentException("stale stage");
            return Optional.empty();
        });
        assertFalse(result.applied()); assertEquals(1, result.state().orElseThrow().getCurrentStage());
    }
    @Test void handlesMissingStateAndDatabaseTransactionFailure() {
        simulateTransaction(new AtomicReference<>(), false);
        assertTrue(repository.updateAtomically("tx", c -> c).state().isEmpty());
        doAnswer(a -> { a.getArgument(0, Transaction.Handler.class).onComplete(DatabaseError.fromCode(DatabaseError.DISCONNECTED), false, null); return null; })
                .when(child).runTransaction(any(Transaction.Handler.class), eq(false));
        assertThrows(IllegalStateException.class, () -> repository.updateAtomically("tx", Function.identity()));
    }
    @Test void timeoutAndInterruptAreReportedWithoutClearingInterrupt() {
        var immediate = new FirebaseEventStateRepository(database, mapper, "workflows", 0);
        assertThrows(IllegalStateException.class, () -> immediate.findById("tx"));
        try {
            Thread.currentThread().interrupt();
            assertThrows(IllegalStateException.class, () -> repository.findById("tx"));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally { Thread.interrupted(); }
    }
    private void simulateTransaction(AtomicReference<Object> current, boolean ignored) {
        doAnswer(a -> {
            Transaction.Handler handler = a.getArgument(0);
            MutableData data = mock(MutableData.class);
            when(data.getValue()).thenAnswer(x -> current.get());
            doAnswer(x -> { current.set(x.getArgument(0)); return null; }).when(data).setValue(any());
            assertTrue(handler.doTransaction(data).isSuccess());
            handler.onComplete(null, true, snapshot("tx", current.get()));
            return null;
        }).when(child).runTransaction(any(Transaction.Handler.class), eq(false));
    }
}
