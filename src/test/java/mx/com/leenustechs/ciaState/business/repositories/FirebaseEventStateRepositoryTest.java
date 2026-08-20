package mx.com.leenustechs.ciaState.business.repositories;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.MutableData;
import com.google.firebase.database.Transaction;
import com.google.firebase.database.ValueEventListener;

import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.records.AtomicStateUpdate;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.ObjectMapper;

class FirebaseEventStateRepositoryTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final FirebaseDatabase database = mock(FirebaseDatabase.class);
    private final DatabaseReference workflows = mock(DatabaseReference.class);
    private final DatabaseReference transaction = mock(DatabaseReference.class);
    private FirebaseEventStateRepository repository;

    @BeforeEach
    void setUp() {
        repository = new FirebaseEventStateRepository(
                database,
                objectMapper,
                "workflows",
                1);
        when(database.getReference("workflows")).thenReturn(workflows);
        when(workflows.child("tx")).thenReturn(transaction);
    }

    @Test
    void convertsWorkflowToJsonNativeRtdbShapeAndBack() throws Exception {
        EventStateEntity entity = sampleEntity();

        Map<String, Object> nativeValue = repository.toNativeValue(entity);

        assertEquals("LOGIN", nativeValue.get("command"));
        assertEquals("PROCESSING", nativeValue.get("status"));
        assertEquals(1, nativeValue.get("currentStage"));

        Map<?, ?> nativeSteps = assertInstanceOf(Map.class, nativeValue.get("steps"));
        assertEquals("COMPLETE", nativeSteps.get("STUDENTS"));

        Map<?, ?> nativeResults = assertInstanceOf(Map.class, nativeValue.get("stepResults"));
        Map<?, ?> students = assertInstanceOf(Map.class, nativeResults.get("1:STUDENTS"));
        assertEquals(Boolean.TRUE, students.get("present"));
        assertInstanceOf(Map.class, students.get("payload"));

        Map<?, ?> nullResult = assertInstanceOf(Map.class, nativeResults.get("1:ASSETS"));
        assertEquals(Boolean.TRUE, nullResult.get("present"));
        assertFalse(nullResult.containsKey("payload"));

        EventStateEntity restored = repository
                .fromNativeValue(nativeValue)
                .orElseThrow();

        assertEquals(entity.getTransactionId(), restored.getTransactionId());
        assertEquals(entity.getCommand(), restored.getCommand());
        assertEquals(entity.getStatus(), restored.getStatus());
        assertEquals(entity.getCurrentStage(), restored.getCurrentStage());
        assertEquals(entity.getSteps(), restored.getSteps());
        assertEquals(entity.getCompletedStages(), restored.getCompletedStages());
        assertEquals("null", restored.getStepResults().get("1:ASSETS"));
        assertEquals(
                objectMapper.readTree(entity.getStepResults().get("1:STUDENTS")),
                objectMapper.readTree(restored.getStepResults().get("1:STUDENTS")));
        assertEquals(entity.getPayload(), restored.getPayload());
        assertEquals(entity.getResult(), restored.getResult());
        assertEquals(entity.getCreatedAt(), restored.getCreatedAt());
        assertEquals(entity.getUpdatedAt(), restored.getUpdatedAt());
    }

    @Test
    void readsDirectNativeStepResultsForMigrationCompatibility() throws Exception {
        Map<String, Object> raw = Map.of(
                "transactionId", "tx",
                "stepResults", Map.of(
                        "1:ROLES", Map.of("roles", 2)));

        EventStateEntity restored = repository
                .fromNativeValue(raw)
                .orElseThrow();

        assertNotNull(restored.getStepResults());
        assertEquals(
                objectMapper.readTree("{\"roles\":2}"),
                objectMapper.readTree(restored.getStepResults().get("1:ROLES")));
    }

    @Test
    void nullNativeValueMeansMissingWorkflow() {
        assertTrue(repository.fromNativeValue(null).isEmpty());
    }

    @Test
    void readsWorkflowThroughSingleValueListener() {
        Map<String, Object> nativeValue = repository.toNativeValue(sampleEntity());

        doAnswer(invocation -> {
            ValueEventListener listener = invocation.getArgument(0);
            listener.onDataChange(snapshot(nativeValue));
            return null;
        }).when(transaction).addListenerForSingleValueEvent(any(ValueEventListener.class));

        EventStateEntity restored = repository.findById("tx").orElseThrow();

        assertEquals("tx", restored.getTransactionId());
        assertEquals(TransactionStatus.PROCESSING, restored.getStatus());
    }

    @Test
    void atomicUpdateWritesAndReturnsCommittedWorkflow() {
        Map<String, Object> nativeValue = repository.toNativeValue(sampleEntity());

        doAnswer(invocation -> {
            Transaction.Handler handler = invocation.getArgument(0);
            MutableData mutableData = mutableData(nativeValue);
            AtomicReference<Object> written = new AtomicReference<>();
            doAnswer(setInvocation -> {
                written.set(setInvocation.getArgument(0));
                return null;
            }).when(mutableData).setValue(any());

            handler.doTransaction(mutableData);
            handler.onComplete(
                    null,
                    true,
                    snapshot(written.get()));
            return null;
        }).when(transaction).runTransaction(any(Transaction.Handler.class), eq(false));

        AtomicStateUpdate update = repository.updateAtomically(
                "tx",
                current -> current.map(entity -> {
                    entity.setStatus(TransactionStatus.COMPLETED);
                    return entity;
                }));

        assertTrue(update.applied());
        assertEquals(
                TransactionStatus.COMPLETED,
                update.state().orElseThrow().getStatus());
    }

    @Test
    void atomicUpdateReevaluatesAfterInitialNullSnapshot() {
        Map<String, Object> nativeValue = repository.toNativeValue(sampleEntity());

        doAnswer(invocation -> {
            Transaction.Handler handler = invocation.getArgument(0);

            MutableData emptyLocalCache = mutableData(null);
            handler.doTransaction(emptyLocalCache);

            MutableData serverState = mutableData(nativeValue);
            AtomicReference<Object> written = new AtomicReference<>();
            doAnswer(setInvocation -> {
                written.set(setInvocation.getArgument(0));
                return null;
            }).when(serverState).setValue(any());

            handler.doTransaction(serverState);
            handler.onComplete(null, true, snapshot(written.get()));
            return null;
        }).when(transaction).runTransaction(any(Transaction.Handler.class), eq(false));

        AtomicStateUpdate update = repository.updateAtomically(
                "tx",
                current -> current.flatMap(entity -> {
                    entity.setStatus(TransactionStatus.COMPLETED);
                    return Optional.of(entity);
                }));

        assertTrue(update.applied());
        assertEquals(
                TransactionStatus.COMPLETED,
                update.state().orElseThrow().getStatus());
    }

    @Test
    void abortedMutationReturnsCurrentWorkflowWithoutApplying() {
        Map<String, Object> nativeValue = repository.toNativeValue(sampleEntity());

        doAnswer(invocation -> {
            Transaction.Handler handler = invocation.getArgument(0);
            handler.doTransaction(mutableData(nativeValue));
            handler.onComplete(null, false, snapshot(nativeValue));
            return null;
        }).when(transaction).runTransaction(any(Transaction.Handler.class), eq(false));

        AtomicStateUpdate update = repository.updateAtomically(
                "tx",
                current -> Optional.empty());

        assertFalse(update.applied());
        assertEquals(
                TransactionStatus.PROCESSING,
                update.state().orElseThrow().getStatus());
    }

    private EventStateEntity sampleEntity() {
        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId("tx");
        entity.setProducer("SECURITY");
        entity.setCommand(OperationType.LOGIN);
        entity.setStatus(TransactionStatus.PROCESSING);
        entity.setCurrentStage(1);
        entity.setSteps(Map.of(
                StepType.STUDENTS, StepStatus.COMPLETE,
                StepType.ASSETS, StepStatus.PROCESSING));
        entity.setCompletedStages(Map.of(0, StepStatus.COMPLETE));
        entity.setStepResults(Map.of(
                "1:STUDENTS", "{\"students\":1}",
                "1:ASSETS", "null"));
        entity.setPayload(objectMapper.createObjectNode().put("userId", 42));
        entity.setResult(objectMapper.createObjectNode().put("ok", true));
        entity.setCreatedAt(Instant.parse("2026-08-20T12:00:00Z"));
        entity.setUpdatedAt(Instant.parse("2026-08-20T12:00:01Z"));
        return entity;
    }

    private MutableData mutableData(Object value) {
        MutableData mutableData = mock(MutableData.class);
        when(mutableData.getValue()).thenReturn(value);
        return mutableData;
    }

    private DataSnapshot snapshot(Object value) {
        DataSnapshot snapshot = mock(DataSnapshot.class);
        when(snapshot.exists()).thenReturn(value != null);
        when(snapshot.getValue()).thenReturn(value);
        return snapshot;
    }
}
