package mx.com.leenustechs.ciaState.business.repositories;

import java.time.Instant;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Repository;

import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

/**
 * Firebase Realtime Database implementation of the workflow state repository.
 *
 * <p>Each transaction is stored below one transactionId node. Mutations that
 * participate in workflow coordination use RTDB transactions on that node.</p>
 */
@Repository
public class FirebaseEventStateRepository implements EventStateRepository {

    private static final String STEP_RESULT_PRESENT = "present";
    private static final String STEP_RESULT_PAYLOAD = "payload";

    private final FirebaseDatabase firebaseDatabase;
    private final ObjectMapper objectMapper;
    private final String workflowPath;
    private final long operationTimeoutSeconds;

    public FirebaseEventStateRepository(
            FirebaseDatabase firebaseDatabase,
            ObjectMapper objectMapper,
            @Value("${firebase.rtdb.workflow-path:workflows}") String workflowPath,
            @Value("${firebase.rtdb.operation-timeout-seconds:10}") long operationTimeoutSeconds) {

        this.firebaseDatabase = firebaseDatabase;
        this.objectMapper = objectMapper;
        this.workflowPath = workflowPath;
        this.operationTimeoutSeconds = operationTimeoutSeconds;
    }

    @Override
    public Optional<EventStateEntity> findById(String transactionId) {
        CompletableFuture<Optional<EventStateEntity>> future =
                new CompletableFuture<>();

        reference(transactionId).addListenerForSingleValueEvent(
                new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot snapshot) {
                        try {
                            future.complete(fromSnapshot(snapshot));
                        } catch (RuntimeException exception) {
                            future.completeExceptionally(exception);
                        }
                    }

                    @Override
                    public void onCancelled(DatabaseError error) {
                        future.completeExceptionally(
                                new IllegalStateException(
                                        "Firebase RTDB read cancelled: "
                                                + error.getMessage()));
                    }
                });

        return await(future, "read workflow " + transactionId);
    }

    @Override
    public AtomicStateUpdate updateAtomically(
            String transactionId,
            Function<Optional<EventStateEntity>, Optional<EventStateEntity>> mutation) {

        CompletableFuture<AtomicStateUpdate> future =
                new CompletableFuture<>();
        AtomicBoolean applied = new AtomicBoolean(false);
        AtomicReference<RuntimeException> mutationFailure =
                new AtomicReference<>();

        reference(transactionId).runTransaction(
                new Transaction.Handler() {
                    @Override
                    public Transaction.Result doTransaction(MutableData currentData) {
                        applied.set(false);

                        try {
                            Optional<EventStateEntity> current =
                                    fromNativeValue(currentData.getValue());

                            Optional<EventStateEntity> desired = mutation.apply(current);

                            if (desired.isEmpty()) {
                                /*
                                 * RTDB transaction callbacks may initially be
                                 * invoked with null even when server data exists.
                                 * Returning success with the unchanged null value
                                 * lets the server detect a hash mismatch and retry
                                 * with the actual value. If the workflow truly does
                                 * not exist this commits as a harmless no-op.
                                 */
                                if (current.isEmpty()) {
                                    return Transaction.success(currentData);
                                }

                                return Transaction.abort();
                            }

                            currentData.setValue(toNativeValue(desired.get()));
                            applied.set(true);
                            return Transaction.success(currentData);
                        } catch (RuntimeException exception) {
                            mutationFailure.set(exception);
                            return Transaction.abort();
                        }
                    }

                    @Override
                    public void onComplete(
                            DatabaseError error,
                            boolean committed,
                            DataSnapshot snapshot) {

                        RuntimeException mutationException =
                                mutationFailure.get();

                        if (mutationException != null) {
                            future.completeExceptionally(mutationException);
                            return;
                        }

                        if (error != null) {
                            future.completeExceptionally(
                                    new IllegalStateException(
                                            "Firebase RTDB transaction failed: "
                                                    + error.getMessage()));
                            return;
                        }

                        try {
                            future.complete(
                                    new AtomicStateUpdate(
                                            committed && applied.get(),
                                            fromSnapshot(snapshot)));
                        } catch (RuntimeException exception) {
                            future.completeExceptionally(exception);
                        }
                    }
                },
                false);

        return await(future, "update workflow " + transactionId);
    }

    DatabaseReference reference(String transactionId) {
        return firebaseDatabase
                .getReference(workflowPath)
                .child(transactionId);
    }

    Map<String, Object> toNativeValue(EventStateEntity entity) {
        Map<String, Object> value = new LinkedHashMap<>();

        put(value, "transactionId", entity.getTransactionId());
        put(value, "producer", entity.getProducer());
        put(value, "command", enumName(entity.getCommand()));
        put(value, "status", enumName(entity.getStatus()));
        put(value, "currentStage", entity.getCurrentStage());
        put(value, "steps", encodeSteps(entity.getSteps()));
        put(value, "completedStages", encodeCompletedStages(entity.getCompletedStages()));
        put(value, "stepResults", encodeStepResults(entity.getStepResults()));
        put(value, "payload", jsonToNative(entity.getPayload()));
        put(value, "result", jsonToNative(entity.getResult()));
        put(value, "createdAt", instantToEpochMillis(entity.getCreatedAt()));
        put(value, "updatedAt", instantToEpochMillis(entity.getUpdatedAt()));

        return value;
    }

    Optional<EventStateEntity> fromNativeValue(Object nativeValue) {
        if (nativeValue == null) {
            return Optional.empty();
        }

        if (!(nativeValue instanceof Map<?, ?> map)) {
            throw new IllegalStateException(
                    "Firebase RTDB workflow must be a JSON object");
        }

        EventStateEntity entity = new EventStateEntity();
        entity.setTransactionId(stringValue(map.get("transactionId")));
        entity.setProducer(stringValue(map.get("producer")));
        entity.setCommand(enumValue(OperationType.class, map.get("command")));
        entity.setStatus(enumValue(TransactionStatus.class, map.get("status")));
        entity.setCurrentStage(integerValue(map.get("currentStage")));
        entity.setSteps(decodeSteps(map.get("steps")));
        entity.setCompletedStages(decodeCompletedStages(map.get("completedStages")));
        entity.setStepResults(decodeStepResults(map.get("stepResults")));
        entity.setPayload(nativeToJson(map.get("payload")));
        entity.setResult(nativeToJson(map.get("result")));
        entity.setCreatedAt(instantValue(map.get("createdAt")));
        entity.setUpdatedAt(instantValue(map.get("updatedAt")));

        return Optional.of(entity);
    }

    private Optional<EventStateEntity> fromSnapshot(DataSnapshot snapshot) {
        if (snapshot == null || !snapshot.exists()) {
            return Optional.empty();
        }

        return fromNativeValue(snapshot.getValue());
    }

    private Map<String, String> decodeStepResults(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawResults)) {
            return null;
        }

        Map<String, String> results = new LinkedHashMap<>();

        for (Map.Entry<?, ?> entry : rawResults.entrySet()) {
            String key = String.valueOf(entry.getKey());
            Object rawResult = entry.getValue();

            try {
                if (rawResult instanceof Map<?, ?> wrapper
                        && Boolean.TRUE.equals(wrapper.get(STEP_RESULT_PRESENT))) {

                    if (!wrapper.containsKey(STEP_RESULT_PAYLOAD)) {
                        results.put(key, "null");
                    } else {
                        results.put(
                                key,
                                objectMapper.writeValueAsString(
                                        wrapper.get(STEP_RESULT_PAYLOAD)));
                    }
                } else {
                    // Allows reading direct/native result nodes during migrations.
                    results.put(key, objectMapper.writeValueAsString(rawResult));
                }
            } catch (Exception exception) {
                throw new IllegalStateException(
                        "Unable to decode Firebase RTDB step result " + key,
                        exception);
            }
        }

        return results;
    }

    private Map<String, Object> encodeStepResults(Map<String, String> stepResults) {
        if (stepResults == null) {
            return null;
        }

        Map<String, Object> encoded = new LinkedHashMap<>();

        for (Map.Entry<String, String> entry : stepResults.entrySet()) {
            Map<String, Object> wrapper = new LinkedHashMap<>();
            wrapper.put(STEP_RESULT_PRESENT, true);

            try {
                Object payload = objectMapper.readValue(entry.getValue(), Object.class);

                if (payload != null) {
                    wrapper.put(STEP_RESULT_PAYLOAD, payload);
                }
            } catch (Exception exception) {
                throw new IllegalStateException(
                        "Unable to encode step result " + entry.getKey(),
                        exception);
            }

            encoded.put(entry.getKey(), wrapper);
        }

        return encoded;
    }

    private Map<String, String> encodeSteps(Map<StepType, StepStatus> steps) {
        if (steps == null) {
            return null;
        }

        Map<String, String> encoded = new LinkedHashMap<>();
        steps.forEach((step, status) ->
                encoded.put(step.name(), status.name()));
        return encoded;
    }

    private Map<StepType, StepStatus> decodeSteps(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawSteps)) {
            return null;
        }

        Map<StepType, StepStatus> steps = new EnumMap<>(StepType.class);
        rawSteps.forEach((key, value) ->
                steps.put(
                        StepType.valueOf(String.valueOf(key)),
                        StepStatus.valueOf(String.valueOf(value))));
        return steps;
    }

    private Map<String, String> encodeCompletedStages(
            Map<Integer, StepStatus> completedStages) {

        if (completedStages == null) {
            return null;
        }

        Map<String, String> encoded = new LinkedHashMap<>();
        completedStages.forEach((stage, status) ->
                encoded.put(String.valueOf(stage), status.name()));
        return encoded;
    }

    private Map<Integer, StepStatus> decodeCompletedStages(Object rawValue) {
        if (!(rawValue instanceof Map<?, ?> rawStages)) {
            return null;
        }

        Map<Integer, StepStatus> stages = new LinkedHashMap<>();
        rawStages.forEach((key, value) ->
                stages.put(
                        Integer.valueOf(String.valueOf(key)),
                        StepStatus.valueOf(String.valueOf(value))));
        return stages;
    }

    private JsonNode nativeToJson(Object value) {
        if (value == null) {
            return null;
        }

        return objectMapper.convertValue(value, JsonNode.class);
    }

    private Object jsonToNative(JsonNode value) {
        if (value == null || value.isNull()) {
            return null;
        }

        return objectMapper.convertValue(value, Object.class);
    }

    private Instant instantValue(Object value) {
        if (value == null) {
            return null;
        }

        if (value instanceof Number number) {
            return Instant.ofEpochMilli(number.longValue());
        }

        return Instant.parse(String.valueOf(value));
    }

    private Long instantToEpochMillis(Instant value) {
        return value == null ? null : value.toEpochMilli();
    }

    private Integer integerValue(Object value) {
        return value == null ? null : ((Number) value).intValue();
    }

    private String stringValue(Object value) {
        return value == null ? null : String.valueOf(value);
    }

    private <E extends Enum<E>> E enumValue(Class<E> enumType, Object value) {
        return value == null
                ? null
                : Enum.valueOf(enumType, String.valueOf(value));
    }

    private String enumName(Enum<?> value) {
        return value == null ? null : value.name();
    }

    private void put(Map<String, Object> target, String key, Object value) {
        if (value != null) {
            target.put(key, value);
        }
    }

    private <T> T await(CompletableFuture<T> future, String operation) {
        try {
            return future.get(operationTimeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(
                    "Interrupted while waiting to " + operation,
                    exception);
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            throw new IllegalStateException(
                    "Unable to " + operation,
                    cause);
        } catch (TimeoutException exception) {
            throw new IllegalStateException(
                    "Timed out while waiting to " + operation,
                    exception);
        }
    }
}
