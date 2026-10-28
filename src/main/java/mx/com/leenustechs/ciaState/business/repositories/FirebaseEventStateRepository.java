package mx.com.leenustechs.ciaState.business.repositories;

import java.time.Instant;
import java.util.EnumMap;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Objects;
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
import com.google.firebase.database.Query;
import com.google.firebase.database.Transaction;
import com.google.firebase.database.ValueEventListener;

import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.records.OutboxEvent;
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
    public List<EventStateEntity> findWithPendingOutbox(int limit, String afterTransactionId) {
        if (limit < 1) throw new IllegalArgumentException("Outbox limit must be positive");
        CompletableFuture<List<EventStateEntity>> future = new CompletableFuture<>();
        Query pending = firebaseDatabase.getReference(workflowPath).orderByChild("outboxPending");
        Query page = afterTransactionId == null ? pending.equalTo(true).limitToFirst(limit)
                : pending.startAt(true, afterTransactionId).endAt(true).limitToFirst(Math.addExact(limit, 1));
        page.addListenerForSingleValueEvent(new ValueEventListener() {
                    @Override
                    public void onDataChange(DataSnapshot snapshot) {
                        try {
                            List<EventStateEntity> states = new ArrayList<>();
                            for (DataSnapshot child : snapshot.getChildren()) {
                                if (!Objects.equals(afterTransactionId, child.getKey()) && states.size() < limit) {
                                    fromSnapshot(child).ifPresent(states::add);
                                }
                            }
                            future.complete(states);
                        } catch (RuntimeException e) {
                            future.completeExceptionally(e);
                        }
                    }
                    @Override
                    public void onCancelled(DatabaseError error) {
                        future.completeExceptionally(new IllegalStateException("Firebase RTDB outbox read cancelled: " + error.getCode()));
                    }
                });
        return await(future, "read pending outbox");
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
                        mutationFailure.set(null);

                        try {
                            Optional<EventStateEntity> current =
                                    fromNativeValue(currentData.getValue());

                            Optional<EventStateEntity> desired = mutation.apply(current);

                            if (desired.isEmpty()) {
                                // A nonempty local cache can be stale as well. A no-op commit
                                // lets the server reject its hash and retry with current data.
                                return Transaction.success(currentData);
                            }

                            currentData.setValue(toNativeValue(desired.get()));
                            applied.set(true);
                            return Transaction.success(currentData);
                        } catch (RuntimeException exception) {
                            mutationFailure.set(exception);
                            // Validate a server-confirmed snapshot before exposing a domain
                            // error; stale local state may be retried with a different outcome.
                            return Transaction.success(currentData);
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
        put(value, "executionId", entity.getExecutionId());
        putJson(value, "initialPayload", entity.getInitialPayload());
        put(value, "pendingOutbox", encodeOutbox(entity.getPendingOutbox()));
        put(value, "outboxPending", entity.getPendingOutbox() != null && !entity.getPendingOutbox().isEmpty());
        put(value, "producer", entity.getProducer());
        put(value, "command", enumName(entity.getCommand()));
        put(value, "status", enumName(entity.getStatus()));
        put(value, "currentStage", entity.getCurrentStage());
        put(value, "steps", encodeSteps(entity.getSteps()));
        put(value, "completedStages", encodeCompletedStages(entity.getCompletedStages()));
        put(value, "stepResults", encodeStepResults(entity.getStepResults()));
        putJson(value, "payload", entity.getPayload());
        putJson(value, "result", entity.getResult());
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
        entity.setExecutionId(stringValue(map.get("executionId")));
        entity.setInitialPayload(readJson(map, "initialPayload"));
        entity.setPendingOutbox(decodeOutbox(map.get("pendingOutbox")));
        entity.setOutboxPending(!entity.getPendingOutbox().isEmpty());
        entity.setProducer(stringValue(map.get("producer")));
        entity.setCommand(enumValue(OperationType.class, map.get("command")));
        entity.setStatus(enumValue(TransactionStatus.class, map.get("status")));
        entity.setCurrentStage(integerValue(map.get("currentStage")));
        entity.setSteps(decodeSteps(map.get("steps")));
        entity.setCompletedStages(decodeCompletedStages(map.get("completedStages")));
        entity.setStepResults(decodeStepResults(map.get("stepResults")));
        entity.setPayload(readJson(map, "payload"));
        entity.setResult(readJson(map, "result"));
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

    private Map<String, Object> encodeOutbox(Map<String, OutboxEvent> events) {
        Map<String, Object> encoded = new LinkedHashMap<>();
        if (events == null) return encoded;
        for (Map.Entry<String, OutboxEvent> entry : events.entrySet()) {
            OutboxEvent event = entry.getValue();
            Map<String, Object> envelope = new LinkedHashMap<>();
            envelope.put("id", event.id());
            envelope.put("topic", event.topic());
            envelope.put("key", event.key());
            // Keep a native projection for readers, plus an exact envelope: RTDB
            // prunes empty containers and turns dense numeric-key objects into arrays.
            Map<String, Object> nativeEvent = objectMapper.convertValue(event.event(), new tools.jackson.core.type.TypeReference<Map<String, Object>>() {});
            nativeEvent.put("payload", jsonToNative(event.event().getPayload()));
            envelope.put("event", nativeEvent);
            envelope.put("eventJson", objectMapper.writeValueAsString(event.event()));
            encoded.put(entry.getKey(), envelope);
        }
        return encoded;
    }

    private Map<String, OutboxEvent> decodeOutbox(Object rawValue) {
        Map<String, OutboxEvent> events = new LinkedHashMap<>();
        if (rawValue == null) return events;
        if (!(rawValue instanceof Map<?, ?> rawEvents)) {
            throw new IllegalStateException("Invalid workflow outbox");
        }
        for (Map.Entry<?, ?> entry : rawEvents.entrySet()) {
            if (!(entry.getValue() instanceof Map<?, ?> value)) {
                throw new IllegalStateException("Invalid workflow outbox event");
            }
            String id = stringValue(value.get("id"));
            String key = String.valueOf(entry.getKey());
            CommonModel event = value.containsKey("eventJson")
                    ? objectMapper.readValue(String.valueOf(value.get("eventJson")), CommonModel.class)
                    : objectMapper.convertValue(value.get("event"), CommonModel.class);
            if (!key.equals(id) || event == null || !Objects.equals(id, event.getEventId())) {
                throw new IllegalStateException("Invalid workflow outbox identity");
            }
            events.put(key, new OutboxEvent(id, stringValue(value.get("topic")), stringValue(value.get("key")), event));
        }
        return events;
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

                    if (wrapper.containsKey("json")) {
                        String json = String.valueOf(wrapper.get("json"));
                        objectMapper.readTree(json); // Fail explicitly on corrupt canonical data.
                        results.put(key, json);
                    } else if (!wrapper.containsKey(STEP_RESULT_PAYLOAD)) {
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
            wrapper.put("json", entry.getValue());

            try {
                Object payload = objectMapper.readValue(entry.getValue(), Object.class);

                if (payload != null) {
                    Object projection = jsonToNative(objectMapper.readTree(entry.getValue()));
                    if (projection != null) wrapper.put(STEP_RESULT_PAYLOAD, projection);
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
                encoded.put("stage_" + stage, status.name()));
        return encoded;
    }

    private Map<Integer, StepStatus> decodeCompletedStages(Object rawValue) {
        if (rawValue == null) return null;
        Map<Integer, StepStatus> stages = new LinkedHashMap<>();
        if (rawValue instanceof List<?> list) {
            // RTDB materializes dense numeric-key objects from older versions as lists.
            for (int i = 0; i < list.size(); i++) {
                if (list.get(i) != null) stages.put(i, StepStatus.valueOf(String.valueOf(list.get(i))));
            }
        } else if (rawValue instanceof Map<?, ?> rawStages) {
            rawStages.forEach((key, value) -> {
                String stage = String.valueOf(key);
                if (stage.startsWith("stage_")) stage = stage.substring(6);
                stages.put(Integer.valueOf(stage), StepStatus.valueOf(String.valueOf(value)));
            });
        } else {
            throw new IllegalStateException("Invalid completed workflow stages");
        }
        return stages;
    }

    private JsonNode nativeToJson(Object value) {
        if (value == null) {
            return null;
        }

        return objectMapper.convertValue(value, JsonNode.class);
    }

    private void putJson(Map<String, Object> target, String name, JsonNode value) {
        if (value == null) return;
        put(target, name, jsonToNative(value));
        target.put(name + "Json", objectMapper.writeValueAsString(value));
    }

    private JsonNode readJson(Map<?, ?> source, String name) {
        if (source.containsKey(name + "Json")) {
            return objectMapper.readTree(String.valueOf(source.get(name + "Json")));
        }
        return nativeToJson(source.get(name));
    }

    private Object jsonToNative(JsonNode value) {
        if (value == null || value.isNull() || !firebaseSafeProjection(value)) return null;
        return objectMapper.convertValue(value, Object.class);
    }

    private boolean firebaseSafeProjection(JsonNode value) {
        if (value.isObject()) {
            for (Map.Entry<String, JsonNode> entry : value.properties()) {
                if (entry.getKey().isEmpty() || entry.getKey().chars()
                        .anyMatch(c -> c < 32 || c == 127 || ".#$[]/".indexOf(c) >= 0)
                        || !firebaseSafeProjection(entry.getValue())) return false;
            }
        } else if (value.isArray()) {
            for (JsonNode child : value) if (!firebaseSafeProjection(child)) return false;
        }
        return true;
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
