package mx.com.leenustechs.ciaState.business.repositories;

import java.util.LinkedHashMap;
import java.util.Map;

import com.google.firebase.database.core.Path;
import com.google.firebase.database.core.ValidationPath;
import com.google.firebase.database.snapshot.NodeUtilities;
import com.google.firebase.database.utilities.Validation;
import com.google.firebase.database.utilities.encoding.CustomClassMapper;

/**
 * Keeps exact JSON companions when a native realtime projection is not writable.
 * The validation pipeline mirrors MutableData.setValue in the pinned Firebase SDK.
 * Internal SDK classes are intentionally confined to this adapter helper.
 */
final class FirebaseValueValidator {
    private static final String INVALID_PATH = "Workflow path exceeds Firebase constraints";
    private static final String INVALID_VALUE = "Workflow value exceeds Firebase constraints";

    private FirebaseValueValidator() { }

    static void validateWorkflowPath(String workflowPath, String transactionId) {
        try {
            Path base = basePath(workflowPath, transactionId);
            ValidationPath.validateWithObject(base, null);
        } catch (RuntimeException failure) {
            // SDK diagnostics can contain user-controlled paths and values.
            throw new IllegalArgumentException(INVALID_PATH);
        }
    }

    static Map<String, Object> prepare(String workflowPath, String transactionId, Map<String, Object> encoded) {
        try {
            Path base = basePath(workflowPath, transactionId);
            Map<String, Object> prepared = copyMap(encoded);
            projection(prepared, "initialPayload", "initialPayloadJson", base);
            projection(prepared, "payload", "payloadJson", base);
            projection(prepared, "result", "resultJson", base);
            if (prepared.get("stepResults") instanceof Map<?, ?> results) {
                Map<String, Object> safeResults = copyMap(results);
                for (Map.Entry<String, Object> entry : safeResults.entrySet()) {
                    if (entry.getValue() instanceof Map<?, ?> wrapper) {
                        Map<String, Object> safeWrapper = copyMap(wrapper);
                        projection(safeWrapper, "payload", "json", child(child(base, "stepResults"), entry.getKey()));
                        entry.setValue(safeWrapper);
                    }
                }
                prepared.put("stepResults", safeResults);
            }
            if (prepared.get("pendingOutbox") instanceof Map<?, ?> outbox) {
                Map<String, Object> safeOutbox = copyMap(outbox);
                for (Map.Entry<String, Object> entry : safeOutbox.entrySet()) {
                    if (entry.getValue() instanceof Map<?, ?> envelope) {
                        Map<String, Object> safeEnvelope = copyMap(envelope);
                        projection(safeEnvelope, "event", "eventJson", child(child(base, "pendingOutbox"), entry.getKey()));
                        entry.setValue(safeEnvelope);
                    }
                }
                prepared.put("pendingOutbox", safeOutbox);
            }
            // Canonical strings and all metadata must fit too; they cannot be dropped.
            validateWritable(base, prepared);
            return prepared;
        } catch (RuntimeException failure) {
            throw new IllegalArgumentException(INVALID_VALUE);
        }
    }

    private static void projection(Map<String, Object> container, String nativeName, String jsonName, Path parent) {
        if (!container.containsKey(jsonName)) return;
        if (!(container.get(jsonName) instanceof String)) {
            throw new IllegalArgumentException(INVALID_VALUE);
        }
        if (container.containsKey(nativeName)) {
            try {
                validateWritable(child(parent, nativeName), container.get(nativeName));
            } catch (RuntimeException unsupportedProjection) {
                container.remove(nativeName);
            }
        }
    }

    private static void validateWritable(Path path, Object value) {
        ValidationPath.validateWithObject(path, value);
        validatePlainJsonKeys(value);
        Object plain = CustomClassMapper.convertToPlainJavaTypes(value);
        Validation.validateWritableObject(plain);
        NodeUtilities.NodeFromJSON(plain);
    }

    private static void validatePlainJsonKeys(Object value) {
        if (value instanceof Map<?, ?> map) {
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                if (!(entry.getKey() instanceof String key)) throw new IllegalArgumentException(INVALID_VALUE);
                validateKey(key);
                validatePlainJsonKeys(entry.getValue());
            }
        } else if (value instanceof Iterable<?> values) {
            for (Object child : values) validatePlainJsonKeys(child);
        }
    }

    private static Path basePath(String workflowPath, String transactionId) {
        if (workflowPath == null || workflowPath.isBlank() || transactionId == null) {
            throw new IllegalArgumentException(INVALID_PATH);
        }
        Validation.validatePathString(workflowPath);
        Path root = new Path(workflowPath);
        if (root.isEmpty()) throw new IllegalArgumentException(INVALID_PATH);
        for (var key : root) validateKey(key.asString());
        validateKey(transactionId);
        return child(root, transactionId);
    }

    private static void validateKey(String key) {
        if (key.isEmpty() || key.chars().anyMatch(c -> c < 32 || c == 127 || ".#$[]/".indexOf(c) >= 0)) {
            throw new IllegalArgumentException(INVALID_VALUE);
        }
        Validation.validateWritableKey(key);
    }

    private static Path child(Path parent, String key) {
        return parent.child(new Path(key));
    }

    private static Map<String, Object> copyMap(Map<?, ?> source) {
        Map<String, Object> copy = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : source.entrySet()) {
            if (!(entry.getKey() instanceof String key)) throw new IllegalArgumentException(INVALID_VALUE);
            copy.put(key, entry.getValue());
        }
        return copy;
    }
}
