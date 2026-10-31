package mx.com.leenustechs.ciaState.business.repositories;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.google.firebase.database.core.Path;
import com.google.firebase.database.core.ValidationPath;
import com.google.firebase.database.snapshot.NodeUtilities;

class FirebaseValueValidatorTest {
    @Test
    void keepsNormalRealtimeProjectionAndCanonicalJson() {
        Map<String, Object> source = Map.of("payload", Map.of("count", 3L), "payloadJson", "{\"count\":3}");
        var prepared = FirebaseValueValidator.prepare("workflows", "tx", source);
        assertEquals(source, prepared);
        assertWritable(prepared);
    }

    @Test
    void unsupportedNumbersRetainExactCanonicalDataWithoutNativeProjection() {
        String exact = "{\"n\":9223372036854775808}";
        for (Object number : List.of(new BigInteger("9223372036854775808"), Double.POSITIVE_INFINITY, Double.NaN)) {
            var source = Map.<String, Object>of("payload", Map.of("n", number), "payloadJson", exact);
            var prepared = FirebaseValueValidator.prepare("workflows", "tx", source);
            assertFalse(prepared.containsKey("payload"));
            assertEquals(exact, prepared.get("payloadJson"));
            assertTrue(source.containsKey("payload"));
            assertWritable(prepared);
        }
    }

    @Test
    void invalidKeysAndServerValueMarkersNeverBecomeNativeProjections() {
        for (String key : List.of("a/b", "a.b", "", ".sv", "x".repeat(800))) {
            var prepared = FirebaseValueValidator.prepare("workflows", "tx",
                    Map.of("initialPayload", Map.of(key, "secret"), "initialPayloadJson", "exact original JSON"));
            assertFalse(prepared.containsKey("initialPayload"));
            assertEquals("exact original JSON", prepared.get("initialPayloadJson"));
            assertWritable(prepared);
        }
    }

    @Test
    void deeplyNestedAndArrayProjectionsFallBackToTheirCanonicalCompanions() {
        Object nested = Map.of("leaf", true);
        for (int i = 0; i < 35; i++) nested = Map.of("nested", nested);
        for (Object projection : List.of(nested, List.of(nested))) {
            var prepared = FirebaseValueValidator.prepare("workflows", "tx",
                    Map.of("result", projection, "resultJson", "exact deep JSON"));
            assertFalse(prepared.containsKey("result"));
            assertEquals("exact deep JSON", prepared.get("resultJson"));
            assertWritable(prepared);
        }
    }

    @Test
    void checksStepResultAndOutboxProjectionsAtTheirActualNestedPaths() {
        Object invalid = Map.of("n", new BigInteger("9223372036854775808"));
        var prepared = FirebaseValueValidator.prepare("workflows", "tx", Map.of(
                "stepResults", Map.of("0:SECURITY", Map.of("present", true, "json", "exact-step", "payload", invalid)),
                "pendingOutbox", Map.of("event-id", Map.of("eventJson", "exact-event", "event", Map.of("payload", invalid)))));
        Map<?, ?> step = (Map<?, ?>) ((Map<?, ?>) prepared.get("stepResults")).get("0:SECURITY");
        Map<?, ?> event = (Map<?, ?>) ((Map<?, ?>) prepared.get("pendingOutbox")).get("event-id");
        assertFalse(step.containsKey("payload"));
        assertEquals("exact-step", step.get("json"));
        assertFalse(event.containsKey("event"));
        assertEquals("exact-event", event.get("eventJson"));
        assertWritable(prepared);
    }

    @Test
    void validatesConfiguredRootAndFullMetadataPathBeforeWriting() {
        FirebaseValueValidator.validateWorkflowPath("tenant/workflows", "tx");
        for (String id : List.of("x".repeat(768), "bad/id", "", "secret\nvalue")) {
            var failure = assertThrows(IllegalArgumentException.class,
                    () -> FirebaseValueValidator.validateWorkflowPath("workflows", id));
            assertEquals("Workflow path exceeds Firebase constraints", failure.getMessage());
            assertNull(failure.getCause());
        }
        for (String root : List.of("", "/", "bad.root", "x/".repeat(33))) {
            assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.validateWorkflowPath(root, "tx"));
        }
        assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.validateWorkflowPath(null, "tx"));
        assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.validateWorkflowPath("workflows", null));
        // The base alone fits, but internal metadata still needs room.
        String boundary = "x".repeat(750);
        FirebaseValueValidator.validateWorkflowPath("workflows", boundary);
        assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.prepare("workflows", boundary,
                Map.of("pendingOutbox", Map.of("event-id", Map.of("eventJson", "exact")))));
    }

    @Test
    void unsupportedRequiredMetadataIsRejectedWithoutPayloadDiagnostics() {
        var failure = assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.prepare("workflows", "tx",
                Map.of("required-secret", new BigInteger("9223372036854775808"))));
        assertEquals("Workflow value exceeds Firebase constraints", failure.getMessage());
        assertNull(failure.getCause());
        assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.prepare("workflows", "tx",
                Map.of("payload", Map.of("bad/key", "secret"))));
        assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.prepare("workflows", "tx",
                Map.of("payloadJson", 12)));
        var nonStringKey = new LinkedHashMap<Object, Object>(); nonStringKey.put(1, "secret");
        assertThrows(IllegalArgumentException.class, () -> FirebaseValueValidator.prepare("workflows", "tx",
                Map.of("metadata", nonStringKey)));
    }

    private void assertWritable(Map<String, Object> prepared) {
        assertDoesNotThrow(() -> {
            ValidationPath.validateWithObject(new Path("workflows/tx"), prepared);
            NodeUtilities.NodeFromJSON(prepared).getValue();
        });
    }
}
