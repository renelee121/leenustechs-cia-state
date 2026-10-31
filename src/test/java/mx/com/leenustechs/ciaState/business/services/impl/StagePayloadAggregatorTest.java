package mx.com.leenustechs.ciaState.business.services.impl;

import static org.junit.jupiter.api.Assertions.*;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import mx.com.leenustechs.ciaState.models.records.StageResultSnapshot;
import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.StepType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

class StagePayloadAggregatorTest {
    final JsonMapper mapper = JsonMapper.builder().build();
    final StagePayloadAggregator aggregator = new StagePayloadAggregator();
    final WorkflowStage single = WorkflowStage.single(StepType.SECURITY);
    final WorkflowStage parallel = WorkflowStage.parallel(StepType.ASSETS, StepType.ROLES);
    JsonNode json(String value) { return mapper.readTree(value); }
    JsonNode merge(String input, String a, String b) {
        return aggregator.aggregate(new StageResultSnapshot(input == null ? null : json(input),
                Map.of(StepType.ASSETS, json(a), StepType.ROLES, json(b))), parallel);
    }
    JsonNode single(String input, String result) {
        return aggregator.aggregate(new StageResultSnapshot(input == null ? null : json(input),
                Map.of(StepType.SECURITY, json(result))), single);
    }
    @Test void preservesInputAndCombinesIndependentNestedChanges() {
        assertEquals(json("{\"base\":1,\"user\":{\"name\":\"B\",\"role\":\"admin\"}}"),
                merge("{\"base\":1,\"user\":{\"name\":\"A\"}}",
                        "{\"base\":1,\"user\":{\"name\":\"B\"}}", "{\"user\":{\"name\":\"A\",\"role\":\"admin\"}}"));
        assertEquals(json("{\"new\":{\"a\":1,\"b\":2}}"), merge(null, "{\"new\":{\"a\":1}}", "{\"new\":{\"b\":2}}"));
        assertEquals(json("{\"x\":1}"), merge("null", "{\"x\":1}", "{\"x\":1}"));
    }
    @Test void rejectsConflictingWritesAndIncompatibleTypes() {
        assertThrows(IllegalStateException.class, () -> merge("{}", "{\"x\":1}", "{\"x\":2}"));
        assertThrows(IllegalStateException.class, () -> merge("{}", "{\"x\":1}", "{\"x\":{\"a\":2}}"));
        assertThrows(IllegalStateException.class, () -> merge("{}", "{\"x\":{\"a\":1}}", "{\"x\":2}"));
        assertThrows(IllegalStateException.class, () -> merge("[]", "{}", "{}"));
        assertThrows(IllegalStateException.class, () -> merge("{}", "[]", "{}"));
        assertThrows(IllegalStateException.class, () -> aggregator.aggregate(new StageResultSnapshot(null, Map.of()), parallel));
        assertThrows(IllegalStateException.class, () -> aggregator.aggregate(new StageResultSnapshot(null, Map.of()), single));
    }
    @Test void nullResultsPreserveBaselineAndSingleResultsCanReplaceScalars() {
        assertEquals(json("{\"base\":1}"), merge("{\"base\":1}", "null", "null"));
        assertNull(single(null, "null"));
        assertEquals(json("{\"base\":1}"), single("{\"base\":1}", "null"));
        assertEquals(json("[1,2]"), single(null, "[1,2]"));
        assertEquals(json("3"), single("1", "3"));
        assertEquals(json("{\"a\":1,\"b\":2}"), single("{\"a\":1}", "{\"b\":2}"));
    }
    @Test void mergeDoesNotMutateBaselineOrWorkerResults() {
        JsonNode input = json("{\"user\":{\"a\":1}}");
        JsonNode first = json("{\"user\":{\"b\":2}}");
        Map<StepType, JsonNode> results = new EnumMap<>(StepType.class);
        results.put(StepType.ASSETS, first); results.put(StepType.ROLES, json("null"));
        JsonNode merged = aggregator.aggregate(new StageResultSnapshot(input, results), parallel);
        assertEquals(json("{\"user\":{\"a\":1}}"), input);
        assertEquals(json("{\"user\":{\"b\":2}}"), first);
        assertEquals(json("{\"user\":{\"a\":1,\"b\":2}}"), merged);
    }
}
