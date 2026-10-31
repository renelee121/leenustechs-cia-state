package mx.com.leenustechs.ciaState.business.utils.mappers;

import static org.assertj.core.api.Assertions.*;
import java.time.Instant;
import java.util.EnumMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.mapstruct.factory.Mappers;
import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.EventStateModel;
import mx.com.leenustechs.ciaState.models.entities.EventStateEntity;
import mx.com.leenustechs.ciaState.models.requests.CommonModelRequest;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import mx.com.leenustechs.ciaState.models.types.StepStatus;
import mx.com.leenustechs.ciaState.models.types.StepType;
import mx.com.leenustechs.ciaState.models.types.TransactionStatus;
import tools.jackson.databind.json.JsonMapper;

class WorkflowMappingTest {
    private final JsonMapper json = JsonMapper.builder().build();

    @Test
    void requestMappingUsesExplicitTransactionIdentityAndKeepsCorrelationUnset() {
        CommonModelMapper mapper = Mappers.getMapper(CommonModelMapper.class);
        var payload = json.createObjectNode().put("user", "test");
        CommonModel mapped = mapper.toModel(new CommonModelRequest("untrusted-id", payload), "server-id", "CIA_API", OperationType.LOGIN);
        assertThat(mapped.getTransactionId()).isEqualTo("server-id");
        assertThat(mapped.getProducer()).isEqualTo("CIA_API");
        assertThat(mapped.getCommand()).isEqualTo(OperationType.LOGIN);
        assertThat(mapped.getPayload()).isEqualTo(payload);
        assertThat(mapped.getExecutionId()).isNull();
        assertThat(mapped.getEventId()).isNull();
        assertThat(mapped.getOutcome()).isNull();
        assertThat(mapped.getStageIndex()).isNull();
        assertThat(mapper.toResponse(null)).isNull();
        assertThat(mapper.toModel(null, null, null, null)).isNull();
    }

    @Test
    void stateMappingPreservesDurableResultsAndTimestamps() {
        EventStateInputMapper input = Mappers.getMapper(EventStateInputMapper.class);
        EventStateModelMapper mapper = Mappers.getMapper(EventStateModelMapper.class);
        Instant now = Instant.parse("2026-10-01T00:00:00Z");
        var payload = json.createObjectNode().put("input", true);
        var result = json.createObjectNode().put("success", true);
        EventStateModel model = input.toEventStateModel(new CommonModel("tx", "SECURITY", OperationType.LOGIN, payload),
                TransactionStatus.PROCESSING, 0, Map.of(StepType.SECURITY, StepStatus.COMPLETE), result, now, now);
        model.setCompletedStages(Map.of(0, StepStatus.COMPLETE));
        model.setStepResults(Map.of("0:SECURITY", result.toString()));
        EventStateEntity entity = mapper.toEntity(model);
        EventStateModel roundtrip = mapper.toModel(entity);
        assertThat(roundtrip).isEqualTo(model);
        assertThat(mapper.toResponse(roundtrip).getStatus()).isEqualTo(TransactionStatus.PROCESSING);
        assertThat(mapper.toResponse(roundtrip).getResult()).isEqualTo(result);
        assertThat(entity.getPendingOutbox()).isNull();
        assertThat(entity.getExecutionId()).isNull();
        assertThat(mapper.toEntity(null)).isNull();
        assertThat(mapper.toModel(null)).isNull();
        assertThat(mapper.toResponse(null)).isNull();
        assertThat(input.toEventStateModel(null, null, null, null, null, null, null)).isNull();
    }

    @Test
    void compatibilityUpdateKeepsCreationAndCoordinationState() {
        EventStateModelMapper mapper = Mappers.getMapper(EventStateModelMapper.class);
        EventStateEntity target = new EventStateEntity();
        Instant created = Instant.parse("2026-10-01T00:00:00Z");
        target.setCreatedAt(created);
        target.setExecutionId("run");
        target.setCompletedStages(Map.of(0, StepStatus.COMPLETE));
        target.setStepResults(Map.of("0:SECURITY", "{}"));
        target.setPayload(json.createObjectNode().put("existing", true));
        target.setSteps(new EnumMap<>(StepType.class));
        target.getSteps().put(StepType.SECURITY, StepStatus.COMPLETE);
        EventStateModel source = new EventStateModel();
        source.setTransactionId("tx");
        source.setStatus(TransactionStatus.PROCESSING);
        source.setPayload(json.createObjectNode().put("new", true));
        source.setSteps(Map.of(StepType.MODULES, StepStatus.PROCESSING));
        mapper.updateFromModel(source, target);
        assertThat(target.getCreatedAt()).isEqualTo(created);
        assertThat(target.getExecutionId()).isEqualTo("run");
        assertThat(target.getCompletedStages()).containsEntry(0, StepStatus.COMPLETE);
        assertThat(target.getStepResults()).containsEntry("0:SECURITY", "{}");
        assertThat(target.getPayload().get("existing").asBoolean()).isTrue();
        assertThat(target.getPayload().get("new").asBoolean()).isTrue();
        assertThat(target.getSteps()).containsEntry(StepType.SECURITY, StepStatus.COMPLETE).containsEntry(StepType.MODULES, StepStatus.PROCESSING);
    }

    @Test
    void compatibilityMergeHandlesMissingPayloadAndStepsWithoutAliasingInputPayload() {
        EventStateModelMapper mapper = Mappers.getMapper(EventStateModelMapper.class);
        EventStateModel source = new EventStateModel();
        EventStateEntity target = new EventStateEntity();
        mapper.mergePayload(source, target);
        mapper.mergeSteps(source, target);
        assertThat(target.getPayload()).isNull();
        assertThat(target.getSteps()).isNull();
        var payload = json.createObjectNode().put("field", 1);
        source.setPayload(payload);
        source.setSteps(Map.of(StepType.SECURITY, StepStatus.PROCESSING));
        mapper.mergePayload(source, target);
        mapper.mergeSteps(source, target);
        payload.put("field", 2);
        assertThat(target.getPayload().get("field").asInt()).isEqualTo(1);
        assertThat(target.getSteps()).containsEntry(StepType.SECURITY, StepStatus.PROCESSING);
        source.setPayload(json.getNodeFactory().textNode("scalar"));
        mapper.mergePayload(source, target);
        assertThat(target.getPayload().isObject()).isTrue();
        mapper.updateFromModel(null, target);
    }
}
