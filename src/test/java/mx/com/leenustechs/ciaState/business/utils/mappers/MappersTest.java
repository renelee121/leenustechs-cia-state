package mx.com.leenustechs.ciaState.business.utils.mappers;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

class MappersTest {

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final CommonModelMapper commonMapper = Mappers.getMapper(CommonModelMapper.class);
    private final EventStateInputMapper inputMapper = Mappers.getMapper(EventStateInputMapper.class);
    private final EventStateModelMapper stateMapper = Mappers.getMapper(EventStateModelMapper.class);

    @Test
    void mapsCommonRequestsAndResponses() {
        JsonNode payload = objectMapper.createObjectNode().put("name", "Ada");
        CommonModelRequest request = new CommonModelRequest("ignored", payload);

        CommonModel model =
                commonMapper.toModel(request, "tx", "API", OperationType.LOGIN);

        assertEquals("tx", model.getTransactionId());
        assertEquals("API", model.getProducer());
        assertEquals(OperationType.LOGIN, model.getCommand());
        assertSame(payload, model.getPayload());
        assertEquals("tx", commonMapper.toResponse(model).getTransactionId());
        assertNull(commonMapper.toResponse(null));
    }

    @Test
    void mapsEventInputEntityModelAndResponse() {
        JsonNode payload = objectMapper.createObjectNode().put("input", true);
        JsonNode result = objectMapper.createObjectNode().put("ok", true);
        CommonModel event = new CommonModel("tx", "API", OperationType.LOGIN, payload);
        Instant created = Instant.parse("2025-01-01T00:00:00Z");
        Instant updated = created.plusSeconds(1);

        EventStateModel model = inputMapper.toEventStateModel(
                event,
                TransactionStatus.PROCESSING,
                0,
                Map.of(StepType.SECURITY, StepStatus.PROCESSING),
                result,
                created,
                updated);
        EventStateEntity entity = stateMapper.toEntity(model);
        EventStateModel roundTrip = stateMapper.toModel(entity);

        assertEquals("tx", model.getTransactionId());
        assertEquals(created, model.getCreatedAt());
        assertEquals(result, stateMapper.toResponse(roundTrip).getResult());
        assertNull(entity.getTtl());
        assertNull(inputMapper.toEventStateModel(null, null, null, null, null, null, null));
    }

    @Test
    void mergesPayloadAndStepsWithoutOverwritingPersistentFields() {
        EventStateEntity target = new EventStateEntity();
        JsonNode originalPayload = objectMapper.createObjectNode().put("old", 1);
        target.setPayload(originalPayload);
        target.setSteps(new EnumMap<>(Map.of(StepType.SECURITY, StepStatus.COMPLETE)));
        target.setCreatedAt(Instant.EPOCH);
        target.setTtl(99L);

        EventStateModel source = new EventStateModel();
        source.setPayload(objectMapper.createObjectNode().put("new", 2));
        source.setSteps(Map.of(StepType.ASSETS, StepStatus.PROCESSING));
        source.setStatus(TransactionStatus.PROCESSING);

        stateMapper.updateFromModel(source, target);

        assertSame(originalPayload, target.getPayload());
        assertEquals(1, target.getPayload().get("old").asInt());
        assertEquals(2, target.getPayload().get("new").asInt());
        assertEquals(2, target.getSteps().size());
        assertEquals(Instant.EPOCH, target.getCreatedAt());
        assertEquals(99L, target.getTtl());
    }

    @Test
    void initializesMissingMergeTargetsAndIgnoresNullSources() {
        EventStateEntity target = new EventStateEntity();
        EventStateModel source = new EventStateModel();
        JsonNode payload = objectMapper.createObjectNode().put("value", 1);
        source.setPayload(payload);
        source.setSteps(Map.of(StepType.MODULES, StepStatus.COMPLETE));

        stateMapper.mergePayload(source, target);
        stateMapper.mergeSteps(source, target);

        assertNotSame(payload, target.getPayload());
        assertEquals(payload, target.getPayload());
        assertTrue(target.getSteps() instanceof EnumMap);

        EventStateModel empty = new EventStateModel();
        stateMapper.mergePayload(empty, target);
        stateMapper.mergeSteps(empty, target);
        assertEquals(payload, target.getPayload());
        assertEquals(1, target.getSteps().size());
    }
}
