package mx.com.leenustechs.ciaState.business.utils.commons;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import mx.com.leenustechs.ciaState.models.records.WorkflowStage;
import mx.com.leenustechs.ciaState.models.types.StepType;
import tools.jackson.databind.ObjectMapper;

class CommonUtilitiesTest {

    @Test
    void findsStagesAndBuildsWorkflowStageVariants() {
        WorkflowStage single = WorkflowStage.single(StepType.SECURITY);
        WorkflowStage parallel = WorkflowStage.parallel(StepType.ASSETS, StepType.ROLES);
        List<WorkflowStage> workflow = List.of(single, parallel);

        assertEquals(0, StageUtils.findStageIndex(StepType.SECURITY, workflow));
        assertEquals(1, StageUtils.findStageIndex(StepType.ROLES, workflow));
        assertEquals(-1, StageUtils.findStageIndex(StepType.MODULES, workflow));
        assertFalse(single.isParallel());
        assertTrue(parallel.isParallel());
    }

    @Test
    void serializesAndDeserializesJson() {
        ObjectMapper mapper = new ObjectMapper();
        CustomSerializer serializer = new CustomSerializer(mapper);
        CustomDeserializer deserializer = new CustomDeserializer(mapper);

        byte[] bytes = serializer.serialize("topic", java.util.Map.of("answer", 42));
        Object result = deserializer.deserialize("topic", bytes);

        assertArrayEquals(mapper.writeValueAsBytes(java.util.Map.of("answer", 42)), bytes);
        assertEquals(42, ((java.util.Map<?, ?>) result).get("answer"));
    }

    @Test
    void handlesSerializationFailuresAccordingToKafkaContracts() throws Exception {
        ObjectMapper mapper = mock(ObjectMapper.class);
        when(mapper.writeValueAsBytes(any())).thenThrow(new RuntimeException("broken"));
        when(mapper.readValue(any(byte[].class), any(Class.class)))
                .thenThrow(new RuntimeException("broken"));

        assertThrows(
                SerializationException.class,
                () -> new CustomSerializer(mapper).serialize("topic", new Object()));
        assertNull(new CustomDeserializer(mapper).deserialize("topic", new byte[] { 1 }));
    }
}
