package mx.com.leenustechs.ciaState.business.utils.commons;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.types.EventOutcome;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class KafkaSerializationTest {
    private final ObjectMapper mapper = JsonMapper.builder().build();
    private final CustomSerializer serializer = new CustomSerializer(mapper);
    private final CustomDeserializer deserializer = new CustomDeserializer(mapper);

    @Test
    void roundTripsTypedEnvelopeWithCorrelationMetadata() {
        CommonModel event = new CommonModel("tx-1", "SECURITY", OperationType.LOGIN,
                mapper.readTree("{\"name\":\"José\"}"));
        event.setEventId("event-1");
        event.setExecutionId("execution-1");
        event.setStageIndex(0);
        event.setOutcome(EventOutcome.SUCCESS);
        CommonModel decoded = deserializer.deserialize("response", serializer.serialize("response", event));
        assertEquals(event, decoded);
        assertEquals("José", decoded.getPayload().get("name").asText());
    }

    @Test
    void preservesTombstonesAndRawDeadLetterBytes() {
        assertNull(serializer.serialize("response", null));
        assertNull(deserializer.deserialize("response", null));
        byte[] malformed = "{not-json-secret".getBytes(StandardCharsets.UTF_8);
        assertSame(malformed, serializer.serialize("response.DLT", malformed));
    }

    @Test
    void malformedDataThrowsWithoutLeakingItsContent() {
        SerializationException error = assertThrows(SerializationException.class,
                () -> deserializer.deserialize("response", "{secret-password".getBytes(StandardCharsets.UTF_8)));
        assertFalse(error.getMessage().contains("secret-password"));
        assertNull(error.getCause());
    }

    @Test
    void rejectsInvalidEnvelopeAndJsonNullInsteadOfSilentlyDiscarding() {
        for (String json : new String[]{"null", "{}", "{\"transactionId\":\"bad/path\",\"producer\":\"SECURITY\",\"command\":\"LOGIN\"}",
                "{\"transactionId\":\"tx\",\"producer\":\"SECURITY\",\"command\":\"INVALID\"}"}) {
            assertThrows(SerializationException.class,
                    () -> deserializer.deserialize("response", json.getBytes(StandardCharsets.UTF_8)));
        }
    }

    @Test
    void serializationFailureIsSanitized() {
        ObjectMapper broken = org.mockito.Mockito.mock(ObjectMapper.class);
        org.mockito.Mockito.when(broken.writeValueAsBytes(org.mockito.ArgumentMatchers.any()))
                .thenThrow(new IllegalArgumentException("secret payload"));
        SerializationException error = assertThrows(SerializationException.class,
                () -> new CustomSerializer(broken).serialize("response", new Object()));
        assertFalse(error.getMessage().contains("secret payload"));
        assertNull(error.getCause());
    }
}
