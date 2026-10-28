package mx.com.leenustechs.ciaState.business.utils.commons;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import lombok.RequiredArgsConstructor;
import tools.jackson.databind.ObjectMapper;

@RequiredArgsConstructor
public class CustomSerializer implements Serializer<Object> {
    private final ObjectMapper objectMapper;

    @Override
    public byte[] serialize(String topic, Object data) {
        if (data == null) {
            return null;
        }
        // ErrorHandlingDeserializer restores invalid records as bytes for DLT recovery.
        if (data instanceof byte[] bytes) {
            return bytes;
        }
        try {
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception exception) {
            throw new SerializationException("Unable to serialize workflow event for topic " + topic);
        }
    }
}
