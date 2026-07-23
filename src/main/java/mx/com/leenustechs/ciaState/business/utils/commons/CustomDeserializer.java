package mx.com.leenustechs.ciaState.business.utils.commons;

import org.apache.kafka.common.serialization.Deserializer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import tools.jackson.databind.ObjectMapper;

@Slf4j
@RequiredArgsConstructor
public class CustomDeserializer implements Deserializer<Object> {
    private final ObjectMapper objectMapper;

    @Override
    public Object deserialize(String topic, byte[] data) {
        try {
            log.info("Deserializing message from topic {}: {}", topic, new String(data));
            return objectMapper.readValue(data, Object.class);
        } catch (Exception e) {
            log.error("Error deserializing message from topic {}: {}", topic, e.getMessage(), e);
            return null;
        }
    }
    
}
