package mx.com.leenustechs.ciaState.business.utils.commons;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import lombok.RequiredArgsConstructor;
import mx.com.leenustechs.ciaState.models.CommonModel;
import tools.jackson.databind.ObjectMapper;

/** Decodes the event contract; malformed records must reach Kafka recovery. */
@RequiredArgsConstructor
public class CustomDeserializer implements Deserializer<CommonModel> {
    private final ObjectMapper objectMapper;

    @Override
    public CommonModel deserialize(String topic, byte[] data) {
        if (data == null) {
            return null; // Kafka tombstone, distinct from malformed JSON.
        }
        try {
            CommonModel event = objectMapper.readValue(data, CommonModel.class);
            if (event == null) {
                throw new SerializationException("JSON null is not a workflow event");
            }
            event.validate();
            return event;
        } catch (Exception exception) {
            // Jackson causes can contain source fragments; never include the payload.
            throw new SerializationException("Unable to deserialize workflow event from topic " + topic);
        }
    }
}
