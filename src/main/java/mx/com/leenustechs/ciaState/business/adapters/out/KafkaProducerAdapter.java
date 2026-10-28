package mx.com.leenustechs.ciaState.business.adapters.out;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import mx.com.leenustechs.ciaState.models.CommonModel;

@Component
public class KafkaProducerAdapter {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final String applicationName;

    public KafkaProducerAdapter(
            KafkaTemplate<String, Object> kafkaTemplate,
            @Value("${spring.application.name:cia-state}") String applicationName) {
        this.kafkaTemplate = kafkaTemplate;
        this.applicationName = applicationName;
    }

    public CompletableFuture<SendResult<String, Object>> publish(String topic, String key, Object event) {
        return publish(topic, key, event,
                event instanceof CommonModel model ? model.getEventId() : null);
    }

    /** The caller acknowledges its outbox entry only after this future succeeds. */
    public CompletableFuture<SendResult<String, Object>> publish(
            String topic, String key, Object event, String eventId) {
        ProducerRecord<String, Object> record = new ProducerRecord<>(topic, key, event);
        record.headers().add("origin-service", applicationName.getBytes(StandardCharsets.UTF_8));
        if (eventId != null) {
            record.headers().add("event-id", eventId.getBytes(StandardCharsets.UTF_8));
        }
        try {
            return kafkaTemplate.send(record);
        } catch (RuntimeException exception) {
            return CompletableFuture.failedFuture(exception);
        }
    }
}
