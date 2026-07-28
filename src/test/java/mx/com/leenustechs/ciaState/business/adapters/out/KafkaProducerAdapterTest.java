package mx.com.leenustechs.ciaState.business.adapters.out;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.util.ReflectionTestUtils;

class KafkaProducerAdapterTest {

    private final KafkaTemplate<String, Object> kafkaTemplate = mock(KafkaTemplate.class);
    private KafkaProducerAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new KafkaProducerAdapter(kafkaTemplate);
        ReflectionTestUtils.setField(adapter, "applicationName", "cia-state");
    }

    @Test
    void publishesRecordWithOriginHeaderAndHandlesSuccess() {
        ProducerRecord<String, Object> sentRecord =
                new ProducerRecord<>("topic", "key", "event");
        RecordMetadata metadata = new RecordMetadata(
                new TopicPartition("topic", 2), 10L, 0, 0L, 3, 5);
        SendResult<String, Object> result = new SendResult<>(sentRecord, metadata);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        adapter.publish("topic", "key", "event");

        ProducerRecord<String, Object> record = capturedRecord();
        assertEquals("topic", record.topic());
        assertEquals("key", record.key());
        assertEquals("event", record.value());
        assertArrayEquals(
                "cia-state".getBytes(StandardCharsets.UTF_8),
                record.headers().lastHeader("origin-service").value());
    }

    @Test
    void handlesAsynchronousPublishingFailure() {
        CompletableFuture<SendResult<String, Object>> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalStateException("Kafka unavailable"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(failed);

        adapter.publish("topic", "key", "event");

        ProducerRecord<String, Object> record = capturedRecord();
        assertEquals("topic", record.topic());
        assertEquals("key", record.key());
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    private ProducerRecord<String, Object> capturedRecord() {
        ArgumentCaptor<ProducerRecord<String, Object>> captor =
                (ArgumentCaptor) ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());
        return captor.getValue();
    }
}
