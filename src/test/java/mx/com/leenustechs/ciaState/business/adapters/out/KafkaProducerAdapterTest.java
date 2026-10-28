package mx.com.leenustechs.ciaState.business.adapters.out;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.types.OperationType;

class KafkaProducerAdapterTest {
    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final KafkaProducerAdapter adapter = new KafkaProducerAdapter(template, "cia-state");

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void retainsKeyBodyAndCorrelationHeaders() {
        CommonModel event = new CommonModel("tx", "CIA_API", OperationType.LOGIN, null);
        event.setEventId("evt");
        CompletableFuture<SendResult<String, Object>> acknowledgement = new CompletableFuture<>();
        when(template.send(any(ProducerRecord.class))).thenReturn(acknowledgement);
        assertSame(acknowledgement, adapter.publish("topic", "tx", event));
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        ProducerRecord<String, Object> record = captor.getValue();
        assertEquals("topic", record.topic());
        assertEquals("tx", record.key());
        assertSame(event, record.value());
        assertEquals("evt", new String(record.headers().lastHeader("event-id").value(), StandardCharsets.UTF_8));
        assertEquals("cia-state", new String(record.headers().lastHeader("origin-service").value(), StandardCharsets.UTF_8));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void exposesAsynchronousSendFailureToOutboxCaller() {
        RuntimeException failure = new RuntimeException("unavailable");
        CompletableFuture<SendResult<String, Object>> acknowledgement = CompletableFuture.failedFuture(failure);
        when(template.send(any(ProducerRecord.class))).thenReturn(acknowledgement);
        CompletionException thrown = assertThrows(CompletionException.class,
                () -> adapter.publish("topic", "tx", "body", "outbox-event").join());
        assertSame(failure, thrown.getCause());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void exposesSynchronousProducerConstructionFailureAsFailedFuture() {
        RuntimeException failure = new IllegalStateException("producer unavailable");
        when(template.send(any(ProducerRecord.class))).thenThrow(failure);
        CompletionException thrown = assertThrows(CompletionException.class,
                () -> adapter.publish("topic", "tx", "body").join());
        assertSame(failure, thrown.getCause());
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void doesNotInventEventIdForLegacyNonEnvelopeRecords() {
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        adapter.publish("topic", "tx", "body");
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        assertNull(captor.getValue().headers().lastHeader("event-id"));
    }
}
