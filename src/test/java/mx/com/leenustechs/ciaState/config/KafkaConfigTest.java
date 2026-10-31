package mx.com.leenustechs.ciaState.config;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import mx.com.leenustechs.ciaState.models.CommonModel;
import mx.com.leenustechs.ciaState.models.types.OperationType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.json.JsonMapper;

class KafkaConfigTest {
    private final ObjectMapper mapper = JsonMapper.builder().build();

    @Test
    void factoriesUseInjectedSerializersAndRecordAcknowledgements() {
        KafkaConfig config = new KafkaConfig(mapper, "user", "password", "localhost:9093", "cia");
        DefaultKafkaProducerFactory<String, Object> producer =
                (DefaultKafkaProducerFactory<String, Object>) config.producerFactory();
        DefaultKafkaConsumerFactory<String, CommonModel> consumer =
                (DefaultKafkaConsumerFactory<String, CommonModel>) config.consumerFactory();
        CommonModel event = new CommonModel("tx", "CIA_API", OperationType.LOGIN, mapper.createObjectNode());
        byte[] bytes = producer.getValueSerializer().serialize("lobby", event);
        assertEquals(event, consumer.getValueDeserializer().deserialize("lobby", new RecordHeaders(), bytes));
        assertEquals("tx", consumer.getKeyDeserializer().deserialize("lobby",
                producer.getKeySerializer().serialize("lobby", "tx")));
        assertEquals(true, producer.getConfigurationProperties().get("enable.idempotence"));
        assertEquals("all", producer.getConfigurationProperties().get("acks"));
        assertEquals(1_000L, producer.getConfigurationProperties().get("max.block.ms"));
        assertEquals(10_000, producer.getConfigurationProperties().get("delivery.timeout.ms"));
        assertEquals(5_000, producer.getConfigurationProperties().get("request.timeout.ms"));
        assertEquals(0, producer.getConfigurationProperties().get("linger.ms"));
        assertEquals(false, consumer.getConfigurationProperties().get("enable.auto.commit"));
        assertTrue(config.getSaslJaasConfig().startsWith("org.apache.kafka.common.security.scram.ScramLoginModule"));
        assertEquals(ContainerProperties.AckMode.RECORD,
                config.kafkaListenerContainerFactory().getContainerProperties().getAckMode());
        assertNotNull(config.kafkaErrorHandler());
    }

    @Test
    void jaasCredentialsAreEscapedAsLiterals() {
        KafkaConfig config = new KafkaConfig(mapper, "a\"b", "c\\d", "localhost:9093", "cia");
        assertTrue(config.getSaslJaasConfig().contains("username=\"a\\\"b\""));
        assertTrue(config.getSaslJaasConfig().contains("password=\"c\\\\d\""));
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void malformedRecordRetainsOriginalBytesInDeadLetterTopic() {
        KafkaTemplate<String, Object> template = template();
        when(template.send(any(ProducerRecord.class))).thenReturn(CompletableFuture.completedFuture(null));
        DeadLetterPublishingRecoverer recoverer = config(template).deadLetterPublishingRecoverer();
        byte[] bytes = "{invalid-secret".getBytes(StandardCharsets.UTF_8);
        ErrorHandlingDeserializer<CommonModel> decoder = new ErrorHandlingDeserializer<>(
                new mx.com.leenustechs.ciaState.business.utils.commons.CustomDeserializer(mapper));
        RecordHeaders headers = new RecordHeaders();
        assertNull(decoder.deserialize("response", headers, bytes));
        ConsumerRecord<String, CommonModel> record = new ConsumerRecord<>("response", 2, 42L, "tx", null);
        headers.forEach(header -> record.headers().add(header));
        recoverer.accept(record, null, new IllegalArgumentException("invalid event"));
        ArgumentCaptor<ProducerRecord<String, Object>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(captor.capture());
        ProducerRecord<String, Object> recovered = captor.getValue();
        assertEquals("response.DLT", recovered.topic());
        assertNull(recovered.partition());
        assertArrayEquals(bytes, (byte[]) recovered.value());
        assertEquals("tx", recovered.key());
        // Sensitive exception details are not propagated into DLT metadata.
        for (org.apache.kafka.common.header.Header header : recovered.headers()) {
            assertFalse(new String(header.value(), StandardCharsets.UTF_8).contains("invalid-secret"));
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    void failedDeadLetterSendFailsRecoverySoInputIsNotAcknowledged() {
        KafkaTemplate<String, Object> template = template();
        when(template.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.failedFuture(new IllegalStateException("broker unavailable")));
        DeadLetterPublishingRecoverer recoverer = config(template).deadLetterPublishingRecoverer();
        ConsumerRecord<String, CommonModel> record = new ConsumerRecord<>("response", 0, 1L, "tx",
                new CommonModel("tx", "SECURITY", OperationType.LOGIN, null));
        assertThrows(RuntimeException.class,
                () -> recoverer.accept(record, null, new IllegalStateException("workflow temporarily unavailable")));
    }

    @Test
    void configuredProducerBoundsCoverBothSynchronousEnqueueAndAsyncDelivery() {
        KafkaConfig config = new KafkaConfig(mapper, "user", "password", "localhost:9093", "cia",
                250L, 3_000, 2_000, 100);
        var properties = config.producerFactory().getConfigurationProperties();
        assertEquals(250L, properties.get("max.block.ms"));
        assertEquals(3_000, properties.get("delivery.timeout.ms"));
        assertEquals(2_000, properties.get("request.timeout.ms"));
        assertEquals(100, properties.get("linger.ms"));
    }

    @Test
    void rejectsIncoherentProducerLimitsBeforeConstructingAClient() {
        assertThrows(IllegalArgumentException.class, () -> new KafkaConfig(mapper, "u", "p", "localhost:9093", "cia", 0, 3_000, 2_000, 0));
        assertThrows(IllegalArgumentException.class, () -> new KafkaConfig(mapper, "u", "p", "localhost:9093", "cia", 100, 0, 2_000, 0));
        assertThrows(IllegalArgumentException.class, () -> new KafkaConfig(mapper, "u", "p", "localhost:9093", "cia", 100, 3_000, 0, 0));
        assertThrows(IllegalArgumentException.class, () -> new KafkaConfig(mapper, "u", "p", "localhost:9093", "cia", 100, 3_000, 2_000, -1));
        assertThrows(IllegalArgumentException.class, () -> new KafkaConfig(mapper, "u", "p", "localhost:9093", "cia", 100, 2_000, 2_000, 1));
    }

    @Test
    void listenerStartupCanBeDisabledForApplicationContextValidation() {
        KafkaConfig config = new KafkaConfig(mapper, "u", "p", "localhost:9093", "cia");
        org.springframework.test.util.ReflectionTestUtils.setField(config, "listenerAutoStartup", false);
        assertFalse(config.kafkaListenerContainerFactory().createContainer("lobby").isAutoStartup());
    }

    private KafkaConfig config(KafkaTemplate<String, Object> template) {
        return new KafkaConfig(mapper, "user", "password", "localhost:9093", "cia") {
            @Override public KafkaTemplate<String, Object> kafkaTemplate() { return template; }
        };
    }

    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, Object> template() {
        KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
        ProducerFactory<String, Object> producerFactory = mock(ProducerFactory.class);
        when(producerFactory.getConfigurationProperties()).thenReturn(Map.of("delivery.timeout.ms", 1000));
        when(template.getProducerFactory()).thenReturn(producerFactory);
        return template;
    }
}
