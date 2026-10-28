package mx.com.leenustechs.ciaState.config;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.ExponentialBackOffWithMaxRetries;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import mx.com.leenustechs.ciaState.business.utils.commons.CustomDeserializer;
import mx.com.leenustechs.ciaState.business.utils.commons.CustomSerializer;
import mx.com.leenustechs.ciaState.models.CommonModel;
import tools.jackson.databind.ObjectMapper;

@EnableKafka
@Configuration
public class KafkaConfig {
    private static final String SECURITY_PROTOCOL = "SASL_SSL";
    private static final String SASL_MECHANISM = "SCRAM-SHA-512";
    private static final String SASL_JAAS_CONFIG =
            "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

    private final ObjectMapper objectMapper;
    private final String kafkaUsername;
    private final String kafkaPassword;
    private final String kafkaBootstrapServers;
    private final String kafkaGroupId;

    public KafkaConfig(
            ObjectMapper objectMapper,
            @Value("${kafka.username}") String kafkaUsername,
            @Value("${kafka.password}") String kafkaPassword,
            @Value("${kafka.bootstrap.servers}") String kafkaBootstrapServers,
            @Value("${kafka.group.id}") String kafkaGroupId) {
        this.objectMapper = objectMapper;
        this.kafkaUsername = kafkaUsername;
        this.kafkaPassword = kafkaPassword;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaGroupId = kafkaGroupId;
    }

    public String getSaslJaasConfig() {
        return SASL_JAAS_CONFIG.formatted(escapeJaas(kafkaUsername), escapeJaas(kafkaPassword));
    }

    private String escapeJaas(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private Map<String, Object> securityProperties() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("bootstrap.servers", kafkaBootstrapServers);
        properties.put("security.protocol", SECURITY_PROTOCOL);
        properties.put("sasl.mechanism", SASL_MECHANISM);
        properties.put("sasl.jaas.config", getSaslJaasConfig());
        return properties;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> properties = securityProperties();
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new DefaultKafkaProducerFactory<>(properties,
                new StringSerializer(), new CustomSerializer(objectMapper));
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, CommonModel> consumerFactory() {
        Map<String, Object> properties = securityProperties();
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(properties,
                () -> new ErrorHandlingDeserializer<>(new StringDeserializer()),
                () -> new ErrorHandlingDeserializer<>(new CustomDeserializer(objectMapper)));
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(
                kafkaTemplate(), (record, exception) -> new TopicPartition(record.topic() + ".DLT", -1));
        recoverer.setFailIfSendResultIsError(true);
        recoverer.setWaitForSendResultTimeout(Duration.ofSeconds(30));
        recoverer.setStripPreviousExceptionHeaders(true);
        recoverer.setLogRecoveryRecord(false);
        // Exception messages/stack traces can include listener arguments or payloads.
        recoverer.setExceptionHeadersCreator((headers, exception, isKey, headerNames) ->
                headers.add("exception-type", exception.getClass().getName()
                        .getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        return recoverer;
    }

    @Bean
    public DefaultErrorHandler kafkaErrorHandler() {
        ExponentialBackOffWithMaxRetries backOff = new ExponentialBackOffWithMaxRetries(3);
        backOff.setInitialInterval(1_000L);
        backOff.setMultiplier(2.0);
        backOff.setMaxInterval(10_000L);
        DefaultErrorHandler handler = new DefaultErrorHandler(deadLetterPublishingRecoverer(), backOff);
        handler.addNotRetryableExceptions(IllegalArgumentException.class);
        return handler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CommonModel> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, CommonModel> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.setCommonErrorHandler(kafkaErrorHandler());
        return factory;
    }
}
