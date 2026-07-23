package mx.com.leenustechs.ciaState.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
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
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;

import lombok.extern.slf4j.Slf4j;
import mx.com.leenustechs.ciaState.business.utils.commons.CustomDeserializer;
import mx.com.leenustechs.ciaState.business.utils.commons.CustomSerializer;

@Slf4j
@EnableKafka
@Configuration
public class KafkaConfig {

    private static final String SECURITY_PROTOCOL = "SASL_SSL";
    private static final String SASL_MECHANISM = "SCRAM-SHA-512";
    public static final String SASL_JAAS_CONFIG = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{0}\" password=\"{1}\";";

    private final String kafkaUsername;
    private final String kafkaPassword;
    private final String kafkaBootstrapServers;
    private final String kafkaGroupId;

    public KafkaConfig(
        @Value("${kafka.username}") String kafkaUsername,
        @Value("${kafka.password}") String kafkaPassword,
        @Value("${kafka.bootstrap.servers}") String kafkaBootstrapServers,
        @Value("${kafka.group.id}") String kafkaGroupId
    ){
        this.kafkaUsername = kafkaUsername;
        this.kafkaPassword = kafkaPassword;
        this.kafkaBootstrapServers = kafkaBootstrapServers;
        this.kafkaGroupId = kafkaGroupId;
    }
    
    public String getSaslJaasConfig() {
        String jaasConfig = String.format(SASL_JAAS_CONFIG, kafkaUsername, kafkaPassword);
        log.info("SASL JAAS Config: {}", jaasConfig);
        return jaasConfig;
    }

    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, CustomSerializer.class);
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public ConsumerFactory<String, Object> consumerFactory(){
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBootstrapServers);
        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaGroupId);
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.KEY_DESERIALIZER_CLASS, StringDeserializer.class);
        configProps.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, CustomDeserializer.class);
        configProps.put("security.protocol", SECURITY_PROTOCOL);
        configProps.put("sasl.mechanism", SASL_MECHANISM);
        configProps.put("sasl.jaas.config", getSaslJaasConfig());
        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        factory.setRecordFilterStrategy(record -> {
            Object event = record.value();
            return event == null || (event instanceof String && ((String) event).isEmpty());
        });
        DefaultErrorHandler errorHandler = new DefaultErrorHandler();
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) -> {
            log.error("Error processing record from topic {}: {}", record.topic(), ex.getMessage(), ex);
        });
        return factory;
    }
}
