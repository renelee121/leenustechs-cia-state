package mx.com.leenustechs.ciaState.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Test;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.core.RedisTemplate;

import feign.RequestTemplate;
import mx.com.leenustechs.ciaState.business.adapters.in.ApiInterceptor;
import tools.jackson.databind.json.JsonMapper;

class ConfigurationTest {

    @Test
    void configuresJacksonAndFeignHeaders() {
        JsonMapper mapper = new JacksonConfig().objectMapper();
        RequestTemplate template = new RequestTemplate();

        new FeingConfig().requestInterceptor().apply(template);

        assertNotNull(mapper);
        assertEquals(java.util.List.of("application/json"), template.headers().get("Content-Type"));
        assertEquals(java.util.List.of("application/json"), template.headers().get("Accept"));
    }

    @Test
    void buildsKafkaFactoriesAndSecurityConfiguration() {
        KafkaConfig config = new KafkaConfig("user", "secret", "broker:9092", "group");

        assertEquals(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"user\" password=\"secret\";",
                config.getSaslJaasConfig());
        assertNotNull(config.producerFactory());
        assertNotNull(config.kafkaTemplate());
        assertNotNull(config.consumerFactory());
        assertNotNull(config.kafkaListenerContainerFactory());
    }

    @Test
    void buildsRedisConnectionAndTemplate() {
        JsonMapper mapper = new JacksonConfig().objectMapper();
        RedisConfig config = new RedisConfig("localhost", 6379, "user", "secret", mapper);
        LettuceConnectionFactory factory = config.redisConnectionFactory();
        RedisTemplate<String, Object> template = config.redisTemplate(factory);

        assertSame(factory, template.getConnectionFactory());
        assertNotNull(template.getKeySerializer());
        assertNotNull(template.getValueSerializer());
    }

    @Test
    void registersApiInterceptor() {
        ApiInterceptor interceptor = new ApiInterceptor();
        InterceptorConfig config = new InterceptorConfig(interceptor);
        org.springframework.web.servlet.config.annotation.InterceptorRegistry registry =
                new org.springframework.web.servlet.config.annotation.InterceptorRegistry();

        config.addInterceptors(registry);

        assertNotNull(config);
    }
}
