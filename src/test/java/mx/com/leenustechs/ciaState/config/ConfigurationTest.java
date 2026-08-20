package mx.com.leenustechs.ciaState.config;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;

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
        assertEquals(
                java.util.List.of("application/json"),
                template.headers().get("Content-Type"));
        assertEquals(
                java.util.List.of("application/json"),
                template.headers().get("Accept"));
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
    void buildsFirebaseRealtimeDatabaseOptions() {
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        FirebaseOptions options = new FirebaseConfig().firebaseOptions(
                credentials,
                "https://cia-state-default-rtdb.firebaseio.com/");

        assertEquals(
                "https://cia-state-default-rtdb.firebaseio.com/",
                options.getDatabaseUrl());

        FirebaseApp app = new FirebaseConfig().firebaseApp(options);
        try {
            assertEquals("cia-state", app.getName());
            assertNotNull(new FirebaseConfig().firebaseDatabase(app));
            assertSameFirebaseApp(app, new FirebaseConfig().firebaseApp(options));
        } finally {
            app.delete();
        }
    }

    private void assertSameFirebaseApp(FirebaseApp expected, FirebaseApp actual) {
        assertEquals(expected.getName(), actual.getName());
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
