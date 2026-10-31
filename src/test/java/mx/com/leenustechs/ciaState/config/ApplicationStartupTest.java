package mx.com.leenustechs.ciaState.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.database.FirebaseDatabase;
import io.micrometer.core.instrument.MeterRegistry;
import mx.com.leenustechs.App;
import mx.com.leenustechs.ciaState.business.services.CommandDispatcherService;
import mx.com.leenustechs.ciaState.business.services.EventStateService;
import org.junit.jupiter.api.Test;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/** Real application wiring; only external Firebase objects are replaced, not auto-configuration. */
class ApplicationStartupTest {
    @Test
    void applicationContextStartsWithCompatibleDependenciesAndStoppedExternalConsumers() {
        GoogleCredentials credentials = mock(GoogleCredentials.class);
        FirebaseApp firebaseApp = mock(FirebaseApp.class);
        FirebaseDatabase database = mock(FirebaseDatabase.class);
        var application = new SpringApplicationBuilder(App.class)
                .web(WebApplicationType.NONE)
                .initializers(context -> context.addBeanFactoryPostProcessor(factory -> {
                    factory.registerSingleton("firebaseCredentials", credentials);
                    factory.registerSingleton("firebaseApp", firebaseApp);
                    factory.registerSingleton("firebaseDatabase", database);
                }));
        try (var context = application.run(
                "--firebase.database-url=https://demo-cia-state.firebaseio.com",
                "--kafka.username=startup-test",
                "--kafka.password=startup-test",
                "--kafka.bootstrap.servers=127.0.0.1:1",
                "--kafka.group.id=startup-test",
                "--spring.kafka.listener.auto-startup=false",
                "--outbox.enabled=false",
                "--workflows.reconciliation.enabled=false")) {
            assertThat(context.isActive()).isTrue();
            assertThat(context.containsBean("compositeCompatibilityVerifier")).isTrue();
            assertThat(context.getBean(CommandDispatcherService.class)).isNotNull();
            assertThat(context.getBean(EventStateService.class)).isNotNull();
            assertThat(context.getBean(KafkaTemplate.class)).isNotNull();
            assertThat(context.getBean(MeterRegistry.class)).isNotNull();
            assertThat(context.getBean(ThreadPoolTaskScheduler.class).getScheduledThreadPoolExecutor().getCorePoolSize()).isGreaterThanOrEqualTo(3);
            var registry = context.getBean(KafkaListenerEndpointRegistry.class);
            assertThat(registry.getListenerContainers()).hasSize(4)
                    .allSatisfy(container -> assertThat(container.isRunning()).isFalse());
            verifyNoInteractions(database);
        }
    }
}
