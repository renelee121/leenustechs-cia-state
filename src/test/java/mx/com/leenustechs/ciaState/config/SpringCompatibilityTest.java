package mx.com.leenustechs.ciaState.config;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.cloud.configuration.CompatibilityVerifierAutoConfiguration;

/** Uses the resolved dependencies and the real Spring Cloud verifier. */
class SpringCompatibilityTest {
    private final ApplicationContextRunner context = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CompatibilityVerifierAutoConfiguration.class));

    @Test
    void resolvedBootAndCloudVersionsPassTheDefaultCompatibilityCheck() {
        context.run(application -> {
            assertThat(application).hasNotFailed();
            assertThat(application).hasBean("compositeCompatibilityVerifier");
        });
    }

    @Test
    void incompatibleVersionsStillPreventContextInitialization() {
        context.withPropertyValues("spring.cloud.compatibility-verifier.compatible-boot-versions=99.0.x")
                .run(application -> {
                    assertThat(application).hasFailed();
                    Throwable rootCause = application.getStartupFailure();
                    while (rootCause.getCause() != null) rootCause = rootCause.getCause();
                    assertThat(rootCause.getClass().getName()).isEqualTo(
                            "org.springframework.cloud.configuration.CompatibilityNotMetException");
                });
    }
}
