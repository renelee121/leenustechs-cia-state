package mx.com.leenustechs.ciaState.config;

import java.time.Clock;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class WorkflowRuntimeConfig {
    @Bean
    public Clock workflowClock() {
        return Clock.systemUTC();
    }
}
