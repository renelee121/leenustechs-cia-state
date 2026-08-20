package mx.com.leenustechs.ciaState.config;

import java.io.IOException;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.database.FirebaseDatabase;

@Configuration
public class FirebaseConfig {

    private static final String APP_NAME = "cia-state";

    @Bean
    public GoogleCredentials firebaseCredentials() throws IOException {
        return GoogleCredentials.getApplicationDefault();
    }

    @Bean
    public FirebaseOptions firebaseOptions(
            GoogleCredentials firebaseCredentials,
            @Value("${firebase.database-url}") String databaseUrl) {

        return FirebaseOptions.builder()
                .setCredentials(firebaseCredentials)
                .setDatabaseUrl(databaseUrl)
                .build();
    }

    @Bean(destroyMethod = "delete")
    public FirebaseApp firebaseApp(FirebaseOptions firebaseOptions) {
        return FirebaseApp.getApps().stream()
                .filter(app -> APP_NAME.equals(app.getName()))
                .findFirst()
                .orElseGet(() ->
                        FirebaseApp.initializeApp(firebaseOptions, APP_NAME));
    }

    @Bean
    public FirebaseDatabase firebaseDatabase(FirebaseApp firebaseApp) {
        return FirebaseDatabase.getInstance(firebaseApp);
    }
}
