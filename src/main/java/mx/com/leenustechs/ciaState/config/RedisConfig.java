package mx.com.leenustechs.ciaState.config;

import java.time.Duration;
import java.util.List;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisClusterConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;
import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.GenericJacksonJsonRedisSerializer;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import io.lettuce.core.api.StatefulConnection;
import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import tools.jackson.databind.ObjectMapper;

@EnableCaching
@Configuration
public class RedisConfig {

    private final String redisHost;
    private final int redisPort;
    private final String redisUser;
    private final String redisPassword;
    private final ObjectMapper objectMapper;

    public RedisConfig(
            @Value("${spring.redis.host}") String redisHost,
            @Value("${spring.redis.port}") int redisPort,
            @Value("${spring.redis.user}") String redisUser,
            @Value("${spring.redis.password}") String redisPassword,
            ObjectMapper objectMapper) {

        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisUser = redisUser;
        this.redisPassword = redisPassword;
        this.objectMapper = objectMapper;
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {

        RedisClusterConfiguration redisConfig =
                new RedisClusterConfiguration(
                        List.of(redisHost + ":" + redisPort)
                );

        redisConfig.setUsername(redisUser);
        redisConfig.setPassword(redisPassword);

        GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig =
                new GenericObjectPoolConfig<>();

        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(50);
        poolConfig.setMinIdle(10);

        ClusterTopologyRefreshOptions topologyRefreshOptions =
                ClusterTopologyRefreshOptions.builder()
                        .enablePeriodicRefresh(Duration.ofSeconds(30))
                        .adaptiveRefreshTriggersTimeout(Duration.ofSeconds(10))
                        .build();

        ClusterClientOptions clientOptions =
                ClusterClientOptions.builder()
                        .topologyRefreshOptions(topologyRefreshOptions)
                        .build();

        LettucePoolingClientConfiguration clientConfig =
                LettucePoolingClientConfiguration.builder()
                        .clientOptions(clientOptions)
                        .poolConfig(poolConfig)
                        .useSsl()
                        .build();

        return new LettuceConnectionFactory(
                redisConfig,
                clientConfig
        );
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(
            LettuceConnectionFactory connectionFactory) {

        RedisTemplate<String, Object> template =
                new RedisTemplate<>();

        template.setConnectionFactory(connectionFactory);

        StringRedisSerializer stringSerializer =
                new StringRedisSerializer();

        GenericJacksonJsonRedisSerializer jsonSerializer =
                new GenericJacksonJsonRedisSerializer(objectMapper);

        template.setKeySerializer(stringSerializer);
        template.setHashKeySerializer(stringSerializer);

        template.setValueSerializer(jsonSerializer);
        template.setHashValueSerializer(jsonSerializer);

        return template;
    }
}