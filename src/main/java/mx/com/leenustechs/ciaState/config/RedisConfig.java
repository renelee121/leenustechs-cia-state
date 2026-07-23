package mx.com.leenustechs.ciaState.config;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisStandaloneConfiguration;
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory;

import io.lettuce.core.cluster.ClusterClientOptions;
import io.lettuce.core.cluster.ClusterTopologyRefreshOptions;
import io.lettuce.core.resource.DefaultClientResources;
import lombok.extern.slf4j.Slf4j;

import org.springframework.data.redis.connection.lettuce.LettucePoolingClientConfiguration;
import org.springframework.data.redis.core.RedisTemplate;

import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;

import io.lettuce.core.api.StatefulConnection;

@SuppressWarnings("removal")
@Slf4j
@EnableCaching
@Configuration
public class RedisConfig {
    private final String redisHost;
    private final int redisPort;
    private final String redisUser;
    private final String redisPassword;

    public RedisConfig(
            @Value("${spring.redis.host}") String redisHost,
            @Value("${spring.redis.port}") int redisPort,
            @Value("${spring.redis.user}") String redisUser,
            @Value("${spring.redis.password}") String redisPassword) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisUser = redisUser;
        this.redisPassword = redisPassword;
    }

    @Bean
    public LettuceConnectionFactory redisConnectionFactory() {
        RedisStandaloneConfiguration redisConfig = new RedisStandaloneConfiguration(redisHost, redisPort);
        redisConfig.setUsername(redisUser);
        redisConfig.setPassword(redisPassword);
        GenericObjectPoolConfig<StatefulConnection<?, ?>> poolConfig = new GenericObjectPoolConfig<>();
        poolConfig.setMaxTotal(100);
        poolConfig.setMaxIdle(50);
        poolConfig.setMinIdle(10);
        poolConfig.setTestOnBorrow(true);
        poolConfig.setTestOnReturn(true);
        ClusterClientOptions clusterClientOptions = ClusterClientOptions.builder()
                .topologyRefreshOptions(ClusterTopologyRefreshOptions.builder()
                        .enablePeriodicRefresh(true)
                        .build())
                .build();
        LettucePoolingClientConfiguration clientConfig = LettucePoolingClientConfiguration.builder()
                .clientResources(DefaultClientResources.create())
                .clientOptions(clusterClientOptions)
                .poolConfig(poolConfig)
                .useSsl()
                .disablePeerVerification()
                .build();
        return new LettuceConnectionFactory(redisConfig, clientConfig);
    }

    @Bean
    public RedisTemplate<String, Object> redisTemplate(LettuceConnectionFactory redisConnectionFactory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(redisConnectionFactory);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new GenericJackson2JsonRedisSerializer());
        try{
            log.info("tratando de conectar a redis con host: {} y puerto: {}", redisHost, redisPort);
            redisConnectionFactory.getConnection().ping();
            log.info("Conexión a Redis establecida correctamente.");
        }catch(Exception e){
            log.error("Error al conectar a Redis: {}", e.getMessage(), e);
            log.error("Causa raíz: ", e.getCause());
        }
        return template;
    }
}
