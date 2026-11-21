package cl.duoc.dsy1107.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
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
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
public class KafkaClientConfig {

    private final KafkaStreamingProperties streamingProperties;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaClientConfig(KafkaStreamingProperties streamingProperties) {
        this.streamingProperties = streamingProperties;
    }

    @Bean
    public ObjectMapper kafkaObjectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
        return mapper;
    }

    @Bean
    public ProducerFactory<String, Object> kafkaProducerFactory(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 5);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 5);

        JsonSerializer<Object> valueSerializer = new JsonSerializer<>(kafkaObjectMapper);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), valueSerializer);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String, Object> kafkaProducerFactory) {
        return new KafkaTemplate<>(kafkaProducerFactory);
    }

    @Bean
    public ConsumerFactory<String, Object> kafkaConsumerFactory(ObjectMapper kafkaObjectMapper) {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 15000);
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(kafkaObjectMapper);
        jsonDeserializer.addTrustedPackages("cl.duoc.dsy1107.dto", "java.util", "java.lang");
        jsonDeserializer.setUseTypeHeaders(true);

        ErrorHandlingDeserializer<Object> safeValueDeserializer = new ErrorHandlingDeserializer<>(jsonDeserializer);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), safeValueDeserializer);
    }

    // Manejo de reintentos y descarte controlado de eventos defectuosos
    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        FixedBackOff backOff = new FixedBackOff(
                streamingProperties.getBackoffMs(),
                streamingProperties.getMaxRetries()
        );
        DefaultErrorHandler errorHandler = new DefaultErrorHandler((record, exception) -> {
            log.error("[KAFKA-ERROR-HANDLER] Evento descartado tras {} reintentos en topico='{}', particion={}, offset={}, key='{}'. Causa: {}",
                    streamingProperties.getMaxRetries(),
                    record.topic(),
                    record.partition(),
                    record.offset(),
                    record.key(),
                    exception.getMessage());
        }, backOff);

        errorHandler.addNotRetryableExceptions(IllegalArgumentException.class);
        return errorHandler;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> kafkaConsumerFactory,
            CommonErrorHandler kafkaErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(kafkaConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        factory.getContainerProperties().setMissingTopicsFatal(false);
        factory.setCommonErrorHandler(kafkaErrorHandler);
        return factory;
    }
}
