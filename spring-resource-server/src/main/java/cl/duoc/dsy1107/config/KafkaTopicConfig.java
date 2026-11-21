package cl.duoc.dsy1107.config;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaTopicConfig {

    private final KafkaStreamingProperties kafkaProperties;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    public KafkaTopicConfig(KafkaStreamingProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public KafkaAdmin kafkaAdmin() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 8000);
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        KafkaAdmin admin = new KafkaAdmin(configs);
        admin.setFatalIfBrokerNotAvailable(false);
        return admin;
    }

    @Bean(destroyMethod = "close")
    public AdminClient kafkaAdminClient() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configs.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, 8000);
        configs.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10000);
        return AdminClient.create(configs);
    }

    // Topico 1: Analitica de ventas en tiempo real (3 particiones, 3 replicas)
    @Bean
    public NewTopic ventasStreamingTopic() {
        KafkaStreamingProperties.TopicSettings cfg = kafkaProperties.getTopics().getVentas();
        return TopicBuilder.name(cfg.getName())
                .partitions(cfg.getPartitions())
                .replicas(cfg.getReplicas())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(cfg.getRetentionMs()))
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, cfg.getCleanupPolicy())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(cfg.getMinInsyncReplicas()))
                .build();
    }

    // Topico 2: Estado operacional e inventario (3 particiones, 3 replicas)
    @Bean
    public NewTopic inventarioStreamingTopic() {
        KafkaStreamingProperties.TopicSettings cfg = kafkaProperties.getTopics().getInventario();
        return TopicBuilder.name(cfg.getName())
                .partitions(cfg.getPartitions())
                .replicas(cfg.getReplicas())
                .config(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(cfg.getRetentionMs()))
                .config(TopicConfig.CLEANUP_POLICY_CONFIG, cfg.getCleanupPolicy())
                .config(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, String.valueOf(cfg.getMinInsyncReplicas()))
                .build();
    }
}
