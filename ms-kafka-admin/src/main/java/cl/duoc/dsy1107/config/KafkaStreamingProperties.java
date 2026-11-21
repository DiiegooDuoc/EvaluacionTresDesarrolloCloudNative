package cl.duoc.dsy1107.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

@Data
@ConfigurationProperties(prefix = "app.kafka")
public class KafkaStreamingProperties {

    private int maxRetries = 2;
    private long backoffMs = 1500L;
    private ConsumerGroups consumerGroups = new ConsumerGroups();
    private Topics topics = new Topics();

    @Data
    public static class ConsumerGroups {
        private String analitica;
        private String alertas;
        private String operaciones;
    }

    @Data
    public static class Topics {
        private TopicSettings ventas = new TopicSettings();
        private TopicSettings inventario = new TopicSettings();
    }

    @Data
    public static class TopicSettings {
        private String name;
        private int partitions = 3;
        private int replicas = 3;
        private long retentionMs = 604800000L;
        private String cleanupPolicy = "delete";
        private int minInsyncReplicas = 2;
    }
}
