package cl.duoc.dsy1107.messaging.kafka.producer;

import cl.duoc.dsy1107.config.KafkaStreamingProperties;
import cl.duoc.dsy1107.dto.InventarioStreamEventDTO;
import cl.duoc.dsy1107.dto.VentaStreamEventDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Component
public class KafkaEventPublisherImpl implements KafkaEventPublisher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaStreamingProperties kafkaProperties;

    public KafkaEventPublisherImpl(KafkaTemplate<String, Object> kafkaTemplate,
                                   KafkaStreamingProperties kafkaProperties) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProperties = kafkaProperties;
    }

    @Override
    public VentaStreamEventDTO publicarEventoVenta(VentaStreamEventDTO event) {
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId("KFK-VTA-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        }
        event.setTimestamp(LocalDateTime.now());

        String topic = kafkaProperties.getTopics().getVentas().getName();
        String partitionKey = event.getCategoria() != null && !event.getCategoria().isBlank()
                ? event.getCategoria().trim().toUpperCase() + "-" + event.getEventId().substring(event.getEventId().length() - 2)
                : event.getEventId();

        kafkaTemplate.send(topic, partitionKey, event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[KAFKA-PRODUCER][VENTAS] Error publicando evento {} en topico '{}': {}",
                        event.getEventId(), topic, ex.getMessage());
            } else {
                log.info("[KAFKA-PRODUCER][VENTAS] Evento {} escrito en topico='{}', particion={}, offset={}",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return event;
    }

    @Override
    public InventarioStreamEventDTO publicarEventoInventario(InventarioStreamEventDTO event) {
        if (event.getEventId() == null || event.getEventId().isBlank()) {
            event.setEventId("KFK-INV-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase());
        }
        event.setTimestamp(LocalDateTime.now());

        String topic = kafkaProperties.getTopics().getInventario().getName();
        // Clave por producto para garantizar orden y compactacion de estado en Kafka
        String partitionKey = "PROD-" + event.getProductoId();

        kafkaTemplate.send(topic, partitionKey, event).whenComplete((result, ex) -> {
            if (ex != null) {
                log.error("[KAFKA-PRODUCER][INVENTARIO] Error publicando evento {} en topico '{}': {}",
                        event.getEventId(), topic, ex.getMessage());
            } else {
                log.info("[KAFKA-PRODUCER][INVENTARIO] Evento {} escrito en topico='{}', particion={}, offset={}",
                        event.getEventId(),
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            }
        });

        return event;
    }
}
