package cl.duoc.dsy1107.messaging.kafka.consumer;

import cl.duoc.dsy1107.config.KafkaStreamingProperties;
import cl.duoc.dsy1107.dto.InventarioStreamEventDTO;
import cl.duoc.dsy1107.service.StreamingAnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class InventarioOperacionesConsumer {

    private final StreamingAnalyticsService analyticsService;
    private final KafkaStreamingProperties properties;

    public InventarioOperacionesConsumer(StreamingAnalyticsService analyticsService,
                                         KafkaStreamingProperties properties) {
        this.analyticsService = analyticsService;
        this.properties = properties;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.inventario.name}",
            groupId = "${app.kafka.consumer-groups.operaciones}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "3"
    )
    public void procesarEventoInventario(
            @Payload InventarioStreamEventDTO event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String groupId = properties.getConsumerGroups().getOperaciones();
        log.info("[KAFKA-CONSUMER][{}] Recibido evento inventario={} prodId={} [particion={}, offset={}]",
                groupId, event.getEventId(), event.getProductoId(), partition, offset);

        if (event.isSimularErrorProcesamiento() || event.getUnidades() == null || event.getUnidades() <= 0) {
            analyticsService.registrarErrorProcesamiento(
                    event.getEventId(), topic, groupId, partition, offset,
                    "Evento de inventario inválido; reintentando según política de backoff"
            );
            throw new RuntimeException("Fallo simulado en procesamiento de evento de inventario");
        }

        analyticsService.actualizarEstadoInventario(event, topic, partition, offset, groupId);
    }
}
