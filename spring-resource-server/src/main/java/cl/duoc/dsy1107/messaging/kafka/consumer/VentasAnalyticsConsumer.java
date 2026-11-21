package cl.duoc.dsy1107.messaging.kafka.consumer;

import cl.duoc.dsy1107.config.KafkaStreamingProperties;
import cl.duoc.dsy1107.dto.VentaStreamEventDTO;
import cl.duoc.dsy1107.service.StreamingAnalyticsService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class VentasAnalyticsConsumer {

    private final StreamingAnalyticsService analyticsService;
    private final KafkaStreamingProperties properties;

    public VentasAnalyticsConsumer(StreamingAnalyticsService analyticsService,
                                   KafkaStreamingProperties properties) {
        this.analyticsService = analyticsService;
        this.properties = properties;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.ventas.name}",
            groupId = "${app.kafka.consumer-groups.analitica}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "3"
    )
    public void consumirVentaParaAnalitica(
            @Payload VentaStreamEventDTO event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String groupId = properties.getConsumerGroups().getAnalitica();
        log.info("[KAFKA-CONSUMER][{}] Procesando evento={} desde topico='{}' [particion={}, offset={}]",
                groupId, event.getEventId(), topic, partition, offset);

        if (event.isSimularErrorProcesamiento() || event.getMontoTotal() == null || event.getMontoTotal() <= 0) {
            analyticsService.registrarErrorProcesamiento(
                    event.getEventId(), topic, groupId, partition, offset,
                    "Evento de venta defectuoso o con falla simulada; activando política de reintentos/descarte"
            );
            throw new RuntimeException("Fallo simulado o payload inválido en procesamiento de analítica de ventas");
        }

        analyticsService.registrarMetricaVenta(event, topic, partition, offset, groupId);
    }
}
