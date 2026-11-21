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
public class AlertasVentasConsumer {

    private final StreamingAnalyticsService analyticsService;
    private final KafkaStreamingProperties properties;

    public AlertasVentasConsumer(StreamingAnalyticsService analyticsService,
                                 KafkaStreamingProperties properties) {
        this.analyticsService = analyticsService;
        this.properties = properties;
    }

    @KafkaListener(
            topics = "${app.kafka.topics.ventas.name}",
            groupId = "${app.kafka.consumer-groups.alertas}",
            containerFactory = "kafkaListenerContainerFactory",
            concurrency = "2"
    )
    public void monitorearAlertasDeVentas(
            @Payload VentaStreamEventDTO event,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset) {

        String groupId = properties.getConsumerGroups().getAlertas();
        if (event.isSimularErrorProcesamiento()) {
            log.warn("[KAFKA-CONSUMER][{}] Evento {} marcado con error; omitido en motor de alertas",
                    groupId, event.getEventId());
            return;
        }

        analyticsService.evaluarReglasAlertaVenta(event, topic, partition, offset, groupId);
    }
}
