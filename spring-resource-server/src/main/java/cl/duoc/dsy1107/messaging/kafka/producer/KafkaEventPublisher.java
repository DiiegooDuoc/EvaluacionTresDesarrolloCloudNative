package cl.duoc.dsy1107.messaging.kafka.producer;

import cl.duoc.dsy1107.dto.InventarioStreamEventDTO;
import cl.duoc.dsy1107.dto.VentaStreamEventDTO;

public interface KafkaEventPublisher {
    VentaStreamEventDTO publicarEventoVenta(VentaStreamEventDTO event);
    InventarioStreamEventDTO publicarEventoInventario(InventarioStreamEventDTO event);
}
