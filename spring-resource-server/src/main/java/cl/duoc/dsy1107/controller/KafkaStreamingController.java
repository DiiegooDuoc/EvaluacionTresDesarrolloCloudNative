package cl.duoc.dsy1107.controller;

import cl.duoc.dsy1107.dto.InventarioStreamEventDTO;
import cl.duoc.dsy1107.dto.VentaStreamEventDTO;
import cl.duoc.dsy1107.messaging.kafka.producer.KafkaEventPublisher;
import cl.duoc.dsy1107.service.StreamingAnalyticsService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/kafka")
public class KafkaStreamingController {

    private final KafkaEventPublisher kafkaPublisher;
    private final StreamingAnalyticsService analyticsService;

    public KafkaStreamingController(KafkaEventPublisher kafkaPublisher,
                                    StreamingAnalyticsService analyticsService) {
        this.kafkaPublisher = kafkaPublisher;
        this.analyticsService = analyticsService;
    }

    @GetMapping("/analytics")
    public ResponseEntity<Map<String, Object>> obtenerDashboardStreaming() {
        return ResponseEntity.ok(analyticsService.obtenerDashboardAnalitica());
    }

    @PostMapping("/eventos/ventas")
    public ResponseEntity<VentaStreamEventDTO> publicarVenta(@Valid @RequestBody VentaStreamEventDTO dto) {
        return ResponseEntity.accepted().body(kafkaPublisher.publicarEventoVenta(dto));
    }

    @PostMapping("/eventos/inventario")
    public ResponseEntity<InventarioStreamEventDTO> publicarInventario(@Valid @RequestBody InventarioStreamEventDTO dto) {
        return ResponseEntity.accepted().body(kafkaPublisher.publicarEventoInventario(dto));
    }

    // Genera una rafaga de eventos variados para observar balanceo entre las 3 particiones
    @PostMapping("/eventos/rafaga-prueba")
    public ResponseEntity<Map<String, Object>> publicarRafagaDeStreaming() {
        String[] categorias = {"COMPUTACION", "PERIFERICOS", "MONITORES", "REDES", "ALMACENAMIENTO", "GAMING"};
        String[] productos = {"Laptop Gamer RTX 4070", "Mouse Inalámbrico Pro", "Monitor Curvo 27\"", "Router WiFi 6E", "SSD NVMe 2TB", "Teclado Mecánico RGB"};
        double[] precios = {1250000.0, 24990.0, 219990.0, 149990.0, 139990.0, 65990.0};
        String[] docs = {"FACTURA", "BOLETA", "FACTURA", "BOLETA", "GUIA_DESPACHO", "BOLETA"};

        List<String> idsGenerados = new ArrayList<>();
        for (int i = 0; i < categorias.length; i++) {
            VentaStreamEventDTO vta = kafkaPublisher.publicarEventoVenta(VentaStreamEventDTO.builder()
                    .clienteEmail("cliente" + (i + 1) + "@duocuc.cl")
                    .productoId((long) ((i % 4) + 1))
                    .productoNombre(productos[i])
                    .categoria(categorias[i])
                    .cantidad(1 + (i % 2))
                    .montoTotal(precios[i] * (1 + (i % 2)))
                    .tipoDocumento(docs[i])
                    .region(i % 2 == 0 ? "RM-SANTIAGO" : "VALPARAISO")
                    .simularErrorProcesamiento(false)
                    .build());

            int stock = analyticsService.calcularNuevoStock((long) ((i % 4) + 1), "RESERVA_PEDIDO", 2);
            InventarioStreamEventDTO inv = kafkaPublisher.publicarEventoInventario(InventarioStreamEventDTO.builder()
                    .productoId((long) ((i % 4) + 1))
                    .productoNombre(productos[i])
                    .tipoMovimiento("RESERVA_PEDIDO")
                    .unidades(2)
                    .stockDisponible(stock)
                    .bodega("BODEGA-CENTRAL-SCL")
                    .estadoOperacional(stock <= 8 ? "STOCK_CRITICO" : "STOCK_NORMAL")
                    .simularErrorProcesamiento(false)
                    .build());

            idsGenerados.add(vta.getEventId());
            idsGenerados.add(inv.getEventId());
        }

        return ResponseEntity.accepted().body(Map.of(
                "eventosPublicados", idsGenerados.size(),
                "eventIds", idsGenerados,
                "mensaje", "Ráfaga de 12 eventos enviada a los 2 tópicos de Kafka (3 particiones cada uno)"
        ));
    }
}
