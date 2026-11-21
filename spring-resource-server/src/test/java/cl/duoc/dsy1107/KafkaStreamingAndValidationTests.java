package cl.duoc.dsy1107;

import cl.duoc.dsy1107.config.KafkaStreamingProperties;
import cl.duoc.dsy1107.config.KafkaTopicConfig;
import cl.duoc.dsy1107.dto.CreateKafkaTopicRequestDTO;
import cl.duoc.dsy1107.dto.InventarioStreamEventDTO;
import cl.duoc.dsy1107.dto.VentaStreamEventDTO;
import cl.duoc.dsy1107.service.StreamingAnalyticsService;
import jakarta.validation.Validation;
import jakarta.validation.Validator;
import jakarta.validation.ValidatorFactory;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class KafkaStreamingAndValidationTests {

    private KafkaStreamingProperties properties;
    private KafkaTopicConfig topicConfig;
    private Validator validator;

    @BeforeEach
    void setUp() {
        properties = new KafkaStreamingProperties();
        properties.getTopics().getVentas().setName("pedidos.streaming.ventas");
        properties.getTopics().getVentas().setPartitions(3);
        properties.getTopics().getVentas().setReplicas(3);
        properties.getTopics().getVentas().setRetentionMs(604800000L);
        properties.getTopics().getVentas().setCleanupPolicy("delete");
        properties.getTopics().getVentas().setMinInsyncReplicas(2);

        properties.getTopics().getInventario().setName("pedidos.streaming.inventario");
        properties.getTopics().getInventario().setPartitions(3);
        properties.getTopics().getInventario().setReplicas(3);
        properties.getTopics().getInventario().setRetentionMs(259200000L);
        properties.getTopics().getInventario().setCleanupPolicy("compact,delete");
        properties.getTopics().getInventario().setMinInsyncReplicas(2);

        topicConfig = new KafkaTopicConfig(properties);

        ValidatorFactory factory = Validation.buildDefaultValidatorFactory();
        validator = factory.getValidator();
    }

    @Test
    void debeDeclararDosTopicosConTresParticionesYTresReplicasCadaUno() {
        NewTopic ventasTopic = topicConfig.ventasStreamingTopic();
        NewTopic invTopic = topicConfig.inventarioStreamingTopic();

        assertEquals("pedidos.streaming.ventas", ventasTopic.name());
        assertEquals(3, ventasTopic.numPartitions());
        assertEquals((short) 3, ventasTopic.replicationFactor());
        assertEquals("delete", ventasTopic.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));

        assertEquals("pedidos.streaming.inventario", invTopic.name());
        assertEquals(3, invTopic.numPartitions());
        assertEquals((short) 3, invTopic.replicationFactor());
        assertEquals("compact,delete", invTopic.configs().get(TopicConfig.CLEANUP_POLICY_CONFIG));
    }

    @Test
    void debeValidarParametrosDeCreacionDeTopicoKafka() {
        CreateKafkaTopicRequestDTO invalidDto = new CreateKafkaTopicRequestDTO("", 0, (short) 5, 100L, "invalido");
        assertFalse(validator.validate(invalidDto).isEmpty());

        CreateKafkaTopicRequestDTO validDto = new CreateKafkaTopicRequestDTO(
                "pedidos.telemetria.log", 3, (short) 3, 604800000L, "delete"
        );
        assertTrue(validator.validate(validDto).isEmpty());
    }

    @Test
    void debeActualizarMetricasYGenerarAlertasReactivasEnTiempoReal() {
        StreamingAnalyticsService service = new StreamingAnalyticsService();

        VentaStreamEventDTO ventaVip = VentaStreamEventDTO.builder()
                .eventId("KFK-VTA-01")
                .clienteEmail("vip@duocuc.cl")
                .productoId(1L)
                .productoNombre("Laptop Gamer RTX 4070")
                .categoria("COMPUTACION")
                .cantidad(1)
                .montoTotal(1250000.0)
                .tipoDocumento("FACTURA")
                .build();

        service.registrarMetricaVenta(ventaVip, "pedidos.streaming.ventas", 0, 1L, "cg-analitica-ventas");
        service.evaluarReglasAlertaVenta(ventaVip, "pedidos.streaming.ventas", 0, 1L, "cg-detector-alertas");

        InventarioStreamEventDTO stockCritico = InventarioStreamEventDTO.builder()
                .eventId("KFK-INV-01")
                .productoId(1L)
                .productoNombre("Laptop Gamer RTX 4070")
                .tipoMovimiento("RESERVA_PEDIDO")
                .unidades(2)
                .stockDisponible(4)
                .estadoOperacional("STOCK_CRITICO")
                .build();

        service.actualizarEstadoInventario(stockCritico, "pedidos.streaming.inventario", 1, 2L, "cg-monitoreo-inventario");

        Map<String, Object> dashboard = service.obtenerDashboardAnalitica();
        assertEquals(1250000.0, dashboard.get("ingresosTotales"));
        assertEquals(1, dashboard.get("totalEventosVentas"));

        List<?> alertas = (List<?>) dashboard.get("alertasReactivas");
        assertEquals(2, alertas.size());
    }
}
