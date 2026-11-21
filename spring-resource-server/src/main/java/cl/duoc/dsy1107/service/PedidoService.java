package cl.duoc.dsy1107.service;

import cl.duoc.dsy1107.dto.CrearPedidoRequestDTO;
import cl.duoc.dsy1107.dto.DocumentoEventDTO;
import cl.duoc.dsy1107.dto.InventarioStreamEventDTO;
import cl.duoc.dsy1107.dto.NotificacionEventDTO;
import cl.duoc.dsy1107.dto.TicketEventDTO;
import cl.duoc.dsy1107.dto.VentaStreamEventDTO;
import cl.duoc.dsy1107.entity.EventoMensajeLog;
import cl.duoc.dsy1107.entity.Pedido;
import cl.duoc.dsy1107.entity.Producto;
import cl.duoc.dsy1107.messaging.kafka.producer.KafkaEventPublisher;
import cl.duoc.dsy1107.messaging.producer.EventMessagePublisher;
import cl.duoc.dsy1107.repository.EventoMensajeLogRepository;
import cl.duoc.dsy1107.repository.PedidoRepository;
import cl.duoc.dsy1107.repository.ProductoRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class PedidoService {

    private final PedidoRepository pedidoRepository;
    private final ProductoRepository productoRepository;
    private final EventoMensajeLogRepository eventoLogRepository;
    private final EventMessagePublisher rabbitPublisher;
    private final KafkaEventPublisher kafkaPublisher;
    private final StreamingAnalyticsService analyticsService;

    public PedidoService(PedidoRepository pedidoRepository,
                         ProductoRepository productoRepository,
                         EventoMensajeLogRepository eventoLogRepository,
                         EventMessagePublisher rabbitPublisher,
                         KafkaEventPublisher kafkaPublisher,
                         StreamingAnalyticsService analyticsService) {
        this.pedidoRepository = pedidoRepository;
        this.productoRepository = productoRepository;
        this.eventoLogRepository = eventoLogRepository;
        this.rabbitPublisher = rabbitPublisher;
        this.kafkaPublisher = kafkaPublisher;
        this.analyticsService = analyticsService;
    }

    public List<Pedido> listarPedidos() {
        return pedidoRepository.findAllByOrderByFechaCreacionDesc();
    }

    public List<EventoMensajeLog> listarHistorialEventos() {
        return eventoLogRepository.findTop50ByOrderByFechaEventoDesc();
    }

    @Transactional
    public Map<String, Object> crearPedidoYDispararEventos(CrearPedidoRequestDTO request) {
        Producto producto = productoRepository.findById(request.getProductoId())
                .orElseThrow(() -> new IllegalArgumentException("El producto con ID " + request.getProductoId() + " no existe"));

        double montoTotal = producto.getPrecio() * request.getCantidad();

        Pedido pedido = new Pedido();
        pedido.setClienteEmail(request.getClienteEmail());
        pedido.setProductoNombre(producto.getNombre());
        pedido.setCantidad(request.getCantidad());
        pedido.setMontoTotal(montoTotal);
        pedido.setTipoDocumento(request.getTipoDocumento());
        pedido.setEstado(request.isSimularErrorDlq() ? "CON_OBSERVACION_DLQ" : "PROCESANDO_ASINCRONO");
        pedido.setFechaCreacion(LocalDateTime.now());

        Pedido guardado = pedidoRepository.save(pedido);

        // 1. Tareas operativas por cola en RabbitMQ
        NotificacionEventDTO notif = rabbitPublisher.publicarNotificacion(NotificacionEventDTO.builder()
                .pedidoId(guardado.getId())
                .destinatario(request.getClienteEmail())
                .asunto("Confirmación de Pedido #" + guardado.getId() + " - " + producto.getNombre())
                .mensaje("Hemos recibido tu pedido de " + request.getCantidad() + "x " + producto.getNombre()
                        + " por un total de $" + montoTotal)
                .canal("EMAIL")
                .simularErrorNoRecuperable(request.isSimularErrorDlq())
                .build());

        TicketEventDTO ticket = rabbitPublisher.publicarTicket(TicketEventDTO.builder()
                .pedidoId(guardado.getId())
                .cliente(request.getClienteEmail())
                .areaResponsable("LOGISTICA_DESPACHO")
                .prioridad(montoTotal > 500000 ? "ALTA" : "MEDIA")
                .descripcion("Preparar orden #" + guardado.getId() + ": " + request.getCantidad() + "x " + producto.getNombre())
                .simularErrorNoRecuperable(request.isSimularErrorDlq())
                .build());

        DocumentoEventDTO documento = rabbitPublisher.publicarDocumento(DocumentoEventDTO.builder()
                .pedidoId(guardado.getId())
                .tipoDocumento(request.getTipoDocumento())
                .rutReceptor("76.543.210-K")
                .detalleGlosa(request.getCantidad() + "x " + producto.getNombre())
                .montoTotal(montoTotal)
                .simularErrorNoRecuperable(request.isSimularErrorDlq())
                .build());

        // 2. Streaming de analitica e inventario en tiempo real hacia Apache Kafka
        String nombreCategoria = producto.getCategoria() != null
                ? producto.getCategoria().getNombre()
                : "GENERAL";

        VentaStreamEventDTO eventoVenta = kafkaPublisher.publicarEventoVenta(VentaStreamEventDTO.builder()
                .pedidoId(guardado.getId())
                .clienteEmail(request.getClienteEmail())
                .productoId(producto.getId())
                .productoNombre(producto.getNombre())
                .categoria(nombreCategoria)
                .cantidad(request.getCantidad())
                .montoTotal(montoTotal)
                .tipoDocumento(request.getTipoDocumento())
                .region("RM-SANTIAGO")
                .simularErrorProcesamiento(false)
                .build());

        int stockRestante = analyticsService.calcularNuevoStock(producto.getId(), "RESERVA_PEDIDO", request.getCantidad());
        InventarioStreamEventDTO eventoInventario = kafkaPublisher.publicarEventoInventario(InventarioStreamEventDTO.builder()
                .pedidoId(guardado.getId())
                .productoId(producto.getId())
                .productoNombre(producto.getNombre())
                .tipoMovimiento("RESERVA_PEDIDO")
                .unidades(request.getCantidad())
                .stockDisponible(stockRestante)
                .bodega("BODEGA-CENTRAL-SCL")
                .estadoOperacional(stockRestante <= 8 ? "STOCK_CRITICO" : "STOCK_NORMAL")
                .simularErrorProcesamiento(false)
                .build());

        Map<String, Object> resultado = new LinkedHashMap<>();
        resultado.put("pedido", guardado);
        resultado.put("eventosPublicados", Map.of(
                "notificacionMessageId", notif.getMessageId(),
                "ticketMessageId", ticket.getMessageId(),
                "documentoMessageId", documento.getMessageId(),
                "kafkaVentaEventId", eventoVenta.getEventId(),
                "kafkaInventarioEventId", eventoInventario.getEventId()
        ));
        return resultado;
    }
}
