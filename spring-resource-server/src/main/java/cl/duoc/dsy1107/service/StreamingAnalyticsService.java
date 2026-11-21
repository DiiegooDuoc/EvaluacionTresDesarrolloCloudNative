package cl.duoc.dsy1107.service;

import cl.duoc.dsy1107.dto.InventarioStreamEventDTO;
import cl.duoc.dsy1107.dto.VentaStreamEventDTO;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.DoubleAdder;

@Service
public class StreamingAnalyticsService {

    private final DoubleAdder ingresosTotales = new DoubleAdder();
    private final AtomicInteger totalEventosVentas = new AtomicInteger(0);
    private final AtomicInteger unidadesVendidas = new AtomicInteger(0);
    private final AtomicInteger erroresControlados = new AtomicInteger(0);

    private final Map<String, Double> ventasPorCategoria = new ConcurrentHashMap<>();
    private final Map<String, Integer> conteoPorDocumento = new ConcurrentHashMap<>();
    private final Map<String, Integer> distribucionParticiones = new ConcurrentHashMap<>();
    private final Map<Long, Map<String, Object>> inventarioTiempoReal = new ConcurrentHashMap<>();

    private final Deque<Map<String, Object>> alertasReactivas = new ConcurrentLinkedDeque<>();
    private final Deque<Map<String, Object>> bitacoraStreaming = new ConcurrentLinkedDeque<>();

    public int calcularNuevoStock(Long productoId, String tipoMovimiento, int unidades) {
        int stockActual = inventarioTiempoReal.containsKey(productoId)
                ? (int) inventarioTiempoReal.get(productoId).get("stockDisponible")
                : 20;

        if ("REPOSICION".equalsIgnoreCase(tipoMovimiento)) {
            return stockActual + unidades;
        }
        return Math.max(0, stockActual - unidades);
    }

    public void registrarMetricaVenta(VentaStreamEventDTO event, String topic, int partition, long offset, String groupId) {
        ingresosTotales.add(event.getMontoTotal());
        totalEventosVentas.incrementAndGet();
        unidadesVendidas.addAndGet(event.getCantidad());

        ventasPorCategoria.merge(event.getCategoria(), event.getMontoTotal(), Double::sum);
        conteoPorDocumento.merge(event.getTipoDocumento(), 1, Integer::sum);
        distribucionParticiones.merge(topic + " [P" + partition + "]", 1, Integer::sum);

        registrarBitacora(
                event.getEventId(),
                topic,
                groupId,
                partition,
                offset,
                event.getCategoria(),
                "PROCESADO_OK",
                "Venta registrada en analítica: " + event.getCantidad() + "x " + event.getProductoNombre()
                        + " ($" + event.getMontoTotal() + ")"
        );
    }

    public void evaluarReglasAlertaVenta(VentaStreamEventDTO event, String topic, int partition, long offset, String groupId) {
        if (event.getMontoTotal() != null && event.getMontoTotal() >= 500000.0) {
            agregarAlerta(
                    "VENTA_ALTO_VALOR",
                    "ALTA",
                    "Compra corporativa/VIP detectada automáticamente por $" + event.getMontoTotal()
                            + " (" + event.getProductoNombre() + " - Cliente: " + event.getClienteEmail() + ")",
                    topic,
                    partition,
                    offset
            );
        }

        registrarBitacora(
                event.getEventId(),
                topic,
                groupId,
                partition,
                offset,
                event.getCategoria(),
                "EVALUADO_ALERTAS",
                event.getMontoTotal() >= 500000.0
                        ? "Alerta automática generada por transacción de alto monto"
                        : "Transacción dentro de umbral normal"
        );
    }

    public void actualizarEstadoInventario(InventarioStreamEventDTO event, String topic, int partition, long offset, String groupId) {
        Map<String, Object> estadoProd = new LinkedHashMap<>();
        estadoProd.put("productoId", event.getProductoId());
        estadoProd.put("productoNombre", event.getProductoNombre());
        estadoProd.put("stockDisponible", event.getStockDisponible());
        estadoProd.put("ultimoMovimiento", event.getTipoMovimiento());
        estadoProd.put("bodega", event.getBodega());
        estadoProd.put("estadoOperacional", event.getEstadoOperacional());
        estadoProd.put("ultimaParticion", partition);
        estadoProd.put("ultimoOffset", offset);
        estadoProd.put("actualizadoEn", LocalDateTime.now().toString());

        inventarioTiempoReal.put(event.getProductoId(), estadoProd);
        distribucionParticiones.merge(topic + " [P" + partition + "]", 1, Integer::sum);

        if (event.getStockDisponible() != null && event.getStockDisponible() <= 8) {
            agregarAlerta(
                    "STOCK_CRITICO",
                    "CRITICA",
                    "Reposición automática sugerida: '" + event.getProductoNombre()
                            + "' quedó con solo " + event.getStockDisponible() + " unidades en " + event.getBodega(),
                    topic,
                    partition,
                    offset
            );
        }

        registrarBitacora(
                event.getEventId(),
                topic,
                groupId,
                partition,
                offset,
                "PROD-" + event.getProductoId(),
                "INVENTARIO_ACTUALIZADO",
                event.getTipoMovimiento() + " (" + event.getUnidades() + " uds.) -> Stock actual: " + event.getStockDisponible()
        );
    }

    public void registrarErrorProcesamiento(String eventId, String topic, String groupId, int partition, long offset, String motivo) {
        erroresControlados.incrementAndGet();
        registrarBitacora(eventId, topic, groupId, partition, offset, "ERROR", "ERROR_CONTROLADO", motivo);
    }

    private void agregarAlerta(String tipo, String severidad, String mensaje, String topic, int partition, long offset) {
        Map<String, Object> alerta = new LinkedHashMap<>();
        alerta.put("tipo", tipo);
        alerta.put("severidad", severidad);
        alerta.put("mensaje", mensaje);
        alerta.put("topico", topic);
        alerta.put("particion", partition);
        alerta.put("offset", offset);
        alerta.put("fecha", LocalDateTime.now().toString());

        alertasReactivas.addFirst(alerta);
        while (alertasReactivas.size() > 25) {
            alertasReactivas.removeLast();
        }
    }

    private void registrarBitacora(String eventId, String topic, String groupId, int partition,
                                   long offset, String key, String estado, String detalle) {
        Map<String, Object> entry = new LinkedHashMap<>();
        entry.put("eventId", eventId != null ? eventId : "EVT");
        entry.put("topico", topic);
        entry.put("consumerGroup", groupId);
        entry.put("particion", partition);
        entry.put("offset", offset);
        entry.put("key", key);
        entry.put("estado", estado);
        entry.put("detalle", detalle);
        entry.put("fecha", LocalDateTime.now().toString());

        bitacoraStreaming.addFirst(entry);
        while (bitacoraStreaming.size() > 60) {
            bitacoraStreaming.removeLast();
        }
    }

    public Map<String, Object> obtenerDashboardAnalitica() {
        int totalTx = totalEventosVentas.get();
        double totalMonto = ingresosTotales.sum();
        double ticketPromedio = totalTx > 0 ? totalMonto / totalTx : 0.0;

        Map<String, Object> resumen = new LinkedHashMap<>();
        resumen.put("ingresosTotales", totalMonto);
        resumen.put("totalEventosVentas", totalTx);
        resumen.put("unidadesVendidas", unidadesVendidas.get());
        resumen.put("ticketPromedio", Math.round(ticketPromedio));
        resumen.put("erroresControlados", erroresControlados.get());
        resumen.put("ventasPorCategoria", new LinkedHashMap<>(ventasPorCategoria));
        resumen.put("conteoPorDocumento", new LinkedHashMap<>(conteoPorDocumento));
        resumen.put("distribucionParticiones", new LinkedHashMap<>(distribucionParticiones));
        resumen.put("inventarioTiempoReal", new ArrayList<>(inventarioTiempoReal.values()));
        resumen.put("alertasReactivas", new ArrayList<>(alertasReactivas));
        resumen.put("ultimosEventos", new ArrayList<>(bitacoraStreaming));
        return resumen;
    }
}
