import React, { useEffect, useState } from 'react';
import { useMsal } from '@azure/msal-react';
import { apiRequest } from '../apiClient';

interface AlertaReactiva {
  tipo: string;
  severidad: string;
  mensaje: string;
  topico: string;
  particion: number;
  offset: number;
  fecha: string;
}

interface InventarioEstado {
  productoId: number;
  productoNombre: string;
  stockDisponible: number;
  ultimoMovimiento: string;
  bodega: string;
  estadoOperacional: string;
  ultimaParticion: number;
  ultimoOffset: number;
}

interface BitacoraItem {
  eventId: string;
  topico: string;
  consumerGroup: string;
  particion: number;
  offset: number;
  key: string;
  estado: string;
  detalle: string;
}

interface DashboardData {
  ingresosTotales: number;
  totalEventosVentas: number;
  unidadesVendidas: number;
  ticketPromedio: number;
  erroresControlados: number;
  ventasPorCategoria: Record<string, number>;
  distribucionParticiones: Record<string, number>;
  inventarioTiempoReal: InventarioEstado[];
  alertasReactivas: AlertaReactiva[];
  ultimosEventos: BitacoraItem[];
}

export function KafkaStreamingView() {
  const { instance } = useMsal();
  const [data, setData] = useState<DashboardData>({
    ingresosTotales: 0,
    totalEventosVentas: 0,
    unidadesVendidas: 0,
    ticketPromedio: 0,
    erroresControlados: 0,
    ventasPorCategoria: {},
    distribucionParticiones: {},
    inventarioTiempoReal: [],
    alertasReactivas: [],
    ultimosEventos: [],
  });
  const [feedback, setFeedback] = useState('');
  const [errorMsg, setErrorMsg] = useState('');

  const cargarAnalitica = async () => {
    try {
      const res = await apiRequest<DashboardData>('/api/kafka/analytics', instance);
      setData(res);
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error consultando analítica de Kafka');
    }
  };

  useEffect(() => {
    void cargarAnalitica();
    const timer = setInterval(() => {
      void cargarAnalitica();
    }, 4000);
    return () => clearInterval(timer);
  }, []);

  const enviarRafaga = async () => {
    try {
      setErrorMsg('');
      const res = await apiRequest<{ mensaje: string }>('/api/kafka/eventos/rafaga-prueba', instance, {
        method: 'POST',
      });
      setFeedback(res.mensaje);
      setTimeout(() => void cargarAnalitica(), 900);
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error enviando ráfaga');
    }
  };

  const enviarVentaVip = async () => {
    try {
      setErrorMsg('');
      const res = await apiRequest<{ eventId: string }>('/api/kafka/eventos/ventas', instance, {
        method: 'POST',
        body: JSON.stringify({
          clienteEmail: 'gerencia.ti@empresa.cl',
          productoId: 1,
          productoNombre: 'Servidor Rack Cloud Native',
          categoria: 'SERVIDORES',
          cantidad: 1,
          montoTotal: 1450000,
          tipoDocumento: 'FACTURA',
          region: 'RM-SANTIAGO',
          simularErrorProcesamiento: false,
        }),
      });
      setFeedback(`Evento VIP ${res.eventId} publicado en pedidos.streaming.ventas (dispara alerta automática).`);
      setTimeout(() => void cargarAnalitica(), 900);
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error publicando venta VIP');
    }
  };

  const enviarStockCritico = async () => {
    try {
      setErrorMsg('');
      const res = await apiRequest<{ eventId: string }>('/api/kafka/eventos/inventario', instance, {
        method: 'POST',
        body: JSON.stringify({
          productoId: 2,
          productoNombre: 'Mouse Inalámbrico Pro',
          tipoMovimiento: 'AJUSTE_STOCK',
          unidades: 5,
          stockDisponible: 3,
          bodega: 'BODEGA-CENTRAL-SCL',
          estadoOperacional: 'STOCK_CRITICO',
          simularErrorProcesamiento: false,
        }),
      });
      setFeedback(`Evento ${res.eventId} publicado en pedidos.streaming.inventario (stock crítico = 3 uds.).`);
      setTimeout(() => void cargarAnalitica(), 900);
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error publicando ajuste de inventario');
    }
  };

  const simularEventoDefectuoso = async () => {
    try {
      setErrorMsg('');
      const res = await apiRequest<{ eventId: string }>('/api/kafka/eventos/ventas', instance, {
        method: 'POST',
        body: JSON.stringify({
          clienteEmail: 'test.error@duocuc.cl',
          productoId: 3,
          productoNombre: 'Teclado Mecánico RGB',
          categoria: 'PERIFERICOS',
          cantidad: 1,
          montoTotal: 65990,
          tipoDocumento: 'BOLETA',
          region: 'RM-SANTIAGO',
          simularErrorProcesamiento: true,
        }),
      });
      setFeedback(
        `Evento con fallo simulado (${res.eventId}) enviado. DefaultErrorHandler aplicará reintentos (FixedBackOff) y descarte seguro sin detener el consumidor.`,
      );
      setTimeout(() => void cargarAnalitica(), 1500);
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error simulando evento defectuoso');
    }
  };

  return (
    <div style={{ display: 'grid', gap: '1.25rem' }}>
      {feedback && (
        <div
          style={{
            background: '#ecfdf5',
            border: '1px solid #10b981',
            color: '#065f46',
            padding: '0.8rem 1rem',
            borderRadius: '8px',
          }}
        >
          <strong>Streaming Kafka:</strong> {feedback}
        </div>
      )}
      {errorMsg && (
        <div
          style={{
            background: '#fef2f2',
            border: '1px solid #ef4444',
            color: '#991b1b',
            padding: '0.8rem 1rem',
            borderRadius: '8px',
          }}
        >
          <strong>Error:</strong> {errorMsg}
        </div>
      )}

      {/* KPIs en Tiempo Real */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(210px, 1fr))', gap: '1rem' }}>
        <div style={{ background: '#fff', padding: '1.1rem', borderRadius: '10px', borderLeft: '5px solid #2563eb' }}>
          <div style={{ fontSize: '0.82rem', color: '#6b7280' }}>Ingresos en Streaming (Kafka)</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 700, marginTop: '0.25rem' }}>
            ${data.ingresosTotales.toLocaleString('es-CL')}
          </div>
        </div>
        <div style={{ background: '#fff', padding: '1.1rem', borderRadius: '10px', borderLeft: '5px solid #059669' }}>
          <div style={{ fontSize: '0.82rem', color: '#6b7280' }}>Eventos Ventas Procesados</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 700, marginTop: '0.25rem' }}>
            {data.totalEventosVentas} ({data.unidadesVendidas} uds.)
          </div>
        </div>
        <div style={{ background: '#fff', padding: '1.1rem', borderRadius: '10px', borderLeft: '5px solid #7c3aed' }}>
          <div style={{ fontSize: '0.82rem', color: '#6b7280' }}>Ticket Promedio Tiempo Real</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 700, marginTop: '0.25rem' }}>
            ${data.ticketPromedio.toLocaleString('es-CL')}
          </div>
        </div>
        <div style={{ background: '#fff', padding: '1.1rem', borderRadius: '10px', borderLeft: '5px solid #f59e0b' }}>
          <div style={{ fontSize: '0.82rem', color: '#6b7280' }}>Reintentos / Errores Controlados</div>
          <div style={{ fontSize: '1.5rem', fontWeight: 700, marginTop: '0.25rem' }}>{data.erroresControlados}</div>
        </div>
      </div>

      {/* Acciones de Simulación en Tiempo Real */}
      <section
        style={{
          background: '#fff',
          padding: '1.2rem',
          borderRadius: '10px',
          boxShadow: '0 1px 4px rgba(0,0,0,0.08)',
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap', gap: '0.5rem' }}>
          <h3 style={{ margin: 0 }}>Productores de Eventos Kafka (2 Tópicos con 3 Particiones y 3 Réplicas)</h3>
          <button type="button" onClick={() => void cargarAnalitica()} style={{ padding: '0.4rem 0.8rem', cursor: 'pointer' }}>
            Refrescar Ahora
          </button>
        </div>
        <p style={{ fontSize: '0.86rem', color: '#4b5563', margin: '0.4rem 0 0.9rem 0' }}>
          Tópicos activos: <code>pedidos.streaming.ventas</code> (retención 7 días, delete) y{' '}
          <code>pedidos.streaming.inventario</code> (compact,delete). También se alimentan automáticamente al crear un
          pedido en la Pestaña 1.
        </p>
        <div style={{ display: 'flex', gap: '0.65rem', flexWrap: 'wrap' }}>
          <button
            type="button"
            onClick={() => void enviarRafaga()}
            style={{
              background: '#2563eb',
              color: '#fff',
              border: 'none',
              padding: '0.6rem 1rem',
              borderRadius: '6px',
              fontWeight: 600,
              cursor: 'pointer',
            }}
          >
            Enviar Ráfaga (12 Eventos &rarr; Balanceo en 3 Particiones)
          </button>
          <button
            type="button"
            onClick={() => void enviarVentaVip()}
            style={{
              background: '#7c3aed',
              color: '#fff',
              border: 'none',
              padding: '0.6rem 1rem',
              borderRadius: '6px',
              fontWeight: 600,
              cursor: 'pointer',
            }}
          >
            Publicar Venta Alto Valor ($1.450.000 &rarr; Alerta Automática)
          </button>
          <button
            type="button"
            onClick={() => void enviarStockCritico()}
            style={{
              background: '#059669',
              color: '#fff',
              border: 'none',
              padding: '0.6rem 1rem',
              borderRadius: '6px',
              fontWeight: 600,
              cursor: 'pointer',
            }}
          >
            Publicar Stock Crítico (3 uds. &rarr; Alerta Inventario)
          </button>
          <button
            type="button"
            onClick={() => void simularEventoDefectuoso()}
            style={{
              background: '#dc2626',
              color: '#fff',
              border: 'none',
              padding: '0.6rem 1rem',
              borderRadius: '6px',
              fontWeight: 600,
              cursor: 'pointer',
            }}
          >
            Simular Evento Defectuoso (Probar Reintentos y Resiliencia)
          </button>
        </div>
      </section>

      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(350px, 1fr))', gap: '1.25rem' }}>
        {/* Alertas Reactivas en Tiempo Real */}
        <section style={{ background: '#fff', padding: '1.15rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
          <h3 style={{ marginTop: 0 }}>Reacción Automática a Eventos (Alertas en Tiempo Real)</h3>
          {data.alertasReactivas.length === 0 ? (
            <p style={{ color: '#6b7280', fontSize: '0.9rem' }}>
              Sin alertas activas. Envía una ráfaga, venta de alto valor o stock crítico para observar la detección reactiva.
            </p>
          ) : (
            <ul style={{ listStyle: 'none', padding: 0, margin: 0, display: 'grid', gap: '0.55rem' }}>
              {data.alertasReactivas.slice(0, 6).map((a, idx) => (
                <li
                  key={idx}
                  style={{
                    padding: '0.6rem 0.75rem',
                    borderRadius: '6px',
                    background: a.severidad === 'CRITICA' ? '#fef2f2' : '#eff6ff',
                    borderLeft: `4px solid ${a.severidad === 'CRITICA' ? '#dc2626' : '#2563eb'}`,
                    fontSize: '0.85rem',
                  }}
                >
                  <strong>[{a.tipo}]</strong> {a.mensaje}
                  <div style={{ fontSize: '0.76rem', color: '#4b5563', marginTop: '0.2rem' }}>
                    Tópico: <code>{a.topico}</code> | Partición: <code>{a.particion}</code> | Offset: <code>{a.offset}</code>
                  </div>
                </li>
              ))}
            </ul>
          )}
        </section>

        {/* Distribución por Particiones e Inventario */}
        <section style={{ background: '#fff', padding: '1.15rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
          <h3 style={{ marginTop: 0 }}>Distribución en Particiones y Estado de Inventario</h3>
          <div style={{ marginBottom: '0.9rem' }}>
            <strong style={{ fontSize: '0.86rem' }}>Mensajes Procesados por Tópico / Partición:</strong>
            <div style={{ display: 'flex', flexWrap: 'wrap', gap: '0.45rem', marginTop: '0.4rem' }}>
              {Object.entries(data.distribucionParticiones).length === 0 ? (
                <span style={{ color: '#6b7280', fontSize: '0.85rem' }}>Aún sin eventos consumidos.</span>
              ) : (
                Object.entries(data.distribucionParticiones).map(([k, count]) => (
                  <span
                    key={k}
                    style={{
                      background: '#f3f4f6',
                      padding: '0.25rem 0.55rem',
                      borderRadius: '4px',
                      fontSize: '0.8rem',
                      fontFamily: 'monospace',
                    }}
                  >
                    {k}: <strong>{count}</strong>
                  </span>
                ))
              )}
            </div>
          </div>

          <strong style={{ fontSize: '0.86rem' }}>Último Estado Compactado por Producto (Inventario):</strong>
          {data.inventarioTiempoReal.length === 0 ? (
            <p style={{ color: '#6b7280', fontSize: '0.85rem' }}>Sin movimientos de inventario registrados aún.</p>
          ) : (
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.82rem', marginTop: '0.4rem' }}>
              <thead>
                <tr style={{ background: '#f9fafb', textAlign: 'left' }}>
                  <th style={{ padding: '0.4rem' }}>Producto</th>
                  <th style={{ padding: '0.4rem' }}>Stock</th>
                  <th style={{ padding: '0.4rem' }}>Estado</th>
                  <th style={{ padding: '0.4rem' }}>Part/Offset</th>
                </tr>
              </thead>
              <tbody>
                {data.inventarioTiempoReal.map((inv) => (
                  <tr key={inv.productoId} style={{ borderBottom: '1px solid #e5e7eb' }}>
                    <td style={{ padding: '0.4rem' }}>{inv.productoNombre}</td>
                    <td style={{ padding: '0.4rem', fontWeight: 700 }}>{inv.stockDisponible}</td>
                    <td style={{ padding: '0.4rem' }}>
                      <span
                        style={{
                          padding: '0.15rem 0.4rem',
                          borderRadius: '4px',
                          background: inv.estadoOperacional === 'STOCK_CRITICO' ? '#fee2e2' : '#d1fae5',
                          color: inv.estadoOperacional === 'STOCK_CRITICO' ? '#991b1b' : '#065f46',
                          fontWeight: 600,
                          fontSize: '0.74rem',
                        }}
                      >
                        {inv.estadoOperacional}
                      </span>
                    </td>
                    <td style={{ padding: '0.4rem', fontFamily: 'monospace' }}>
                      P{inv.ultimaParticion} / #{inv.ultimoOffset}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </section>
      </div>

      {/* Flujo en Vivo de Consumer Groups */}
      <section style={{ background: '#fff', padding: '1.2rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
        <h3 style={{ marginTop: 0 }}>Flujo Reciente de Consumidores (@KafkaListener por Consumer Group)</h3>
        {data.ultimosEventos.length === 0 ? (
          <p style={{ color: '#6b7280' }}>Envía eventos arriba o crea un pedido para ver el flujo de consumo en tiempo real.</p>
        ) : (
          <div style={{ overflowX: 'auto' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.84rem' }}>
              <thead>
                <tr style={{ background: '#f3f4f6', textAlign: 'left' }}>
                  <th style={{ padding: '0.5rem' }}>Event ID</th>
                  <th style={{ padding: '0.5rem' }}>Tópico</th>
                  <th style={{ padding: '0.5rem' }}>Consumer Group</th>
                  <th style={{ padding: '0.5rem' }}>Partición</th>
                  <th style={{ padding: '0.5rem' }}>Offset</th>
                  <th style={{ padding: '0.5rem' }}>Estado</th>
                  <th style={{ padding: '0.5rem' }}>Detalle</th>
                </tr>
              </thead>
              <tbody>
                {data.ultimosEventos.slice(0, 15).map((ev, i) => (
                  <tr key={`${ev.eventId}-${ev.consumerGroup}-${i}`} style={{ borderBottom: '1px solid #e5e7eb' }}>
                    <td style={{ padding: '0.5rem', fontFamily: 'monospace', fontWeight: 600 }}>{ev.eventId}</td>
                    <td style={{ padding: '0.5rem', fontFamily: 'monospace' }}>{ev.topico}</td>
                    <td style={{ padding: '0.5rem', fontFamily: 'monospace', color: '#1d4ed8' }}>{ev.consumerGroup}</td>
                    <td style={{ padding: '0.5rem', fontWeight: 700 }}>P{ev.particion}</td>
                    <td style={{ padding: '0.5rem', fontFamily: 'monospace' }}>{ev.offset}</td>
                    <td style={{ padding: '0.5rem' }}>
                      <span
                        style={{
                          padding: '0.18rem 0.45rem',
                          borderRadius: '4px',
                          background: ev.estado.includes('ERROR') ? '#fee2e2' : '#dcfce7',
                          color: ev.estado.includes('ERROR') ? '#991b1b' : '#166534',
                          fontWeight: 700,
                          fontSize: '0.75rem',
                        }}
                      >
                        {ev.estado}
                      </span>
                    </td>
                    <td style={{ padding: '0.5rem' }}>{ev.detalle}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </section>
    </div>
  );
}
