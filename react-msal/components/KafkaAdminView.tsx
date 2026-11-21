import React, { useEffect, useState } from 'react';
import { useMsal } from '@azure/msal-react';
import { apiRequest } from '../apiClient';

interface BrokerNode {
  id: number;
  host: string;
  port: number;
  isController: boolean;
}

interface PartitionDetail {
  partition: number;
  leader: number;
  replicas: number[];
  isr: number[];
  endOffset: number;
  messages: number;
}

interface TopicInfo {
  name: string;
  partitions: number;
  replicas: number;
  totalMessages: number;
  configs: Record<string, string>;
  partitionDetails: PartitionDetail[];
}

interface OffsetLagItem {
  topic: string;
  partition: number;
  currentOffset: number;
  endOffset: number;
  lag: number;
}

interface ConsumerGroupInfo {
  groupId: string;
  state: string;
  memberCount: number;
  totalLag: number;
  offsets: OffsetLagItem[];
}

export function KafkaAdminView() {
  const { instance } = useMsal();
  const [clusterId, setClusterId] = useState('');
  const [brokers, setBrokers] = useState<BrokerNode[]>([]);
  const [topics, setTopics] = useState<TopicInfo[]>([]);
  const [groups, setGroups] = useState<ConsumerGroupInfo[]>([]);
  const [statusMsg, setStatusMsg] = useState('');
  const [errorMsg, setErrorMsg] = useState('');

  // Crear tópico
  const [newTopicName, setNewTopicName] = useState('pedidos.auditoria.eventos');
  const [newPartitions, setNewPartitions] = useState(3);
  const [newReplicas, setNewReplicas] = useState(3);
  const [newRetentionMs, setNewRetentionMs] = useState(86400000);
  const [newCleanupPolicy, setNewCleanupPolicy] = useState('delete');

  // Actualizar configuración de tópico existente
  const [editTopicName, setEditTopicName] = useState('pedidos.streaming.ventas');
  const [editRetentionMs, setEditRetentionMs] = useState(1209600000);
  const [editCleanupPolicy, setEditCleanupPolicy] = useState('delete');

  const cargarTodo = async () => {
    try {
      setErrorMsg('');
      const [clusterRes, topicsRes, groupsRes] = await Promise.all([
        apiRequest<{ clusterId: string; brokers: BrokerNode[] }>('/kafka/brokers', instance),
        apiRequest<TopicInfo[]>('/kafka/topics', instance),
        apiRequest<ConsumerGroupInfo[]>('/kafka/consumer-groups', instance),
      ]);
      setClusterId(clusterRes.clusterId);
      setBrokers(clusterRes.brokers);
      setTopics(topicsRes);
      setGroups(groupsRes);
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error consultando administración de Kafka');
    }
  };

  useEffect(() => {
    void cargarTodo();
  }, []);

  const handleCrearTopico = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setErrorMsg('');
      setStatusMsg('');
      const res = await apiRequest<{ message: string }>('/kafka/topics', instance, {
        method: 'POST',
        body: JSON.stringify({
          name: newTopicName,
          partitions: newPartitions,
          replicas: newReplicas,
          retentionMs: newRetentionMs,
          cleanupPolicy: newCleanupPolicy,
        }),
      });
      setStatusMsg(res.message);
      await cargarTodo();
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error creando tópico');
    }
  };

  const handleProbarValidacionInvalida = async () => {
    try {
      setErrorMsg('');
      setStatusMsg('');
      await apiRequest('/kafka/topics', instance, {
        method: 'POST',
        body: JSON.stringify({
          name: '',
          partitions: 0,
          replicas: 3,
          cleanupPolicy: 'invalido',
        }),
      });
    } catch (err: unknown) {
      setErrorMsg(`[Validación DTO @Valid - HTTP 400]: ${err instanceof Error ? err.message : 'Entrada inválida'}`);
    }
  };

  const handleActualizarTopico = async (e: React.FormEvent) => {
    e.preventDefault();
    try {
      setErrorMsg('');
      setStatusMsg('');
      const res = await apiRequest<{ message: string }>(
        `/kafka/topics/${encodeURIComponent(editTopicName)}`,
        instance,
        {
          method: 'PUT',
          body: JSON.stringify({
            retentionMs: editRetentionMs,
            cleanupPolicy: editCleanupPolicy,
          }),
        },
      );
      setStatusMsg(res.message);
      await cargarTodo();
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error actualizando tópico');
    }
  };

  const handleEliminarTopico = async (name: string) => {
    try {
      setErrorMsg('');
      setStatusMsg('');
      const res = await apiRequest<{ message: string }>(`/kafka/topics/${encodeURIComponent(name)}`, instance, {
        method: 'DELETE',
      });
      setStatusMsg(res.message);
      await cargarTodo();
    } catch (err: unknown) {
      setErrorMsg(err instanceof Error ? err.message : 'Error eliminando tópico');
    }
  };

  return (
    <div style={{ display: 'grid', gap: '1.25rem' }}>
      {statusMsg && (
        <div
          style={{
            background: '#ecfdf5',
            border: '1px solid #10b981',
            color: '#065f46',
            padding: '0.8rem 1rem',
            borderRadius: '8px',
          }}
        >
          <strong>KafkaAdminService:</strong> {statusMsg}
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
          <strong>Respuesta Validación / Error:</strong> {errorMsg}
        </div>
      )}

      {/* Brokers activos */}
      <section style={{ background: '#fff', padding: '1.15rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', flexWrap: 'wrap' }}>
          <h3 style={{ margin: 0 }}>
            Clúster Apache Kafka Activo ({brokers.length} Brokers) {clusterId ? `— ID: ${clusterId}` : ''}
          </h3>
          <button type="button" onClick={() => void cargarTodo()} style={{ padding: '0.4rem 0.85rem', cursor: 'pointer', fontWeight: 600 }}>
            Refrescar Brokers, Tópicos y Lag
          </button>
        </div>
        <div style={{ display: 'flex', gap: '0.75rem', flexWrap: 'wrap', marginTop: '0.75rem' }}>
          {brokers.map((b) => (
            <div
              key={b.id}
              style={{
                background: '#f8fafc',
                border: '1px solid #cbd5e1',
                padding: '0.6rem 1rem',
                borderRadius: '8px',
                fontSize: '0.88rem',
              }}
            >
              <strong>Broker #{b.id}</strong> — <code>{b.host}:{b.port}</code>{' '}
              {b.isController && (
                <span
                  style={{
                    background: '#dbeafe',
                    color: '#1e40af',
                    padding: '0.15rem 0.45rem',
                    borderRadius: '4px',
                    fontSize: '0.75rem',
                    fontWeight: 700,
                    marginLeft: '0.4rem',
                  }}
                >
                  CONTROLLER
                </span>
              )}
            </div>
          ))}
        </div>
      </section>

      {/* Formularios Crear y Modificar Tópicos */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(330px, 1fr))', gap: '1.25rem' }}>
        <section style={{ background: '#fff', padding: '1.15rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
          <h3 style={{ marginTop: 0 }}>POST /kafka/topics (Crear Tópico)</h3>
          <form onSubmit={(e) => void handleCrearTopico(e)} style={{ display: 'grid', gap: '0.55rem' }}>
            <input
              type="text"
              value={newTopicName}
              onChange={(e) => setNewTopicName(e.target.value)}
              placeholder="Nombre del tópico"
              style={{ padding: '0.45rem' }}
            />
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
              <label style={{ fontSize: '0.82rem' }}>
                Particiones:
                <input
                  type="number"
                  min={1}
                  max={24}
                  value={newPartitions}
                  onChange={(e) => setNewPartitions(Number(e.target.value))}
                  style={{ width: '100%', padding: '0.4rem', boxSizing: 'border-box' }}
                />
              </label>
              <label style={{ fontSize: '0.82rem' }}>
                Réplicas:
                <input
                  type="number"
                  min={1}
                  max={3}
                  value={newReplicas}
                  onChange={(e) => setNewReplicas(Number(e.target.value))}
                  style={{ width: '100%', padding: '0.4rem', boxSizing: 'border-box' }}
                />
              </label>
            </div>
            <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: '0.5rem' }}>
              <label style={{ fontSize: '0.82rem' }}>
                retention.ms:
                <input
                  type="number"
                  value={newRetentionMs}
                  onChange={(e) => setNewRetentionMs(Number(e.target.value))}
                  style={{ width: '100%', padding: '0.4rem', boxSizing: 'border-box' }}
                />
              </label>
              <label style={{ fontSize: '0.82rem' }}>
                cleanup.policy:
                <select
                  value={newCleanupPolicy}
                  onChange={(e) => setNewCleanupPolicy(e.target.value)}
                  style={{ width: '100%', padding: '0.42rem' }}
                >
                  <option value="delete">delete</option>
                  <option value="compact">compact</option>
                  <option value="compact,delete">compact,delete</option>
                </select>
              </label>
            </div>
            <button
              type="submit"
              style={{
                background: '#2563eb',
                color: '#fff',
                border: 'none',
                padding: '0.55rem',
                borderRadius: '6px',
                fontWeight: 600,
                cursor: 'pointer',
              }}
            >
              Crear Tópico en Clúster Kafka
            </button>
            <button
              type="button"
              onClick={() => void handleProbarValidacionInvalida()}
              style={{
                background: '#f59e0b',
                color: '#fff',
                border: 'none',
                padding: '0.45rem',
                borderRadius: '6px',
                cursor: 'pointer',
                fontSize: '0.8rem',
              }}
            >
              Probar Validación DTO (Tópico vacío / inválido -&gt; 400)
            </button>
          </form>
        </section>

        <section style={{ background: '#fff', padding: '1.15rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
          <h3 style={{ marginTop: 0 }}>PUT /kafka/topics/&#123;name&#125; (Modificar Retención / Policy)</h3>
          <form onSubmit={(e) => void handleActualizarTopico(e)} style={{ display: 'grid', gap: '0.55rem' }}>
            <label style={{ fontSize: '0.82rem' }}>
              Tópico a modificar:
              <select
                value={editTopicName}
                onChange={(e) => setEditTopicName(e.target.value)}
                style={{ width: '100%', padding: '0.45rem', marginTop: '0.2rem' }}
              >
                {topics.map((t) => (
                  <option key={t.name} value={t.name}>
                    {t.name}
                  </option>
                ))}
              </select>
            </label>
            <label style={{ fontSize: '0.82rem' }}>
              Nuevo retention.ms (ej. 14 días = 1209600000):
              <input
                type="number"
                value={editRetentionMs}
                onChange={(e) => setEditRetentionMs(Number(e.target.value))}
                style={{ width: '100%', padding: '0.45rem', boxSizing: 'border-box', marginTop: '0.2rem' }}
              />
            </label>
            <label style={{ fontSize: '0.82rem' }}>
              cleanup.policy:
              <select
                value={editCleanupPolicy}
                onChange={(e) => setEditCleanupPolicy(e.target.value)}
                style={{ width: '100%', padding: '0.45rem', marginTop: '0.2rem' }}
              >
                <option value="delete">delete</option>
                <option value="compact">compact</option>
                <option value="compact,delete">compact,delete</option>
              </select>
            </label>
            <button
              type="submit"
              style={{
                background: '#7c3aed',
                color: '#fff',
                border: 'none',
                padding: '0.55rem',
                borderRadius: '6px',
                fontWeight: 600,
                cursor: 'pointer',
              }}
            >
              Actualizar Configuración del Tópico
            </button>
          </form>
        </section>
      </div>

      {/* Tabla de Tópicos Activos */}
      <section style={{ background: '#fff', padding: '1.2rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
        <h3 style={{ marginTop: 0 }}>Tópicos Configurados (Particiones, Réplicas, Retención y Offsets)</h3>
        <div style={{ overflowX: 'auto' }}>
          <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.86rem' }}>
            <thead>
              <tr style={{ background: '#f3f4f6', textAlign: 'left' }}>
                <th style={{ padding: '0.55rem' }}>Nombre del Tópico</th>
                <th style={{ padding: '0.55rem' }}>Particiones</th>
                <th style={{ padding: '0.55rem' }}>Réplicas</th>
                <th style={{ padding: '0.55rem' }}>cleanup.policy</th>
                <th style={{ padding: '0.55rem' }}>retention.ms</th>
                <th style={{ padding: '0.55rem' }}>Mensajes Totales</th>
                <th style={{ padding: '0.55rem' }}>Acciones</th>
              </tr>
            </thead>
            <tbody>
              {topics.map((t) => (
                <tr key={t.name} style={{ borderBottom: '1px solid #e5e7eb' }}>
                  <td style={{ padding: '0.55rem', fontFamily: 'monospace', fontWeight: 700 }}>{t.name}</td>
                  <td style={{ padding: '0.55rem', fontWeight: 700 }}>{t.partitions}</td>
                  <td style={{ padding: '0.55rem', fontWeight: 700 }}>{t.replicas}</td>
                  <td style={{ padding: '0.55rem', fontFamily: 'monospace' }}>{t.configs['cleanup.policy'] || '-'}</td>
                  <td style={{ padding: '0.55rem', fontFamily: 'monospace' }}>{t.configs['retention.ms'] || '-'}</td>
                  <td style={{ padding: '0.55rem', fontWeight: 700, color: '#1d4ed8' }}>{t.totalMessages}</td>
                  <td style={{ padding: '0.55rem' }}>
                    <button
                      type="button"
                      onClick={() => void handleEliminarTopico(t.name)}
                      style={{
                        background: '#dc2626',
                        color: '#fff',
                        border: 'none',
                        padding: '0.3rem 0.6rem',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        fontSize: '0.78rem',
                      }}
                    >
                      DELETE
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      {/* Tabla de Consumer Groups, Offsets y Lag */}
      <section style={{ background: '#fff', padding: '1.2rem', borderRadius: '10px', boxShadow: '0 1px 4px rgba(0,0,0,0.08)' }}>
        <h3 style={{ marginTop: 0 }}>Supervisión de Consumer Groups ( Estado, Offsets y Lag por Partición )</h3>
        {groups.length === 0 ? (
          <p style={{ color: '#6b7280' }}>No se encontraron consumer groups activos todavía.</p>
        ) : (
          <div style={{ display: 'grid', gap: '0.9rem' }}>
            {groups.map((g) => (
              <div
                key={g.groupId}
                style={{
                  border: '1px solid #e5e7eb',
                  borderRadius: '8px',
                  padding: '0.85rem 1rem',
                  background: '#fafafa',
                }}
              >
                <div style={{ display: 'flex', justifyContent: 'space-between', flexWrap: 'wrap', gap: '0.5rem' }}>
                  <div>
                    <strong style={{ fontFamily: 'monospace', fontSize: '0.95rem', color: '#1e3a8a' }}>
                      {g.groupId}
                    </strong>{' '}
                    — Estado:{' '}
                    <span
                      style={{
                        background: '#dcfce7',
                        color: '#166534',
                        padding: '0.15rem 0.45rem',
                        borderRadius: '4px',
                        fontSize: '0.76rem',
                        fontWeight: 700,
                      }}
                    >
                      {g.state}
                    </span>
                  </div>
                  <div style={{ fontSize: '0.85rem' }}>
                    Consumidores activos: <strong>{g.memberCount}</strong> | Lag Total:{' '}
                    <strong style={{ color: g.totalLag > 0 ? '#d97706' : '#059669' }}>{g.totalLag}</strong>
                  </div>
                </div>

                {g.offsets.length > 0 && (
                  <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '0.8rem', marginTop: '0.55rem' }}>
                    <thead>
                      <tr style={{ background: '#f1f5f9', textAlign: 'left' }}>
                        <th style={{ padding: '0.35rem' }}>Tópico</th>
                        <th style={{ padding: '0.35rem' }}>Partición</th>
                        <th style={{ padding: '0.35rem' }}>Current Offset</th>
                        <th style={{ padding: '0.35rem' }}>Log End Offset</th>
                        <th style={{ padding: '0.35rem' }}>Lag</th>
                      </tr>
                    </thead>
                    <tbody>
                      {g.offsets.map((o, idx) => (
                        <tr key={idx} style={{ borderBottom: '1px solid #e2e8f0' }}>
                          <td style={{ padding: '0.35rem', fontFamily: 'monospace' }}>{o.topic}</td>
                          <td style={{ padding: '0.35rem', fontWeight: 600 }}>P{o.partition}</td>
                          <td style={{ padding: '0.35rem', fontFamily: 'monospace' }}>{o.currentOffset}</td>
                          <td style={{ padding: '0.35rem', fontFamily: 'monospace' }}>{o.endOffset}</td>
                          <td
                            style={{
                              padding: '0.35rem',
                              fontWeight: 700,
                              color: o.lag > 0 ? '#d97706' : '#059669',
                            }}
                          >
                            {o.lag}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                )}
              </div>
            ))}
          </div>
        )}
      </section>
    </div>
  );
}
