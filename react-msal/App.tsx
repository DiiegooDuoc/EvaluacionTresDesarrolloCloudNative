import React, { useState } from 'react';
import { AuthenticatedTemplate, useMsal } from '@azure/msal-react';
import { ProtectedRoute } from './ProtectedRoute';
import { CatalogoPedidosView } from './components/CatalogoPedidosView';
import { MensajeriaAsyncView } from './components/MensajeriaAsyncView';
import { RabbitAdminView } from './components/RabbitAdminView';
import { EventosLogView } from './components/EventosLogView';
import { KafkaStreamingView } from './components/KafkaStreamingView';
import { KafkaAdminView } from './components/KafkaAdminView';

type TabKey = 'PEDIDOS' | 'KAFKA_STREAM' | 'KAFKA_ADMIN' | 'MENSAJERIA' | 'RABBIT_ADMIN' | 'LOGS';

export function App() {
  const { instance, accounts } = useMsal();
  const [activeTab, setActiveTab] = useState<TabKey>('PEDIDOS');
  const [jwtPreview, setJwtPreview] = useState<string>('');

  const handleLogout = () => {
    instance.logoutPopup().catch((e) => console.error('Error de logout:', e));
  };

  const mostrarTokenJwt = async () => {
    try {
      const scope = import.meta.env.VITE_API_SCOPE;
      const res = await instance.acquireTokenSilent({
        scopes: [scope],
        account: accounts[0],
      });
      setJwtPreview(res.accessToken);
    } catch (e) {
      console.error('Error obteniendo token:', e);
    }
  };

  const tabButtonStyle = (tab: TabKey): React.CSSProperties => ({
    padding: '0.58rem 0.95rem',
    border: 'none',
    borderRadius: '6px',
    cursor: 'pointer',
    fontWeight: 600,
    fontSize: '0.86rem',
    background: activeTab === tab ? '#1d4ed8' : '#e5e7eb',
    color: activeTab === tab ? '#ffffff' : '#1f2937',
  });

  return (
    <div style={{ maxWidth: '1280px', margin: '0 auto', padding: '1.5rem', fontFamily: 'system-ui, sans-serif' }}>
      <header
        style={{
          background: '#1e293b',
          color: '#ffffff',
          padding: '1.25rem 1.5rem',
          borderRadius: '10px',
          display: 'flex',
          justifyContent: 'space-between',
          alignItems: 'center',
          flexWrap: 'wrap',
          gap: '1rem',
        }}
      >
        <div>
          <h1 style={{ margin: 0, fontSize: '1.4rem' }}>
            Sistema Cloud Native: Pedidos, RabbitMQ &amp; Streaming Apache Kafka
          </h1>
          <p style={{ margin: '0.25rem 0 0 0', fontSize: '0.86rem', color: '#cbd5e1' }}>
            DSY1107 Desarrollo Cloud Native I — Entra ID (OIDC) + Clúster RabbitMQ + Clúster Kafka (3 ZK, 3 Brokers)
          </p>
        </div>

        <AuthenticatedTemplate>
          <div style={{ display: 'flex', alignItems: 'center', gap: '0.75rem' }}>
            <span style={{ fontSize: '0.86rem', background: '#334155', padding: '0.4rem 0.75rem', borderRadius: '6px' }}>
              Usuario: <strong>{accounts[0]?.username}</strong>
            </span>
            <button
              type="button"
              onClick={() => void mostrarTokenJwt()}
              style={{
                background: '#3b82f6',
                color: '#fff',
                border: 'none',
                padding: '0.45rem 0.8rem',
                borderRadius: '6px',
                cursor: 'pointer',
                fontSize: '0.82rem',
              }}
            >
              Ver JWT
            </button>
            <button
              type="button"
              onClick={handleLogout}
              style={{
                background: '#ef4444',
                color: '#fff',
                border: 'none',
                padding: '0.45rem 0.8rem',
                borderRadius: '6px',
                cursor: 'pointer',
                fontSize: '0.82rem',
              }}
            >
              Cerrar Sesión
            </button>
          </div>
        </AuthenticatedTemplate>
      </header>

      {jwtPreview && (
        <div
          style={{
            background: '#eff6ff',
            border: '1px solid #93c5fd',
            padding: '0.75rem 1rem',
            borderRadius: '8px',
            marginTop: '1rem',
            fontSize: '0.8rem',
            wordBreak: 'break-all',
          }}
        >
          <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: '0.35rem' }}>
            <strong>Access Token JWT (Bearer enviado al API Manager / Backend):</strong>
            <button type="button" onClick={() => setJwtPreview('')} style={{ cursor: 'pointer' }}>
              Ocultar
            </button>
          </div>
          <code>{jwtPreview}</code>
        </div>
      )}

      <ProtectedRoute>
        <AuthenticatedTemplate>
          <nav style={{ display: 'flex', gap: '0.5rem', margin: '1.2rem 0', flexWrap: 'wrap' }}>
            <button type="button" style={tabButtonStyle('PEDIDOS')} onClick={() => setActiveTab('PEDIDOS')}>
              1. Catálogo y Pedidos
            </button>
            <button type="button" style={tabButtonStyle('KAFKA_STREAM')} onClick={() => setActiveTab('KAFKA_STREAM')}>
              2. Streaming y Analítica Kafka
            </button>
            <button type="button" style={tabButtonStyle('KAFKA_ADMIN')} onClick={() => setActiveTab('KAFKA_ADMIN')}>
              3. Administrador Kafka (Tópicos &amp; Consumer Groups)
            </button>
            <button type="button" style={tabButtonStyle('MENSAJERIA')} onClick={() => setActiveTab('MENSAJERIA')}>
              4. Colas RabbitMQ (ACK / DLQ)
            </button>
            <button type="button" style={tabButtonStyle('RABBIT_ADMIN')} onClick={() => setActiveTab('RABBIT_ADMIN')}>
              5. Administrador RabbitMQ
            </button>
            <button type="button" style={tabButtonStyle('LOGS')} onClick={() => setActiveTab('LOGS')}>
              6. Logs RabbitMQ DLQ
            </button>
          </nav>

          <main>
            {activeTab === 'PEDIDOS' && <CatalogoPedidosView />}
            {activeTab === 'KAFKA_STREAM' && <KafkaStreamingView />}
            {activeTab === 'KAFKA_ADMIN' && <KafkaAdminView />}
            {activeTab === 'MENSAJERIA' && <MensajeriaAsyncView />}
            {activeTab === 'RABBIT_ADMIN' && <RabbitAdminView />}
            {activeTab === 'LOGS' && <EventosLogView />}
          </main>
        </AuthenticatedTemplate>
      </ProtectedRoute>
    </div>
  );
}
