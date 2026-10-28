import { useCallback, useEffect, useMemo, useState } from 'react';
import { io } from 'socket.io-client';

const API_URL = import.meta.env.VITE_API_URL || 'http://localhost:3000';
const KAFKA_UI_URL = 'http://localhost:8080';
const MAILHOG_URL = 'http://localhost:8025';

const PLANS = ['Plan Digital', 'Plan Fibra 500', 'Plan Móvil 20GB', 'Plan Full Hogar'];
const FAILURES = [
  { value: 'none', label: 'Ninguno' },
  { value: 'billing', label: 'Billing' },
  { value: 'provisioning', label: 'Provisioning' },
];

const EVENT_KIND = {
  ActivationRequested: 'ok',
  BillingAccountCreated: 'ok',
  ProvisioningCompleted: 'ok',
  ActivationCompleted: 'ok',
  BillingFailed: 'error',
  ProvisioningFailed: 'error',
  ActivationFailed: 'error',
  BillingAccountCancelled: 'warn',
};

const STATUS_LABEL = {
  PENDING: 'Pendiente',
  IN_PROGRESS: 'En curso',
  ACTIVE: 'Activa',
  FAILED: 'Fallida',
};

const randomCustomer = () => `C-${Math.floor(1000 + Math.random() * 9000)}`;

function formatTime(iso) {
  const d = new Date(iso);
  return d.toLocaleTimeString('es-AR', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
}

function elapsed(fromIso, toIso) {
  const ms = new Date(toIso).getTime() - new Date(fromIso).getTime();
  return `+${(ms / 1000).toFixed(1)} s`;
}

async function api(path, options = {}) {
  const res = await fetch(`${API_URL}${path}`, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });
  const body = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(body?.message || `HTTP ${res.status}`);
  return body;
}

export default function App() {
  const [activations, setActivations] = useState({});
  const [selectedId, setSelectedId] = useState(null);
  const [connected, setConnected] = useState(false);
  const [customerId, setCustomerId] = useState(randomCustomer);
  const [planId, setPlanId] = useState(PLANS[0]);
  const [simulateFailure, setSimulateFailure] = useState('none');
  const [busy, setBusy] = useState(false);
  const [toasts, setToasts] = useState([]);

  const notify = useCallback((text, kind = 'info') => {
    const id = Math.random().toString(36).slice(2);
    setToasts((t) => [...t, { id, text, kind }]);
    setTimeout(() => setToasts((t) => t.filter((x) => x.id !== id)), 6000);
  }, []);

  const upsert = useCallback((activation) => {
    setActivations((prev) => ({ ...prev, [activation.activationId]: activation }));
  }, []);

  // Carga inicial + timeline en vivo por WebSocket
  useEffect(() => {
    api('/activations')
      .then((list) => {
        const map = {};
        list.forEach((a) => (map[a.activationId] = a));
        setActivations(map);
        if (list[0]) setSelectedId((current) => current ?? list[0].activationId);
      })
      .catch(() => notify('No se pudo conectar con activation-api (¿todavía está arrancando?)', 'error'));

    const socket = io(API_URL, { transports: ['websocket', 'polling'] });
    socket.on('connect', () => setConnected(true));
    socket.on('disconnect', () => setConnected(false));
    socket.on('activation.updated', upsert);
    return () => socket.disconnect();
  }, [upsert, notify]);

  const sorted = useMemo(
    () => Object.values(activations).sort((a, b) => new Date(b.createdAt) - new Date(a.createdAt)),
    [activations],
  );
  const selected = selectedId ? activations[selectedId] : null;

  async function contract(e) {
    e.preventDefault();
    setBusy(true);
    try {
      const created = await api('/activations', {
        method: 'POST',
        body: JSON.stringify({ customerId, planId, simulateFailure }),
      });
      upsert(created);
      setSelectedId(created.activationId);
      notify(`202 Accepted · ${created.activationId} en estado ${created.status}`, 'ok');
    } catch (err) {
      notify(`Error: ${err.message}`, 'error');
    } finally {
      setBusy(false);
    }
  }

  async function replay() {
    if (!selected) return;
    try {
      const r = await api(`/demo/replay/${selected.activationId}`, { method: 'POST' });
      notify(`Reenviado ${r.eventId.slice(0, 8)}… a ${r.topic}. Mirá los logs: se descarta por idempotencia.`, 'info');
    } catch (err) {
      notify(`Error: ${err.message}`, 'error');
    }
  }

  async function poison() {
    try {
      const r = await api('/demo/poison', { method: 'POST', body: JSON.stringify({}) });
      notify(`Mensaje inválido en ${r.topic}. En ~7 s aparece en ${r.expectedDlq} (Kafka UI).`, 'warn');
    } catch (err) {
      notify(`Error: ${err.message}`, 'error');
    }
  }

  return (
    <div className="app">
      <header className="topbar">
        <div>
          <h1>Activación de servicios</h1>
          <p className="subtitle">POC · Kafka + NestJS + React · un solo evento dispara toda la activación</p>
        </div>
        <nav className="links">
          <span className={`pill ${connected ? 'pill-ok' : 'pill-off'}`}>
            <span className="dot" /> {connected ? 'WebSocket conectado' : 'Sin conexión'}
          </span>
          <a href={KAFKA_UI_URL} target="_blank" rel="noreferrer">Kafka UI ↗</a>
          <a href={MAILHOG_URL} target="_blank" rel="noreferrer">Mailhog ↗</a>
        </nav>
      </header>

      <form className="card form" onSubmit={contract}>
        <h2>Contratar plan</h2>
        <div className="form-row">
          <label>
            Cliente
            <div className="input-with-btn">
              <input value={customerId} onChange={(e) => setCustomerId(e.target.value)} required />
              <button type="button" className="ghost" title="Otro cliente" onClick={() => setCustomerId(randomCustomer())}>
                ↻
              </button>
            </div>
          </label>
          <label>
            Plan
            <select value={planId} onChange={(e) => setPlanId(e.target.value)}>
              {PLANS.map((p) => (
                <option key={p}>{p}</option>
              ))}
            </select>
          </label>
          <fieldset>
            <legend>Forzar fallo</legend>
            <div className="segmented">
              {FAILURES.map((f) => (
                <button
                  type="button"
                  key={f.value}
                  className={simulateFailure === f.value ? 'active' : ''}
                  onClick={() => setSimulateFailure(f.value)}
                >
                  {f.label}
                </button>
              ))}
            </div>
          </fieldset>
          <button className="primary" type="submit" disabled={busy}>
            {busy ? 'Enviando…' : 'Contratar plan'}
          </button>
        </div>
      </form>

      <main className="grid">
        <section className="card list">
          <h2>Activaciones <span className="count">{sorted.length}</span></h2>
          {sorted.length === 0 && <p className="empty">Todavía no hay activaciones. Contratá un plan para empezar.</p>}
          <ul>
            {sorted.map((a) => (
              <li
                key={a.activationId}
                className={a.activationId === selectedId ? 'selected' : ''}
                onClick={() => setSelectedId(a.activationId)}
              >
                <div className="row">
                  <code>{a.activationId}</code>
                  <StatusBadge status={a.status} />
                </div>
                <div className="row muted">
                  <span>
                    {a.customerId} · {a.planId}
                  </span>
                  <span>{formatTime(a.createdAt)}</span>
                </div>
              </li>
            ))}
          </ul>
        </section>

        <section className="card detail">
          {!selected ? (
            <p className="empty">Seleccioná una activación para ver su timeline.</p>
          ) : (
            <>
              <div className="detail-head">
                <div>
                  <h2>
                    <code>{selected.activationId}</code>
                  </h2>
                  <p className="muted">
                    {selected.customerId} · {selected.planId} · fallo simulado: <b>{selected.simulateFailure}</b>
                  </p>
                </div>
                <StatusBadge status={selected.status} big />
              </div>

              <div className="steps">
                <Step name="Billing" result={selected.steps?.billing} />
                <Step name="Provisioning" result={selected.steps?.provisioning} />
              </div>
              {selected.failureReason && <p className="failure">Motivo: {selected.failureReason}</p>}

              <h3>Timeline de eventos</h3>
              <ol className="timeline">
                {selected.history.map((h) => (
                  <li key={h.eventId} className={`ev ev-${EVENT_KIND[h.eventType] || 'ok'}`}>
                    <span className="marker" />
                    <div className="ev-body">
                      <div className="row">
                        <strong>{h.eventType}</strong>
                        <span className="muted">
                          {formatTime(h.at)} · {elapsed(selected.history[0].at, h.at)}
                        </span>
                      </div>
                      <div className="row muted">
                        <span>{h.detail}</span>
                        <span className="source">{h.source}</span>
                      </div>
                    </div>
                  </li>
                ))}
              </ol>

              <div className="tools">
                <button className="ghost" onClick={replay} title="Vuelve a publicar el ActivationRequested original (mismo eventId)">
                  Reenviar evento duplicado
                </button>
              </div>
            </>
          )}
        </section>
      </main>

      <section className="card demo-tools">
        <h2>Herramientas de demo</h2>
        <div className="tools">
          <button className="ghost danger" onClick={poison}>
            Enviar mensaje inválido → DLQ
          </button>
          <span className="muted">
            Se reintenta 3 veces (1 s, 2 s, 4 s) y termina en <code>activation.requested.dlq</code>. El resto del flujo sigue.
          </span>
        </div>
      </section>

      <div className="toasts">
        {toasts.map((t) => (
          <div key={t.id} className={`toast toast-${t.kind}`}>
            {t.text}
          </div>
        ))}
      </div>
    </div>
  );
}

function StatusBadge({ status, big }) {
  return <span className={`badge badge-${status} ${big ? 'badge-big' : ''}`}>{STATUS_LABEL[status] || status}</span>;
}

function Step({ name, result }) {
  const state = !result ? 'wait' : result.status === 'OK' ? 'ok' : 'error';
  const icon = state === 'wait' ? '…' : state === 'ok' ? '✓' : '✕';
  return (
    <div className={`step step-${state}`}>
      <span className="step-icon">{icon}</span>
      <div>
        <strong>{name}</strong>
        <div className="muted small">{result ? result.detail : 'esperando resultado'}</div>
      </div>
    </div>
  );
}
