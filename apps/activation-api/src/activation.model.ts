import { ActivationRequestedPayload, EventEnvelope, SimulateFailure } from '@poc/contracts';

export type ActivationStatus = 'PENDING' | 'IN_PROGRESS' | 'ACTIVE' | 'FAILED';

export interface StepResult {
  status: 'OK' | 'FAILED';
  at: string;
  detail?: string;
}

export interface HistoryEntry {
  eventId: string;
  eventType: string;
  source: string;
  at: string;
  detail?: string;
}

/** Documento de la colección `activations` (estado de la saga). */
export interface ActivationDoc {
  _id: string; // = activationId = correlationId
  customerId: string;
  planId: string;
  channel: string;
  simulateFailure: SimulateFailure;
  status: ActivationStatus;
  steps: {
    billing: StepResult | null;
    provisioning: StepResult | null;
  };
  history: HistoryEntry[];
  failureReason?: string;
  /** Evento original: lo usa la demo para reenviarlo y probar la idempotencia. */
  requestEvent: EventEnvelope<ActivationRequestedPayload>;
  createdAt: Date;
  updatedAt: Date;
}

export type ActivationView = Omit<ActivationDoc, 'requestEvent'> & { activationId: string };

export function toView(doc: ActivationDoc): ActivationView {
  const { requestEvent, ...rest } = doc;
  return { activationId: doc._id, ...rest };
}

export function toHistory(event: EventEnvelope<any>): HistoryEntry {
  return {
    eventId: event.eventId,
    eventType: event.eventType,
    source: event.source,
    at: event.occurredAt,
    detail: describe(event),
  };
}

function describe(event: EventEnvelope<any>): string | undefined {
  const p = event.payload ?? {};
  switch (event.eventType) {
    case 'ActivationRequested':
      return `${p.planId} · fallo simulado: ${p.simulateFailure}`;
    case 'BillingAccountCreated':
      return `Cuenta ${p.billingAccountId}`;
    case 'BillingAccountCancelled':
      return `Cuenta ${p.billingAccountId} anulada: ${p.reason}`;
    case 'ProvisioningCompleted':
      return `Línea ${p.lineNumber} (orden ${p.orderId})`;
    case 'BillingFailed':
    case 'ProvisioningFailed':
      return p.reason;
    case 'ActivationCompleted':
      return `${p.planId} activo`;
    case 'ActivationFailed':
      return `Falló ${p.failedStep}: ${p.reason}`;
    default:
      return undefined;
  }
}
