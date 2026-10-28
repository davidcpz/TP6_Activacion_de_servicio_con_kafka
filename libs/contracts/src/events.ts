/**
 * Contratos de eventos (v1). Única fuente de verdad: todos los servicios importan de acá.
 *
 * Reglas de evolución:
 *  - Los nombres de eventos están en pasado (describen un hecho).
 *  - Cambios compatibles: sólo se agregan campos opcionales (misma version).
 *  - Cambios incompatibles: se sube `version` y los consumidores deciden qué versiones aceptan.
 */
export const CONTRACTS_VERSION = '1.0.0';

export const EVENT_TYPES = {
  ACTIVATION_REQUESTED: 'ActivationRequested',
  BILLING_ACCOUNT_CREATED: 'BillingAccountCreated',
  BILLING_FAILED: 'BillingFailed',
  BILLING_ACCOUNT_CANCELLED: 'BillingAccountCancelled',
  PROVISIONING_COMPLETED: 'ProvisioningCompleted',
  PROVISIONING_FAILED: 'ProvisioningFailed',
  ACTIVATION_COMPLETED: 'ActivationCompleted',
  ACTIVATION_FAILED: 'ActivationFailed',
} as const;

export type EventType = (typeof EVENT_TYPES)[keyof typeof EVENT_TYPES];

export type SimulateFailure = 'none' | 'billing' | 'provisioning';
export const SIMULATE_FAILURE_VALUES: SimulateFailure[] = ['none', 'billing', 'provisioning'];

export type ServiceName =
  | 'activation-api'
  | 'billing-service'
  | 'provisioning-service'
  | 'notification-service'
  | 'crm-analytics-service'
  | 'loyalty-service';

/** Sobre común a todos los eventos. El payload cambia según eventType. */
export interface EventEnvelope<TPayload = Record<string, unknown>> {
  /** Identificador único: base de la idempotencia. */
  eventId: string;
  eventType: EventType;
  /** Versión del contrato de este eventType. */
  version: number;
  /** ISO-8601 */
  occurredAt: string;
  /** = activationId. Agrupa todos los eventos de una activación. */
  correlationId: string;
  /** También es la clave del mensaje en Kafka: garantiza orden por cliente. */
  customerId: string;
  /** Servicio que publicó el evento. */
  source: ServiceName;
  payload: TPayload;
}

// ------------------------------------------------------------------ payloads v1

export interface ActivationRequestedPayload {
  planId: string;
  channel: string;
  /** Sólo para la demo: indica a billing o provisioning que deben fallar. */
  simulateFailure: SimulateFailure;
}

export interface BillingAccountCreatedPayload {
  billingAccountId: string;
  planId: string;
}

export interface BillingFailedPayload {
  reason: string;
}

export interface BillingAccountCancelledPayload {
  billingAccountId: string;
  reason: string;
}

export interface ProvisioningCompletedPayload {
  orderId: string;
  lineNumber: string;
}

export interface ProvisioningFailedPayload {
  reason: string;
}

export interface ActivationCompletedPayload {
  planId: string;
}

export interface ActivationFailedPayload {
  planId: string;
  failedStep: 'billing' | 'provisioning';
  reason: string;
}

/** Versión actual de cada evento (todas v1 en la POC). */
export const EVENT_VERSIONS: Record<EventType, number> = {
  ActivationRequested: 1,
  BillingAccountCreated: 1,
  BillingFailed: 1,
  BillingAccountCancelled: 1,
  ProvisioningCompleted: 1,
  ProvisioningFailed: 1,
  ActivationCompleted: 1,
  ActivationFailed: 1,
};
