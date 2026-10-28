import { randomUUID } from 'crypto';
import { EVENT_TYPES, EVENT_VERSIONS, EventEnvelope, EventType, ServiceName } from './events';

export interface CreateEventParams<T> {
  eventType: EventType;
  correlationId: string;
  customerId: string;
  source: ServiceName;
  payload: T;
}

/** Construye un evento con el sobre común (eventId nuevo, versión vigente, fecha actual). */
export function createEvent<T>(params: CreateEventParams<T>): EventEnvelope<T> {
  return {
    eventId: randomUUID(),
    eventType: params.eventType,
    version: EVENT_VERSIONS[params.eventType] ?? 1,
    occurredAt: new Date().toISOString(),
    correlationId: params.correlationId,
    customerId: params.customerId,
    source: params.source,
    payload: params.payload,
  };
}

export class InvalidEventError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'InvalidEventError';
  }
}

const KNOWN_TYPES = new Set<string>(Object.values(EVENT_TYPES));

/**
 * Valida que un mensaje recibido de Kafka cumpla el sobre común.
 * Acepta el valor ya parseado (objeto), un string JSON o un Buffer.
 * Lanza InvalidEventError si no es un evento válido (termina en la DLQ).
 */
export function parseEnvelope(raw: unknown): EventEnvelope<any> {
  let value: any = raw;
  if (Buffer.isBuffer(value)) value = value.toString('utf8');
  if (typeof value === 'string') {
    try {
      value = JSON.parse(value);
    } catch {
      throw new InvalidEventError('El mensaje no es JSON válido');
    }
  }
  if (value === null || typeof value !== 'object' || Array.isArray(value)) {
    throw new InvalidEventError('El mensaje no es un objeto');
  }
  const requiredStrings = ['eventId', 'eventType', 'occurredAt', 'correlationId', 'customerId', 'source'];
  for (const field of requiredStrings) {
    if (typeof value[field] !== 'string' || value[field].length === 0) {
      throw new InvalidEventError(`Falta el campo obligatorio "${field}"`);
    }
  }
  if (typeof value.version !== 'number') {
    throw new InvalidEventError('Falta el campo obligatorio "version"');
  }
  if (!KNOWN_TYPES.has(value.eventType)) {
    throw new InvalidEventError(`eventType desconocido: ${value.eventType}`);
  }
  if (value.payload === null || typeof value.payload !== 'object') {
    throw new InvalidEventError('Falta el payload');
  }
  return value as EventEnvelope<any>;
}

/** Headers que acompañan a cada mensaje (útiles para filtrar en Kafka UI). */
export function eventHeaders(event: EventEnvelope<unknown>): Record<string, string> {
  return {
    eventId: event.eventId,
    eventType: event.eventType,
    version: String(event.version),
    correlationId: event.correlationId,
    source: event.source,
  };
}
