/**
 * Nombres de topics. Convención: dominio.tipo
 * Todos tienen 3 particiones, retención de 7 días y customerId como clave.
 */
export const TOPICS = {
  ACTIVATION_REQUESTED: 'activation.requested',
  BILLING_EVENTS: 'billing.events',
  PROVISIONING_EVENTS: 'provisioning.events',
  ACTIVATION_EVENTS: 'activation.events',
} as const;

export type Topic = (typeof TOPICS)[keyof typeof TOPICS];

export const ALL_TOPICS: Topic[] = Object.values(TOPICS);

/** Cada topic tiene su Dead Letter Queue: <topic>.dlq */
export const dlqTopic = (topic: string): string => `${topic}.dlq`;
