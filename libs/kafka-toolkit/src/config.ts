import { ServiceName } from '@poc/contracts';

export interface ServiceConfig {
  serviceName: ServiceName;
  brokers: string[];
  clientId: string;
  groupId: string;
  mongoUri: string;
  mongoDb: string;
}

export function env(name: string, fallback?: string): string {
  const value = process.env[name];
  if (value === undefined || value === '') {
    if (fallback !== undefined) return fallback;
    throw new Error(`Falta la variable de entorno ${name}`);
  }
  return value;
}

/**
 * Configuración común de cada servicio a partir de variables de entorno.
 * Los valores por defecto sirven para correr un servicio fuera de Docker
 * (con la infraestructura levantada por docker compose).
 */
export function loadServiceConfig(
  serviceName: ServiceName,
  defaults: { groupId: string; mongoDb: string },
): ServiceConfig {
  return {
    serviceName,
    brokers: env('KAFKA_BROKERS', 'localhost:9092')
      .split(',')
      .map((b) => b.trim())
      .filter(Boolean),
    clientId: env('KAFKA_CLIENT_ID', serviceName),
    groupId: env('KAFKA_GROUP_ID', defaults.groupId),
    mongoUri: env('MONGO_URI', 'mongodb://localhost:27017/?replicaSet=rs0&directConnection=true'),
    mongoDb: env('MONGO_DB', defaults.mongoDb),
  };
}
