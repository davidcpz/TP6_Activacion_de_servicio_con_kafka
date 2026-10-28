import { KafkaOptions, Transport } from '@nestjs/microservices';
import { ServiceConfig } from './config';

/**
 * Opciones del consumidor Kafka de Nest (@EventPattern).
 *
 * - groupId sin sufijo (postfixId: ''): el consumer group se llama igual que en el documento.
 * - autoCommit: false -> el offset se confirma a mano DESPUÉS de procesar (ver EventConsumer).
 * - fromBeginning: true -> un consumer group nuevo lee el historial desde el inicio
 *   (auto.offset.reset=earliest). Si el grupo ya tiene offsets, sigue desde ahí.
 * - partitionsConsumedConcurrently: 3 -> procesa las 3 particiones en paralelo;
 *   dentro de cada partición el orden se mantiene.
 */
export function kafkaServerOptions(config: ServiceConfig): KafkaOptions {
  return {
    transport: Transport.KAFKA,
    options: {
      postfixId: '',
      client: {
        clientId: config.clientId,
        brokers: config.brokers,
        retry: { initialRetryTime: 300, retries: 15 },
      },
      consumer: {
        groupId: config.groupId,
        sessionTimeout: 10000,
        heartbeatInterval: 3000,
        rebalanceTimeout: 15000,
        allowAutoTopicCreation: false,
      },
      subscribe: { fromBeginning: true },
      run: { autoCommit: false, partitionsConsumedConcurrently: 3 },
    },
  } as KafkaOptions;
}
