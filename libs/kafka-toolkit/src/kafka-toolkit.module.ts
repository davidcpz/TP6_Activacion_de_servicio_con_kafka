import { DynamicModule, Global, Inject, Injectable, Logger, Module, OnApplicationShutdown, Provider } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { MongoClient } from 'mongodb';
import { ServiceConfig } from './config';
import { EventConsumer } from './event-consumer.service';
import { EventPublisher } from './event-publisher.service';
import { OutboxService } from './outbox.service';
import { KAFKA_PRODUCER, MONGO_CLIENT, MONGO_DB, TOOLKIT_CONFIG } from './tokens';

@Injectable()
class MongoLifecycle implements OnApplicationShutdown {
  constructor(@Inject(MONGO_CLIENT) private readonly client: MongoClient) {}

  async onApplicationShutdown(): Promise<void> {
    await this.client.close();
  }
}

/**
 * Módulo compartido por todos los servicios: conexión a Mongo, productor Kafka,
 * outbox y consumidor idempotente con reintentos y DLQ.
 * Aísla el cliente Kafka para poder reemplazar kafkajs sin tocar los servicios.
 */
@Global()
@Module({})
export class KafkaToolkitModule {
  static forRoot(config: ServiceConfig): DynamicModule {
    const providers: Provider[] = [
      { provide: TOOLKIT_CONFIG, useValue: config },
      {
        provide: MONGO_CLIENT,
        useFactory: async () => {
          const logger = new Logger('Mongo');
          const client = new MongoClient(config.mongoUri, { serverSelectionTimeoutMS: 30000 });
          await client.connect();
          logger.log(`Conectado a MongoDB (base ${config.mongoDb})`);
          return client;
        },
      },
      {
        provide: MONGO_DB,
        inject: [MONGO_CLIENT],
        useFactory: (client: MongoClient) => client.db(config.mongoDb),
      },
      {
        provide: KAFKA_PRODUCER,
        useFactory: () =>
          new ClientKafka({
            client: {
              clientId: `${config.clientId}-producer`,
              brokers: config.brokers,
              retry: { initialRetryTime: 300, retries: 15 },
            },
            producerOnlyMode: true,
            producer: { allowAutoTopicCreation: false },
          } as any),
      },
      MongoLifecycle,
      EventPublisher,
      OutboxService,
      EventConsumer,
    ];

    return {
      module: KafkaToolkitModule,
      global: true,
      providers,
      exports: [TOOLKIT_CONFIG, MONGO_CLIENT, MONGO_DB, KAFKA_PRODUCER, EventPublisher, OutboxService, EventConsumer],
    };
  }
}
