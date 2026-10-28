import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ClientKafka } from '@nestjs/microservices';
import { EventEnvelope, eventHeaders } from '@poc/contracts';
import { lastValueFrom } from 'rxjs';
import { KAFKA_PRODUCER } from './tokens';

/** Publica en Kafka con ClientKafka. La clave del mensaje es siempre el customerId. */
@Injectable()
export class EventPublisher implements OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(EventPublisher.name);

  constructor(@Inject(KAFKA_PRODUCER) private readonly client: ClientKafka) {}

  async onModuleInit(): Promise<void> {
    await this.client.connect();
    this.logger.log('Productor Kafka conectado');
  }

  async onApplicationShutdown(): Promise<void> {
    await this.client.close();
  }

  async publish(topic: string, event: EventEnvelope<unknown>): Promise<void> {
    await this.send(topic, event.customerId, JSON.stringify(event), eventHeaders(event));
    this.logger.log(`[${event.correlationId}] → ${event.eventType} publicado en ${topic}`);
  }

  /** Envío de bajo nivel (lo usa también la DLQ para reenviar el mensaje original tal cual). */
  async send(topic: string, key: string | null, value: string, headers: Record<string, string> = {}): Promise<void> {
    await lastValueFrom(this.client.emit(topic, { key, value, headers }), { defaultValue: undefined });
  }
}
