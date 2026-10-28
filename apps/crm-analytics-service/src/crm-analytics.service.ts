import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { EventConsumer, MONGO_DB } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';

interface EventLogDoc {
  _id: string; // = eventId
  eventType: string;
  version: number;
  correlationId: string;
  customerId: string;
  source: string;
  occurredAt: string;
  topic: string;
  partition: number;
  offset: string;
  payload: unknown;
  receivedAt: Date;
}

const STATS_INTERVAL_MS = 15000;

/**
 * RF-10: registra una copia de cada evento (colección event_log).
 * Para reprocesar el historial desde el inicio alcanza con levantarlo con otro consumer group
 * y otra base (ver docs/demo-script.md): como el log de Kafka se conserva 7 días, lo reconstruye entero.
 */
@Injectable()
export class CrmAnalyticsService implements OnModuleInit, OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(CrmAnalyticsService.name);
  private readonly eventLog: Collection<EventLogDoc>;
  private timer: NodeJS.Timeout | undefined;
  private lastTotal = -1;

  constructor(
    @Inject(MONGO_DB) db: Db,
    private readonly consumer: EventConsumer,
  ) {
    this.eventLog = db.collection<EventLogDoc>('event_log');
  }

  async onModuleInit(): Promise<void> {
    await this.eventLog.createIndex({ correlationId: 1 });
    await this.eventLog.createIndex({ eventType: 1 });
  }

  onApplicationBootstrap(): void {
    this.timer = setInterval(() => this.logStats().catch(() => undefined), STATS_INTERVAL_MS);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  record(ctx: KafkaContext) {
    const message = ctx.getMessage();
    const topic = ctx.getTopic();
    const partition = ctx.getPartition();
    return this.consumer.process(ctx, {
      handle: async (event, tx) => {
        await this.eventLog.insertOne(
          {
            _id: event.eventId,
            eventType: event.eventType,
            version: event.version,
            correlationId: event.correlationId,
            customerId: event.customerId,
            source: event.source,
            occurredAt: event.occurredAt,
            topic,
            partition,
            offset: String(message.offset),
            payload: event.payload,
            receivedAt: new Date(),
          },
          { session: tx.session },
        );
        this.logger.log(`[${event.correlationId}] ${event.eventType} registrado (${topic} p${partition} @${message.offset})`);
      },
    });
  }

  /** Resumen periódico: cuántos eventos de cada tipo se registraron. */
  private async logStats(): Promise<void> {
    const rows = await this.eventLog
      .aggregate<{ _id: string; count: number }>([{ $group: { _id: '$eventType', count: { $sum: 1 } } }, { $sort: { _id: 1 } }])
      .toArray();
    const total = rows.reduce((acc, r) => acc + r.count, 0);
    if (total === this.lastTotal) return;
    this.lastTotal = total;
    const summary = rows.map((r) => `${r._id}=${r.count}`).join(', ');
    this.logger.log(`Resumen CRM: ${total} eventos · ${summary || 'sin eventos'}`);
  }
}
