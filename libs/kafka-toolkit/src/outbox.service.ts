import { Inject, Injectable, Logger, OnApplicationBootstrap, OnModuleDestroy } from '@nestjs/common';
import { EventEnvelope } from '@poc/contracts';
import { ClientSession, Collection, Db } from 'mongodb';
import { hostname } from 'os';
import { EventPublisher } from './event-publisher.service';
import { MONGO_DB } from './tokens';

export interface OutboxDoc {
  _id: string; // = eventId
  topic: string;
  key: string;
  event: EventEnvelope<unknown>;
  status: 'PENDING' | 'PUBLISHING' | 'SENT';
  createdAt: Date;
  attempts: number;
  lockedAt?: Date;
  lockedBy?: string;
  sentAt?: Date;
  lastError?: string;
}

const POLL_INTERVAL_MS = 500;
/** Si una instancia tomó un evento y murió antes de publicarlo, otra lo retoma pasado este tiempo. */
const STALE_LOCK_MS = 10000;

/**
 * Outbox transaccional.
 *
 * El problema: si un servicio guarda en Mongo y se cae antes de publicar en Kafka, el evento se pierde.
 * La solución: el evento se guarda en la colección `outbox` en la MISMA transacción que el cambio de
 * estado. Un proceso aparte (este relay) lee la outbox cada 500 ms, publica los pendientes y los marca
 * como enviados. Además se dispara un flush inmediato después de cada transacción para no esperar.
 *
 * Garantía: al menos una vez. Si se cae entre publicar y marcar SENT, el evento se publica de nuevo,
 * y por eso todos los consumidores son idempotentes.
 */
@Injectable()
export class OutboxService implements OnApplicationBootstrap, OnModuleDestroy {
  private readonly logger = new Logger(OutboxService.name);
  private readonly collection: Collection<OutboxDoc>;
  private readonly instanceId = `${hostname()}-${process.pid}`;
  private timer: NodeJS.Timeout | undefined;
  private flushing = false;
  private flushRequested = false;

  constructor(
    @Inject(MONGO_DB) db: Db,
    private readonly publisher: EventPublisher,
  ) {
    this.collection = db.collection<OutboxDoc>('outbox');
  }

  async onApplicationBootstrap(): Promise<void> {
    await this.collection.createIndex({ status: 1, createdAt: 1 });
    this.timer = setInterval(() => {
      this.flush().catch((err) => this.logger.error(`Error en el relay de outbox: ${err.message}`));
    }, POLL_INTERVAL_MS);
    this.logger.log(`Relay de outbox iniciado (cada ${POLL_INTERVAL_MS} ms)`);
  }

  onModuleDestroy(): void {
    if (this.timer) clearInterval(this.timer);
  }

  /** Agrega un evento a la outbox dentro de la transacción en curso. */
  async add(session: ClientSession, topic: string, event: EventEnvelope<unknown>): Promise<void> {
    await this.collection.insertOne(
      {
        _id: event.eventId,
        topic,
        key: event.customerId,
        event,
        status: 'PENDING',
        createdAt: new Date(),
        attempts: 0,
      },
      { session },
    );
  }

  /** Publica todos los eventos pendientes, en orden de creación. */
  async flush(): Promise<void> {
    if (this.flushing) {
      this.flushRequested = true;
      return;
    }
    this.flushing = true;
    try {
      do {
        this.flushRequested = false;
        await this.drain();
      } while (this.flushRequested);
    } finally {
      this.flushing = false;
    }
  }

  private async drain(): Promise<void> {
    for (;;) {
      const now = Date.now();
      // Se "toma" un evento de forma atómica para que dos instancias no publiquen el mismo a la vez.
      const doc = await this.collection.findOneAndUpdate(
        {
          $or: [{ status: 'PENDING' }, { status: 'PUBLISHING', lockedAt: { $lt: new Date(now - STALE_LOCK_MS) } }],
        },
        { $set: { status: 'PUBLISHING', lockedAt: new Date(now), lockedBy: this.instanceId } },
        { sort: { createdAt: 1 }, returnDocument: 'after' },
      );
      if (!doc) return;

      try {
        await this.publisher.publish(doc.topic, doc.event);
        await this.collection.updateOne({ _id: doc._id }, { $set: { status: 'SENT', sentAt: new Date() } });
      } catch (err) {
        await this.collection.updateOne(
          { _id: doc._id },
          { $set: { status: 'PENDING', lastError: String(err?.message ?? err) }, $inc: { attempts: 1 } },
        );
        this.logger.warn(`No se pudo publicar ${doc.event.eventType} (${doc._id}); se reintenta: ${err?.message}`);
        return;
      }
    }
  }
}
