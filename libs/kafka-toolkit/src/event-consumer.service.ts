import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { EventEnvelope, dlqTopic, parseEnvelope } from '@poc/contracts';
import { ClientSession, Collection, Db, MongoClient } from 'mongodb';
import { ServiceConfig } from './config';
import { EventPublisher } from './event-publisher.service';
import { OutboxService } from './outbox.service';
import { MONGO_CLIENT, MONGO_DB, TOOLKIT_CONFIG } from './tokens';

/** Esperas entre reintentos: 1 s, 2 s, 4 s. Después del último, el mensaje va a la DLQ. */
export const RETRY_DELAYS_MS = [1000, 2000, 4000];
const MAX_ATTEMPTS = RETRY_DELAYS_MS.length + 1;
const PROCESSED_TTL_SECONDS = 7 * 24 * 60 * 60; // = retención de los topics

export interface ProcessedEventDoc {
  _id: string; // = eventId
  eventType: string;
  correlationId: string;
  topic: string;
  processedAt: Date;
}

/** Lo que recibe el handler de negocio: la sesión de la transacción y una forma de publicar. */
export interface TxContext {
  session: ClientSession;
  db: Db;
  /** Publica un evento vía outbox, dentro de la misma transacción que el efecto. */
  publish: (topic: string, event: EventEnvelope<unknown>) => Promise<void>;
}

export interface ProcessOptions {
  /** Efecto de negocio. Todo lo que escriba en Mongo debe usar tx.session. */
  handle: (event: EventEnvelope<any>, tx: TxContext) => Promise<void>;
  /** Trabajo previo fuera de la transacción (p. ej. simular una demora). */
  before?: (event: EventEnvelope<any>) => Promise<void>;
  /** Se ejecuta después del commit (p. ej. avisar por WebSocket). */
  after?: (event: EventEnvelope<any>) => Promise<void>;
}

class DuplicateEventError extends Error {}

/**
 * Procesa un mensaje de Kafka de forma confiable:
 *
 * 1. Idempotencia: inserta el eventId en `processed_events` (índice único) en la MISMA transacción
 *    que el efecto. Si ya existía, descarta el evento y confirma el offset.
 * 2. Reintentos: ante un error técnico reintenta 3 veces con espera creciente (1 s, 2 s, 4 s).
 * 3. DLQ: si sigue fallando, publica el mensaje original en <topic>.dlq con el motivo en los headers
 *    y sigue con el siguiente. Un mensaje roto no frena al resto de la partición.
 * 4. Commit manual del offset, siempre DESPUÉS de procesar (nunca antes).
 */
@Injectable()
export class EventConsumer implements OnModuleInit {
  private readonly logger = new Logger(EventConsumer.name);
  private readonly processed: Collection<ProcessedEventDoc>;

  constructor(
    @Inject(MONGO_CLIENT) private readonly mongo: MongoClient,
    @Inject(MONGO_DB) private readonly db: Db,
    @Inject(TOOLKIT_CONFIG) private readonly config: ServiceConfig,
    private readonly publisher: EventPublisher,
    private readonly outbox: OutboxService,
  ) {
    this.processed = db.collection<ProcessedEventDoc>('processed_events');
  }

  async onModuleInit(): Promise<void> {
    // _id = eventId ya es único. El TTL coincide con la retención de los topics:
    // un evento más viejo que eso ya no puede volver a llegar.
    await this.processed.createIndex({ processedAt: 1 }, { expireAfterSeconds: PROCESSED_TTL_SECONDS });
  }

  async process(ctx: KafkaContext, options: ProcessOptions): Promise<void> {
    const message = ctx.getMessage();
    const topic = ctx.getTopic();
    const partition = ctx.getPartition();
    const where = `${topic}[p${partition}]@${message.offset}`;

    let lastError: Error | undefined;
    let attempts = 0;
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
      attempts = attempt;
      try {
        await this.processOnce(message.value, topic, options);
        lastError = undefined;
        break;
      } catch (err) {
        lastError = err instanceof Error ? err : new Error(String(err));
        const delay = RETRY_DELAYS_MS[attempt - 1];
        if (delay === undefined) break;
        this.logger.warn(
          `Intento ${attempt}/${MAX_ATTEMPTS} falló para ${where}: ${lastError.message}. Reintento en ${delay / 1000} s`,
        );
        await this.sleepWithHeartbeat(ctx, delay);
      }
    }

    if (lastError) {
      await this.sendToDlq(ctx, lastError, attempts);
    }
    await this.commit(ctx);
  }

  private async processOnce(rawValue: unknown, topic: string, options: ProcessOptions): Promise<void> {
    const event = parseEnvelope(rawValue);

    // Atajo: si ya está procesado, ni siquiera se ejecuta el trabajo previo.
    if (await this.processed.findOne({ _id: event.eventId })) {
      this.logDuplicate(event);
      return;
    }

    if (options.before) await options.before(event);

    let published = 0;
    const session = this.mongo.startSession();
    try {
      await session.withTransaction(async () => {
        try {
          await this.processed.insertOne(
            {
              _id: event.eventId,
              eventType: event.eventType,
              correlationId: event.correlationId,
              topic,
              processedAt: new Date(),
            },
            { session },
          );
        } catch (err) {
          if (err?.code === 11000) throw new DuplicateEventError();
          throw err;
        }
        await options.handle(event, {
          session,
          db: this.db,
          publish: async (targetTopic, outgoing) => {
            await this.outbox.add(session, targetTopic, outgoing);
            published++;
          },
        });
      });
    } catch (err) {
      if (err instanceof DuplicateEventError) {
        this.logDuplicate(event);
        return;
      }
      throw err;
    } finally {
      await session.endSession();
    }

    if (published > 0) await this.outbox.flush();

    if (options.after) {
      try {
        await options.after(event);
      } catch (err) {
        this.logger.warn(`Error en post-proceso de ${event.eventType}: ${err?.message}`);
      }
    }
  }

  private logDuplicate(event: EventEnvelope<unknown>): void {
    this.logger.warn(
      `[${event.correlationId}] ${event.eventType} ${event.eventId} ya fue procesado: se descarta (idempotencia)`,
    );
  }

  private async sendToDlq(ctx: KafkaContext, error: Error, attempts: number): Promise<void> {
    const message = ctx.getMessage();
    const topic = ctx.getTopic();
    const partition = ctx.getPartition();
    const target = dlqTopic(topic);
    const headers: Record<string, string> = {
      ...stringifyHeaders(message.headers),
      'dlq-reason': error.message.slice(0, 500),
      'dlq-error-type': error.name,
      'dlq-original-topic': topic,
      'dlq-original-partition': String(partition),
      'dlq-original-offset': String(message.offset),
      'dlq-consumer-group': this.config.groupId,
      'dlq-attempts': String(attempts),
      'dlq-failed-at': new Date().toISOString(),
    };
    await this.publisher.send(target, toText(message.key) || null, toText(message.value), headers);
    this.logger.error(
      `Mensaje ${topic}[p${partition}]@${message.offset} enviado a ${target} después de ${attempts} intentos: ${error.message}`,
    );
  }

  private async commit(ctx: KafkaContext): Promise<void> {
    const message = ctx.getMessage();
    await ctx.getConsumer().commitOffsets([
      { topic: ctx.getTopic(), partition: ctx.getPartition(), offset: (BigInt(message.offset) + 1n).toString() },
    ]);
  }

  private async sleepWithHeartbeat(ctx: KafkaContext, ms: number): Promise<void> {
    const heartbeat = ctx.getHeartbeat?.();
    let remaining = ms;
    while (remaining > 0) {
      const step = Math.min(1000, remaining);
      await new Promise((resolve) => setTimeout(resolve, step));
      remaining -= step;
      try {
        if (heartbeat) await heartbeat();
      } catch {
        // el heartbeat puede fallar durante un rebalanceo; no es crítico
      }
    }
  }
}

function toText(value: unknown): string {
  if (value === null || value === undefined) return '';
  if (Buffer.isBuffer(value)) return value.toString('utf8');
  if (typeof value === 'string') return value;
  return JSON.stringify(value);
}

function stringifyHeaders(headers: unknown): Record<string, string> {
  const result: Record<string, string> = {};
  if (!headers || typeof headers !== 'object') return result;
  for (const [key, value] of Object.entries(headers as Record<string, unknown>)) {
    if (value === undefined) continue;
    result[key] = Array.isArray(value) ? value.map(toText).join(',') : toText(value);
  }
  return result;
}
