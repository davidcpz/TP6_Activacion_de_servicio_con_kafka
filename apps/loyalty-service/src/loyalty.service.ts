import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { EVENT_TYPES } from '@poc/contracts';
import { EventConsumer, MONGO_DB } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';

interface LoyaltyAccountDoc {
  _id: string; // = customerId
  points: number;
  activations: string[];
  updatedAt: Date;
}

const POINTS_PER_ACTIVATION = 100;

/**
 * Escenario 5 (replay del log): servicio nuevo que se suma DESPUÉS de que pasaron activaciones.
 * Como su consumer group es nuevo y usa auto.offset.reset=earliest (fromBeginning), lee
 * activation.events desde el inicio y otorga puntos por cada activación completada en el pasado,
 * sin tocar a ningún otro servicio.
 */
@Injectable()
export class LoyaltyService implements OnModuleInit {
  private readonly logger = new Logger(LoyaltyService.name);
  private readonly accounts: Collection<LoyaltyAccountDoc>;

  constructor(
    @Inject(MONGO_DB) db: Db,
    private readonly consumer: EventConsumer,
  ) {
    this.accounts = db.collection<LoyaltyAccountDoc>('loyalty_accounts');
  }

  async onModuleInit(): Promise<void> {
    // Crea la colección al arrancar. Si se creara dentro de transacciones concurrentes,
    // Mongo abortaría alguna por conflicto (se reintenta sola, pero ensucia los logs).
    await this.accounts.createIndex({ updatedAt: -1 });
  }

  consume(ctx: KafkaContext) {
    // El resultado se guarda acá y se loguea DESPUÉS del commit: si Mongo reintenta la
    // transacción, el log no sale duplicado.
    const awarded = new Map<string, number>();
    return this.consumer.process(ctx, {
      handle: async (event, tx) => {
        if (event.eventType !== EVENT_TYPES.ACTIVATION_COMPLETED) return;
        const result = await this.accounts.findOneAndUpdate(
          { _id: event.customerId },
          {
            $inc: { points: POINTS_PER_ACTIVATION },
            $push: { activations: event.correlationId },
            $set: { updatedAt: new Date() },
          },
          { upsert: true, returnDocument: 'after', session: tx.session },
        );
        awarded.set(event.eventId, result?.points ?? 0);
      },
      after: async (event) => {
        if (!awarded.has(event.eventId)) return;
        this.logger.log(
          `[${event.correlationId}] ${event.customerId} suma ${POINTS_PER_ACTIVATION} puntos ` +
            `por ${event.payload?.planId} (total ${awarded.get(event.eventId)}) · evento del ${event.occurredAt}`,
        );
      },
    });
  }
}
