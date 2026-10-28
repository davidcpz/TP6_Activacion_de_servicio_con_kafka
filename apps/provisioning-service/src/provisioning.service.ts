import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import {
  ActivationRequestedPayload,
  EVENT_TYPES,
  EventEnvelope,
  ProvisioningCompletedPayload,
  ProvisioningFailedPayload,
  TOPICS,
  createEvent,
} from '@poc/contracts';
import { EventConsumer, MONGO_DB, TxContext, randomBetween, shortId, sleep } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';

interface ProvisioningOrderDoc {
  activationId: string;
  customerId: string;
  planId: string;
  orderId: string;
  lineNumber?: string;
  status: 'COMPLETED' | 'FAILED';
  reason?: string;
  durationMs: number;
  createdAt: Date;
}

@Injectable()
export class ProvisioningService implements OnModuleInit {
  private readonly logger = new Logger(ProvisioningService.name);
  private readonly orders: Collection<ProvisioningOrderDoc>;
  /** Demora simulada de cada activación (se guarda para registrarla en la orden). */
  private readonly delays = new Map<string, number>();

  constructor(
    @Inject(MONGO_DB) db: Db,
    private readonly consumer: EventConsumer,
  ) {
    this.orders = db.collection<ProvisioningOrderDoc>('provisioning_orders');
  }

  async onModuleInit(): Promise<void> {
    await this.orders.createIndex({ activationId: 1 }, { unique: true });
  }

  consumeActivationRequested(ctx: KafkaContext) {
    return this.consumer.process(ctx, {
      // La demora de 1 a 3 s ocurre fuera de la transacción para no mantenerla abierta.
      before: async (event) => {
        const ms = randomBetween(1000, 3000);
        this.delays.set(event.eventId, ms);
        this.logger.log(`[${event.correlationId}] Aprovisionando línea... (${ms} ms)`);
        await sleep(ms);
      },
      handle: (event, tx) => this.provision(event, tx),
    });
  }

  private async provision(event: EventEnvelope<ActivationRequestedPayload>, tx: TxContext): Promise<void> {
    const activationId = event.correlationId;
    const existing = await this.orders.findOne({ activationId }, { session: tx.session });
    if (existing) {
      this.logger.warn(`[${activationId}] Ya existe la orden ${existing.orderId}; no se aprovisiona de nuevo`);
      return;
    }

    const durationMs = this.delays.get(event.eventId) ?? 0;
    this.delays.delete(event.eventId);
    const orderId = shortId('PO');

    if (event.payload.simulateFailure === 'provisioning') {
      const reason = 'Fallo simulado: sin cobertura en la zona del cliente';
      await this.orders.insertOne(
        {
          activationId,
          customerId: event.customerId,
          planId: event.payload.planId,
          orderId,
          status: 'FAILED',
          reason,
          durationMs,
          createdAt: new Date(),
        },
        { session: tx.session },
      );
      await tx.publish(
        TOPICS.PROVISIONING_EVENTS,
        createEvent<ProvisioningFailedPayload>({
          eventType: EVENT_TYPES.PROVISIONING_FAILED,
          correlationId: activationId,
          customerId: event.customerId,
          source: 'provisioning-service',
          payload: { reason },
        }),
      );
      this.logger.warn(`[${activationId}] ProvisioningFailed (fallo simulado)`);
      return;
    }

    const lineNumber = `11-${randomBetween(4000, 6999)}-${randomBetween(1000, 9999)}`;
    await this.orders.insertOne(
      {
        activationId,
        customerId: event.customerId,
        planId: event.payload.planId,
        orderId,
        lineNumber,
        status: 'COMPLETED',
        durationMs,
        createdAt: new Date(),
      },
      { session: tx.session },
    );
    await tx.publish(
      TOPICS.PROVISIONING_EVENTS,
      createEvent<ProvisioningCompletedPayload>({
        eventType: EVENT_TYPES.PROVISIONING_COMPLETED,
        correlationId: activationId,
        customerId: event.customerId,
        source: 'provisioning-service',
        payload: { orderId, lineNumber },
      }),
    );
    this.logger.log(`[${activationId}] Línea ${lineNumber} aprovisionada (orden ${orderId})`);
  }
}
