import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import {
  ActivationRequestedPayload,
  BillingAccountCancelledPayload,
  BillingAccountCreatedPayload,
  BillingFailedPayload,
  EVENT_TYPES,
  EventEnvelope,
  TOPICS,
  createEvent,
} from '@poc/contracts';
import { EventConsumer, MONGO_DB, TxContext, randomBetween, shortId, sleep } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';
import { hostname } from 'os';

/**
 * CREATED          cuenta creada
 * CANCELLED        cuenta creada y después anulada (compensación)
 * REJECTED         billing falló (fallo simulado), no hay cuenta
 * NOT_CREATED      llegó ActivationFailed antes de ActivationRequested; si después se crea, se anula enseguida
 */
type AccountStatus = 'CREATED' | 'CANCELLED' | 'REJECTED' | 'NOT_CREATED';

interface BillingAccountDoc {
  activationId: string;
  customerId: string;
  planId?: string;
  billingAccountId?: string;
  status: AccountStatus;
  /** Ya se vio ActivationFailed para esta activación. */
  failureSeen: boolean;
  createdAt: Date;
  updatedAt: Date;
  cancelledAt?: Date;
  processedBy: string;
}

@Injectable()
export class BillingService implements OnModuleInit {
  private readonly logger = new Logger(BillingService.name);
  private readonly accounts: Collection<BillingAccountDoc>;
  private readonly instance = hostname();

  constructor(
    @Inject(MONGO_DB) db: Db,
    private readonly consumer: EventConsumer,
  ) {
    this.accounts = db.collection<BillingAccountDoc>('billing_accounts');
  }

  async onModuleInit(): Promise<void> {
    await this.accounts.createIndex({ activationId: 1 }, { unique: true });
  }

  consumeActivationRequested(ctx: KafkaContext) {
    return this.consumer.process(ctx, {
      before: () => sleep(randomBetween(200, 700)), // simula la llamada al sistema de facturación
      handle: (event, tx) => this.createAccount(event, tx),
    });
  }

  consumeActivationEvent(ctx: KafkaContext) {
    return this.consumer.process(ctx, {
      handle: (event, tx) => this.onActivationEvent(event, tx),
    });
  }

  private async createAccount(event: EventEnvelope<ActivationRequestedPayload>, tx: TxContext): Promise<void> {
    const activationId = event.correlationId;
    const { planId, simulateFailure } = event.payload;
    const existing = await this.accounts.findOne({ activationId }, { session: tx.session });

    if (existing && existing.status !== 'NOT_CREATED') {
      this.logger.warn(`[${activationId}] Ya hay una cuenta (${existing.status}); no se crea otra`);
      return;
    }

    if (simulateFailure === 'billing') {
      await this.upsert(tx, activationId, {
        customerId: event.customerId,
        planId,
        status: 'REJECTED',
        failureSeen: existing?.failureSeen ?? false,
      });
      await tx.publish(
        TOPICS.BILLING_EVENTS,
        createEvent<BillingFailedPayload>({
          eventType: EVENT_TYPES.BILLING_FAILED,
          correlationId: activationId,
          customerId: event.customerId,
          source: 'billing-service',
          payload: { reason: 'Fallo simulado: medio de pago rechazado' },
        }),
      );
      this.logger.warn(`[${activationId}] BillingFailed (fallo simulado) [${this.instance}]`);
      return;
    }

    const billingAccountId = shortId('BA');
    const failureAlreadySeen = existing?.failureSeen === true;

    await this.upsert(tx, activationId, {
      customerId: event.customerId,
      planId,
      billingAccountId,
      status: failureAlreadySeen ? 'CANCELLED' : 'CREATED',
      failureSeen: failureAlreadySeen,
      ...(failureAlreadySeen ? { cancelledAt: new Date() } : {}),
    });
    await tx.publish(
      TOPICS.BILLING_EVENTS,
      createEvent<BillingAccountCreatedPayload>({
        eventType: EVENT_TYPES.BILLING_ACCOUNT_CREATED,
        correlationId: activationId,
        customerId: event.customerId,
        source: 'billing-service',
        payload: { billingAccountId, planId },
      }),
    );
    this.logger.log(`[${activationId}] Cuenta ${billingAccountId} creada para ${event.customerId} [${this.instance}]`);

    // Caso borde: ActivationFailed llegó antes de que billing creara la cuenta. Se anula enseguida.
    if (failureAlreadySeen) {
      await this.publishCancelled(tx, event, billingAccountId, 'La activación ya había fallado al crear la cuenta');
    }
  }

  private async onActivationEvent(event: EventEnvelope<any>, tx: TxContext): Promise<void> {
    if (event.eventType !== EVENT_TYPES.ACTIVATION_FAILED) return; // ActivationCompleted: nada que hacer

    const activationId = event.correlationId;
    const existing = await this.accounts.findOne({ activationId }, { session: tx.session });

    if (!existing) {
      // Todavía no llegó (o no se procesó) ActivationRequested: se deja la marca para anular después.
      await this.upsert(tx, activationId, { customerId: event.customerId, status: 'NOT_CREATED', failureSeen: true });
      this.logger.warn(`[${activationId}] ActivationFailed antes de crear la cuenta: queda marcada para anular`);
      return;
    }

    if (existing.status === 'CREATED') {
      await this.upsert(tx, activationId, { status: 'CANCELLED', failureSeen: true, cancelledAt: new Date() });
      await this.publishCancelled(tx, event, existing.billingAccountId, `Compensación: ${event.payload?.reason ?? 'activación fallida'}`);
      return;
    }

    await this.upsert(tx, activationId, { failureSeen: true });
    this.logger.log(`[${activationId}] ActivationFailed: no hay cuenta para anular (estado ${existing.status})`);
  }

  private async publishCancelled(tx: TxContext, event: EventEnvelope<any>, billingAccountId: string, reason: string) {
    await tx.publish(
      TOPICS.BILLING_EVENTS,
      createEvent<BillingAccountCancelledPayload>({
        eventType: EVENT_TYPES.BILLING_ACCOUNT_CANCELLED,
        correlationId: event.correlationId,
        customerId: event.customerId,
        source: 'billing-service',
        payload: { billingAccountId, reason },
      }),
    );
    this.logger.warn(`[${event.correlationId}] Cuenta ${billingAccountId} ANULADA (compensación) [${this.instance}]`);
  }

  private async upsert(tx: TxContext, activationId: string, fields: Partial<BillingAccountDoc>): Promise<void> {
    const now = new Date();
    await this.accounts.updateOne(
      { activationId },
      {
        $set: { ...fields, updatedAt: now, processedBy: this.instance },
        $setOnInsert: { activationId, createdAt: now },
      },
      { upsert: true, session: tx.session },
    );
  }
}
