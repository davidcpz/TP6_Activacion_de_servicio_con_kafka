import { BadRequestException, Inject, Injectable, Logger, NotFoundException, OnModuleInit } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import {
  ActivationCompletedPayload,
  ActivationFailedPayload,
  ActivationRequestedPayload,
  EVENT_TYPES,
  EventEnvelope,
  SIMULATE_FAILURE_VALUES,
  SimulateFailure,
  TOPICS,
  createEvent,
} from '@poc/contracts';
import { EventConsumer, MONGO_CLIENT, MONGO_DB, OutboxService, TxContext, shortId } from '@poc/kafka-toolkit';
import { Collection, Db, MongoClient } from 'mongodb';
import { ActivationDoc, ActivationView, HistoryEntry, StepResult, toHistory, toView } from './activation.model';
import { ActivationsGateway } from './activations.gateway';

export interface CreateActivationDto {
  customerId?: string;
  planId?: string;
  simulateFailure?: string;
}

type Step = 'billing' | 'provisioning';

/** Qué paso de la saga resuelve cada evento de resultado. */
const RESULT_EVENTS: Record<string, { step: Step; status: StepResult['status'] }> = {
  [EVENT_TYPES.BILLING_ACCOUNT_CREATED]: { step: 'billing', status: 'OK' },
  [EVENT_TYPES.BILLING_FAILED]: { step: 'billing', status: 'FAILED' },
  [EVENT_TYPES.PROVISIONING_COMPLETED]: { step: 'provisioning', status: 'OK' },
  [EVENT_TYPES.PROVISIONING_FAILED]: { step: 'provisioning', status: 'FAILED' },
};

@Injectable()
export class ActivationsService implements OnModuleInit {
  private readonly logger = new Logger(ActivationsService.name);
  private readonly activations: Collection<ActivationDoc>;

  constructor(
    @Inject(MONGO_CLIENT) private readonly mongo: MongoClient,
    @Inject(MONGO_DB) db: Db,
    private readonly outbox: OutboxService,
    private readonly consumer: EventConsumer,
    private readonly gateway: ActivationsGateway,
  ) {
    this.activations = db.collection<ActivationDoc>('activations');
  }

  async onModuleInit(): Promise<void> {
    await this.activations.createIndex({ customerId: 1 });
    await this.activations.createIndex({ createdAt: -1 });
  }

  // ------------------------------------------------------------------ REST

  /**
   * Crea la activación (PENDING) y el evento ActivationRequested en la misma transacción (outbox).
   * Responde enseguida: no espera a billing ni a provisioning.
   */
  async create(dto: CreateActivationDto): Promise<ActivationView> {
    const customerId = String(dto?.customerId ?? '').trim();
    const planId = String(dto?.planId ?? '').trim();
    const simulateFailure = (String(dto?.simulateFailure ?? 'none').trim() || 'none') as SimulateFailure;

    if (!customerId) throw new BadRequestException('customerId es obligatorio');
    if (!planId) throw new BadRequestException('planId es obligatorio');
    if (!SIMULATE_FAILURE_VALUES.includes(simulateFailure)) {
      throw new BadRequestException(`simulateFailure debe ser uno de: ${SIMULATE_FAILURE_VALUES.join(', ')}`);
    }

    const activationId = shortId('act');
    const event = createEvent<ActivationRequestedPayload>({
      eventType: EVENT_TYPES.ACTIVATION_REQUESTED,
      correlationId: activationId,
      customerId,
      source: 'activation-api',
      payload: { planId, channel: 'web', simulateFailure },
    });
    const now = new Date();
    const doc: ActivationDoc = {
      _id: activationId,
      customerId,
      planId,
      channel: 'web',
      simulateFailure,
      status: 'PENDING',
      steps: { billing: null, provisioning: null },
      history: [toHistory(event)],
      requestEvent: event,
      createdAt: now,
      updatedAt: now,
    };

    const session = this.mongo.startSession();
    try {
      await session.withTransaction(async () => {
        await this.activations.insertOne(doc, { session });
        await this.outbox.add(session, TOPICS.ACTIVATION_REQUESTED, event);
      });
    } finally {
      await session.endSession();
    }
    this.logger.log(`[${activationId}] Activación creada para ${customerId} (${planId}, fallo: ${simulateFailure})`);

    // Publicación inmediata (sin bloquear la respuesta). Si falla, el relay la reintenta cada 500 ms.
    this.outbox.flush().catch((err) => this.logger.warn(`flush de outbox: ${err.message}`));

    const view = toView(doc);
    this.gateway.emitActivation(view);
    return view;
  }

  async list(): Promise<ActivationView[]> {
    const docs = await this.activations.find({}).sort({ createdAt: -1 }).limit(50).toArray();
    return docs.map(toView);
  }

  async get(activationId: string): Promise<ActivationView> {
    return toView(await this.getDoc(activationId));
  }

  async getDoc(activationId: string): Promise<ActivationDoc> {
    const doc = await this.activations.findOne({ _id: activationId });
    if (!doc) throw new NotFoundException(`No existe la activación ${activationId}`);
    return doc;
  }

  // ------------------------------------------------------------------ Kafka: resultados de billing y provisioning

  async consumeResult(ctx: KafkaContext): Promise<void> {
    await this.consumer.process(ctx, {
      handle: (event, tx) => this.applyResult(event, tx),
      after: (event) => this.emitById(event.correlationId),
    });
  }

  /**
   * Regla de agregación: guarda el resultado de cada servicio en el documento de la activación.
   * Con los dos OK publica ActivationCompleted; con el primer fallo publica ActivationFailed.
   * Una vez en estado final, los resultados posteriores sólo se registran en el historial.
   *
   * Si billing y provisioning responden a la vez, las dos transacciones escriben el mismo documento:
   * Mongo detecta el conflicto y reintenta una, que entonces ve el resultado de la otra.
   */
  private async applyResult(event: EventEnvelope<any>, tx: TxContext): Promise<void> {
    const activationId = event.correlationId;
    const activation = await this.activations.findOne({ _id: activationId }, { session: tx.session });
    if (!activation) {
      this.logger.warn(`[${activationId}] ${event.eventType} de una activación desconocida: se ignora`);
      return;
    }

    const entries: HistoryEntry[] = [toHistory(event)];
    const set: Partial<ActivationDoc> = { updatedAt: new Date() };
    const result = RESULT_EVENTS[event.eventType];
    const isFinal = activation.status === 'ACTIVE' || activation.status === 'FAILED';

    if (result && !isFinal) {
      const reason: string | undefined = event.payload?.reason;
      const steps = {
        ...activation.steps,
        [result.step]: { status: result.status, at: event.occurredAt, detail: entries[0].detail } as StepResult,
      };
      set.steps = steps;

      if (result.status === 'FAILED') {
        set.status = 'FAILED';
        set.failureReason = reason ?? `Falló ${result.step}`;
        const failed = createEvent<ActivationFailedPayload>({
          eventType: EVENT_TYPES.ACTIVATION_FAILED,
          correlationId: activationId,
          customerId: activation.customerId,
          source: 'activation-api',
          payload: { planId: activation.planId, failedStep: result.step, reason: set.failureReason },
        });
        await tx.publish(TOPICS.ACTIVATION_EVENTS, failed);
        entries.push(toHistory(failed));
        this.logger.warn(`[${activationId}] FAILED (${result.step}): ${set.failureReason}`);
      } else if (steps.billing?.status === 'OK' && steps.provisioning?.status === 'OK') {
        set.status = 'ACTIVE';
        const completed = createEvent<ActivationCompletedPayload>({
          eventType: EVENT_TYPES.ACTIVATION_COMPLETED,
          correlationId: activationId,
          customerId: activation.customerId,
          source: 'activation-api',
          payload: { planId: activation.planId },
        });
        await tx.publish(TOPICS.ACTIVATION_EVENTS, completed);
        entries.push(toHistory(completed));
        this.logger.log(`[${activationId}] ACTIVE`);
      } else {
        set.status = 'IN_PROGRESS';
        this.logger.log(`[${activationId}] IN_PROGRESS (${result.step} ${result.status}, falta el otro paso)`);
      }
    } else {
      this.logger.log(`[${activationId}] ${event.eventType} registrado en el historial (estado ${activation.status})`);
    }

    await this.activations.updateOne(
      { _id: activationId },
      { $set: set, $push: { history: { $each: entries } } },
      { session: tx.session },
    );
  }

  private async emitById(activationId: string): Promise<void> {
    const doc = await this.activations.findOne({ _id: activationId });
    if (doc) this.gateway.emitActivation(toView(doc));
  }
}
