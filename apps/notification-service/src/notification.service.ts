
import { Inject, Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaContext } from '@nestjs/microservices';
import { ActivationCompletedPayload, ActivationFailedPayload, EVENT_TYPES, EventEnvelope } from '@poc/contracts';
import { EventConsumer, MONGO_DB, TxContext } from '@poc/kafka-toolkit';
import { Collection, Db } from 'mongodb';
import { MailerService } from './mailer.service';

interface NotificationDoc {
  activationId: string;
  eventType: string;
  eventId: string;
  customerId: string;
  to: string;
  subject: string;
  sentAt: Date;
}

@Injectable()
export class NotificationService implements OnModuleInit {
  private readonly logger = new Logger(NotificationService.name);
  private readonly notifications: Collection<NotificationDoc>;

  constructor(
    @Inject(MONGO_DB) db: Db,
    private readonly consumer: EventConsumer,
    private readonly mailer: MailerService,
  ) {
    this.notifications = db.collection<NotificationDoc>('notifications');
  }

  async onModuleInit(): Promise<void> {
    // Una sola notificación por activación y tipo de evento.
    await this.notifications.createIndex({ activationId: 1, eventType: 1 }, { unique: true });
  }

  consumeActivationEvent(ctx: KafkaContext) {
    return this.consumer.process(ctx, { handle: (event, tx) => this.notify(event, tx) });
  }

  /**
   * El email se envía dentro de la transacción, antes del commit: si el envío falla, la transacción
   * se aborta y el mensaje se reintenta (y termina en la DLQ si Mailhog no responde).
   * Riesgo documentado: si el servicio muere justo entre enviar y confirmar, el email puede repetirse.
   */
  private async notify(event: EventEnvelope<any>, tx: TxContext): Promise<void> {
    const isCompleted = event.eventType === EVENT_TYPES.ACTIVATION_COMPLETED;
    const isFailed = event.eventType === EVENT_TYPES.ACTIVATION_FAILED;
    if (!isCompleted && !isFailed) return;

    const activationId = event.correlationId;
    const already = await this.notifications.findOne(
      { activationId, eventType: event.eventType },
      { session: tx.session },
    );
    if (already) {
      this.logger.warn(`[${activationId}] Ya se envió el email de ${event.eventType}; no se repite`);
      return;
    }

    const to = `${event.customerId.toLowerCase()}@clientes.demo`;
    const { subject, html } = isCompleted
      ? this.welcome(event as EventEnvelope<ActivationCompletedPayload>)
      : this.failure(event as EventEnvelope<ActivationFailedPayload>);

    await this.notifications.insertOne(
      { activationId, eventType: event.eventType, eventId: event.eventId, customerId: event.customerId, to, subject, sentAt: new Date() },
      { session: tx.session },
    );
    await this.mailer.send(to, subject, html);
  }

  private welcome(event: EventEnvelope<ActivationCompletedPayload>) {
    return {
      subject: `¡Bienvenido! Tu ${event.payload.planId} ya está activo`,
      html: emailLayout({
        color: '#1f9d55',
        icon: '✓',
        title: '¡Tu plan está activo!',
        body: `
          <p style="margin:0 0 12px">Hola <b>${event.customerId}</b>,</p>
          <p style="margin:0 0 12px">Tu <b>${event.payload.planId}</b> ya está listo para usar. ¡Gracias por elegirnos!</p>`,
        event,
      }),
    };
  }

  private failure(event: EventEnvelope<ActivationFailedPayload>) {
    const step = event.payload.failedStep === 'billing' ? 'facturación' : 'aprovisionamiento';
    const charge =
      event.payload.failedStep === 'provisioning'
        ? 'No se realizó ningún cobro: la cuenta de facturación fue anulada.'
        : 'No se realizó ningún cobro.';
    return {
      subject: `No pudimos activar tu ${event.payload.planId}`,
      html: emailLayout({
        color: '#d64545',
        icon: '!',
        title: 'No pudimos activar tu plan',
        body: `
          <p style="margin:0 0 12px">Hola <b>${event.customerId}</b>,</p>
          <p style="margin:0 0 12px">Hubo un problema al activar tu <b>${event.payload.planId}</b>.</p>
          <p style="margin:0 0 12px;padding:10px 12px;background:#fdeaea;border-radius:6px">
            <b>Motivo:</b> ${event.payload.reason}<br><b>Paso:</b> ${step}
          </p>
          <p style="margin:0 0 12px">${charge}</p>`,
        event,
      }),
    };
  }
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString('es-AR', {
      timeZone: 'America/Argentina/Buenos_Aires',
      dateStyle: 'short',
      timeStyle: 'medium',
    });
  } catch {
    return iso;
  }
}

/** Plantilla común de los emails: tarjeta centrada con estilos inline (lo que soportan los clientes de mail). */
function emailLayout(opts: { color: string; icon: string; title: string; body: string; event: EventEnvelope<unknown> }): string {
  const { color, icon, title, body, event } = opts;
  return `
<div style="background:#f4f6f9;padding:32px 12px;font-family:Arial,Helvetica,sans-serif;color:#1c2430">
  <div style="max-width:520px;margin:0 auto;background:#ffffff;border:1px solid #e2e7ee;border-radius:12px;overflow:hidden">
    <div style="background:#1f4e8c;color:#ffffff;padding:16px 24px;font-size:15px;font-weight:bold">Activación de servicios</div>
    <div style="padding:24px">
      <div style="width:44px;height:44px;line-height:44px;border-radius:50%;background:${color};color:#ffffff;text-align:center;font-size:22px;font-weight:bold;margin-bottom:12px">${icon}</div>
      <h2 style="margin:0 0 16px;font-size:20px;color:${color}">${title}</h2>
      <div style="font-size:15px;line-height:1.5">${body}</div>
    </div>
    <div style="padding:12px 24px;border-top:1px solid #e2e7ee;font-size:12px;color:#6b7686">
      Activación <code>${event.correlationId}</code> · ${formatDate(event.occurredAt)}<br>
      Email simulado de la POC · Sistemas Distribuidos 2026
    </div>
  </div>
</div>`;
}
