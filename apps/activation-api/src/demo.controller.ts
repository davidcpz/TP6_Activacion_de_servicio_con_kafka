import { Body, Controller, Get, Logger, Param, Post } from '@nestjs/common';
import { TOPICS } from '@poc/contracts';
import { EventPublisher } from '@poc/kafka-toolkit';
import { ActivationsService } from './activations.service';

/**
 * Endpoints de apoyo para la demo y para verificar los criterios de aceptación.
 * No forman parte del flujo de negocio.
 */
@Controller()
export class DemoController {
  private readonly logger = new Logger(DemoController.name);

  constructor(
    private readonly service: ActivationsService,
    private readonly publisher: EventPublisher,
  ) {}

  @Get('health')
  health() {
    return { status: 'ok' };
  }

  /**
   * Criterio: "Reenviar un evento ya procesado no genera cuentas ni emails duplicados".
   * Vuelve a publicar el ActivationRequested ORIGINAL (mismo eventId). Billing y provisioning
   * lo descartan por idempotencia: no se crea otra cuenta ni otra orden.
   */
  @Post('demo/replay/:id')
  async replay(@Param('id') id: string) {
    const doc = await this.service.getDoc(id);
    await this.publisher.publish(TOPICS.ACTIVATION_REQUESTED, doc.requestEvent);
    this.logger.warn(`[${id}] Reenviado ActivationRequested duplicado (eventId ${doc.requestEvent.eventId})`);
    return { replayed: true, activationId: id, eventId: doc.requestEvent.eventId, topic: TOPICS.ACTIVATION_REQUESTED };
  }

  /**
   * Criterio: "Un mensaje inválido termina en la DLQ y el resto del flujo sigue".
   * Publica un mensaje que no respeta el contrato. Cada consumidor lo reintenta 3 veces
   * (1 s, 2 s, 4 s) y lo manda a <topic>.dlq.
   */
  @Post('demo/poison')
  async poison(@Body() body: { topic?: string; customerId?: string }) {
    const topic = body?.topic || TOPICS.ACTIVATION_REQUESTED;
    const key = body?.customerId || 'C-POISON';
    const value = '{ "esto": "no es un evento válido", "falta": "eventId, eventType, ..."';
    await this.publisher.send(topic, key, value, { source: 'demo-poison' });
    this.logger.warn(`Mensaje inválido publicado en ${topic} (clave ${key})`);
    return { published: true, topic, key, expectedDlq: `${topic}.dlq` };
  }
}
