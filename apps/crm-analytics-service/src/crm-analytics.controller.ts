import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { CrmAnalyticsService } from './crm-analytics.service';

/** Escucha los 4 topics y no publica nada: se sumó sin tocar a ningún otro servicio. */
@Controller()
export class CrmAnalyticsController {
  constructor(private readonly crm: CrmAnalyticsService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Ctx() ctx: KafkaContext) {
    return this.crm.record(ctx);
  }

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBillingEvent(@Ctx() ctx: KafkaContext) {
    return this.crm.record(ctx);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioningEvent(@Ctx() ctx: KafkaContext) {
    return this.crm.record(ctx);
  }

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Ctx() ctx: KafkaContext) {
    return this.crm.record(ctx);
  }
}
