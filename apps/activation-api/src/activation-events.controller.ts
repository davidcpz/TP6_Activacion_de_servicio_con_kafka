import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { ActivationsService } from './activations.service';

/** activation-api consume los resultados de billing y provisioning (consumer group activation-api). */
@Controller()
export class ActivationEventsController {
  constructor(private readonly service: ActivationsService) {}

  @EventPattern(TOPICS.BILLING_EVENTS)
  onBillingEvent(@Ctx() ctx: KafkaContext) {
    return this.service.consumeResult(ctx);
  }

  @EventPattern(TOPICS.PROVISIONING_EVENTS)
  onProvisioningEvent(@Ctx() ctx: KafkaContext) {
    return this.service.consumeResult(ctx);
  }
}
