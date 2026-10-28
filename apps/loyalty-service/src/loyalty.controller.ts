import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { LoyaltyService } from './loyalty.service';

@Controller()
export class LoyaltyController {
  constructor(private readonly loyalty: LoyaltyService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Ctx() ctx: KafkaContext) {
    return this.loyalty.consume(ctx);
  }
}
