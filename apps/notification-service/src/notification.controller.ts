import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { NotificationService } from './notification.service';

@Controller()
export class NotificationController {
  constructor(private readonly notifications: NotificationService) {}

  @EventPattern(TOPICS.ACTIVATION_EVENTS)
  onActivationEvent(@Ctx() ctx: KafkaContext) {
    return this.notifications.consumeActivationEvent(ctx);
  }
}
