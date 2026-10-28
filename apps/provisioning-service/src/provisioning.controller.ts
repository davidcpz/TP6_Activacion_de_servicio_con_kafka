import { Controller } from '@nestjs/common';
import { Ctx, EventPattern, KafkaContext } from '@nestjs/microservices';
import { TOPICS } from '@poc/contracts';
import { ProvisioningService } from './provisioning.service';

@Controller()
export class ProvisioningController {
  constructor(private readonly provisioning: ProvisioningService) {}

  @EventPattern(TOPICS.ACTIVATION_REQUESTED)
  onActivationRequested(@Ctx() ctx: KafkaContext) {
    return this.provisioning.consumeActivationRequested(ctx);
  }
}
