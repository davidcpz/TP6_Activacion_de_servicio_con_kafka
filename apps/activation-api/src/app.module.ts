import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { ActivationEventsController } from './activation-events.controller';
import { ActivationsController } from './activations.controller';
import { ActivationsGateway } from './activations.gateway';
import { ActivationsService } from './activations.service';
import { config } from './config';
import { DemoController } from './demo.controller';

@Module({
  imports: [KafkaToolkitModule.forRoot(config)],
  controllers: [ActivationsController, ActivationEventsController, DemoController],
  providers: [ActivationsService, ActivationsGateway],
})
export class AppModule {}
