import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { config } from './config';
import { ProvisioningController } from './provisioning.controller';
import { ProvisioningService } from './provisioning.service';

@Module({
  imports: [KafkaToolkitModule.forRoot(config)],
  controllers: [ProvisioningController],
  providers: [ProvisioningService],
})
export class AppModule {}
