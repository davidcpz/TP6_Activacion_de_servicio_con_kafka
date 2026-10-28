import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { BillingController } from './billing.controller';
import { BillingService } from './billing.service';
import { config } from './config';

@Module({
  imports: [KafkaToolkitModule.forRoot(config)],
  controllers: [BillingController],
  providers: [BillingService],
})
export class AppModule {}
