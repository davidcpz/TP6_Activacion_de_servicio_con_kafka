import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { config } from './config';
import { LoyaltyController } from './loyalty.controller';
import { LoyaltyService } from './loyalty.service';

@Module({
  imports: [KafkaToolkitModule.forRoot(config)],
  controllers: [LoyaltyController],
  providers: [LoyaltyService],
})
export class AppModule {}
