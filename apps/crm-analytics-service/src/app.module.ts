import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { config } from './config';
import { CrmAnalyticsController } from './crm-analytics.controller';
import { CrmAnalyticsService } from './crm-analytics.service';

@Module({
  imports: [KafkaToolkitModule.forRoot(config)],
  controllers: [CrmAnalyticsController],
  providers: [CrmAnalyticsService],
})
export class AppModule {}
