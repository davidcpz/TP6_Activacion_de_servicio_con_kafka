import { Module } from '@nestjs/common';
import { KafkaToolkitModule } from '@poc/kafka-toolkit';
import { config } from './config';
import { MailerService } from './mailer.service';
import { NotificationController } from './notification.controller';
import { NotificationService } from './notification.service';

@Module({
  imports: [KafkaToolkitModule.forRoot(config)],
  controllers: [NotificationController],
  providers: [NotificationService, MailerService],
})
export class AppModule {}
