import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';
import { config } from './config';

async function bootstrap() {
  // Microservicio puro: sólo consume y publica en Kafka (no expone HTTP).
  const app = await NestFactory.createMicroservice(AppModule, kafkaServerOptions(config));
  app.enableShutdownHooks();
  await app.listen();
  new Logger('provisioning-service').log(`Consumiendo con el consumer group "${config.groupId}"`);
}

bootstrap();
