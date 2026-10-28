import 'reflect-metadata';
import { Logger } from '@nestjs/common';
import { NestFactory } from '@nestjs/core';
import { kafkaServerOptions } from '@poc/kafka-toolkit';
import { AppModule } from './app.module';
import { config, PORT } from './config';

async function bootstrap() {
  // App híbrida: HTTP + WebSocket (puerto 3000) y consumidor Kafka (@EventPattern).
  const app = await NestFactory.create(AppModule);
  app.enableCors({ origin: true });
  app.enableShutdownHooks();
  app.connectMicroservice(kafkaServerOptions(config));
  await app.init(); // conecta Mongo y el productor antes de empezar a consumir
  await app.startAllMicroservices();
  await app.listen(PORT, '0.0.0.0');
  new Logger('activation-api').log(`Escuchando en http://localhost:${PORT} · consumer group "${config.groupId}"`);
}

bootstrap();
