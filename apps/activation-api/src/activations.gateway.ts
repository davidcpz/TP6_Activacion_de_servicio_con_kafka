import { Logger } from '@nestjs/common';
import { OnGatewayConnection, WebSocketGateway, WebSocketServer } from '@nestjs/websockets';
import { Server, Socket } from 'socket.io';
import { ActivationView } from './activation.model';

/** Envía a la UI cada cambio de estado de una activación (timeline en vivo). */
@WebSocketGateway({ cors: { origin: '*' } })
export class ActivationsGateway implements OnGatewayConnection {
  private readonly logger = new Logger(ActivationsGateway.name);

  @WebSocketServer()
  server: Server;

  handleConnection(client: Socket): void {
    this.logger.log(`UI conectada por WebSocket (${client.id})`);
  }

  emitActivation(activation: ActivationView): void {
    this.server?.emit('activation.updated', activation);
  }
}
