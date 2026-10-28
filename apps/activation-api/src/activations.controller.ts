import { Body, Controller, Get, HttpCode, Param, Post } from '@nestjs/common';
import { ActivationsService, CreateActivationDto } from './activations.service';

@Controller('activations')
export class ActivationsController {
  constructor(private readonly service: ActivationsService) {}

  /** RF-02: responde 202 Accepted con el activationId y estado PENDING, sin esperar a los demás. */
  @Post()
  @HttpCode(202)
  create(@Body() body: CreateActivationDto) {
    return this.service.create(body);
  }

  @Get()
  list() {
    return this.service.list();
  }

  /** RF-09: estado actual e historial de eventos. */
  @Get(':id')
  get(@Param('id') id: string) {
    return this.service.get(id);
  }
}
