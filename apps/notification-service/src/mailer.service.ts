import { Injectable, Logger } from '@nestjs/common';
import { env } from '@poc/kafka-toolkit';
import * as nodemailer from 'nodemailer';

/** Envío de emails a Mailhog (SMTP sin autenticación). Bandeja visible en http://localhost:8025 */
@Injectable()
export class MailerService {
  private readonly logger = new Logger(MailerService.name);
  private readonly from = env('MAIL_FROM', 'Activaciones POC <no-reply@activation-poc.local>');
  private readonly transporter = nodemailer.createTransport({
    host: env('SMTP_HOST', 'localhost'),
    port: Number(env('SMTP_PORT', '1025')),
    secure: false,
    ignoreTLS: true,
  });

  async send(to: string, subject: string, html: string): Promise<void> {
    await this.transporter.sendMail({ from: this.from, to, subject, html });
    this.logger.log(`Email enviado a ${to}: "${subject}"`);
  }
}
