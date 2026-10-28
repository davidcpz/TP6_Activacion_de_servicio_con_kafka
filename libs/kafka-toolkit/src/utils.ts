import { randomBytes } from 'crypto';

export const sleep = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

export const randomBetween = (min: number, max: number) => Math.floor(min + Math.random() * (max - min + 1));

/** Id corto legible para la demo: prefijo + 8 hex (ej. act-3f9a1c2b). */
export const shortId = (prefix: string) => `${prefix}-${randomBytes(4).toString('hex')}`;
