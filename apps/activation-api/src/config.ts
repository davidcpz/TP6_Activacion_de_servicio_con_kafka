import { env, loadServiceConfig } from '@poc/kafka-toolkit';

export const config = loadServiceConfig('activation-api', { groupId: 'activation-api', mongoDb: 'activation_db' });
export const PORT = Number(env('PORT', '3000'));
