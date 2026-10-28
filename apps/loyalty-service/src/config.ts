import { loadServiceConfig } from '@poc/kafka-toolkit';

export const config = loadServiceConfig('loyalty-service', { groupId: 'loyalty-svc', mongoDb: 'loyalty_db' });
