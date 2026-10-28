import { loadServiceConfig } from '@poc/kafka-toolkit';

export const config = loadServiceConfig('billing-service', { groupId: 'billing-svc', mongoDb: 'billing_db' });
