import { loadServiceConfig } from '@poc/kafka-toolkit';

export const config = loadServiceConfig('crm-analytics-service', { groupId: 'crm-analytics', mongoDb: 'crm_db' });
