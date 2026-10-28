import { loadServiceConfig } from '@poc/kafka-toolkit';

export const config = loadServiceConfig('provisioning-service', { groupId: 'provisioning-svc', mongoDb: 'provisioning_db' });
