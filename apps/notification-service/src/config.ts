import { loadServiceConfig } from '@poc/kafka-toolkit';

export const config = loadServiceConfig('notification-service', { groupId: 'notification-svc', mongoDb: 'notification_db' });
