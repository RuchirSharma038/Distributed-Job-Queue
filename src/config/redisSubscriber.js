import Redis from 'ioredis';
import { logger } from './logger.js';
import { getIo } from './socket.js';

const subscriber = new Redis({
    host: process.env.REDIS_HOST || '127.0.0.1',
    port: process.env.REDIS_PORT ? parseInt(process.env.REDIS_PORT) : 6379,
    password: process.env.REDIS_PASSWORD,
});

subscriber.on('connect', () => logger.info('Redis subscriber connected'));
subscriber.on('error', (err) => logger.error({ err: err.message }, 'Redis subscriber error'));


export const JOB_UPDATES_CHANNEL = 'job_updates';

export function startSubscriber() {
    subscriber.subscribe(JOB_UPDATES_CHANNEL, (err, count) => {
        if (err) {
            logger.error({ err: err.message }, 'Failed to subscribe to job_updates channel');
            return;
        }
        logger.info({ channel: JOB_UPDATES_CHANNEL, count }, 'Subscribed to Redis pub/sub channel');
    });


    subscriber.on('message', (channel, rawMessage) => {
        if (channel !== JOB_UPDATES_CHANNEL) return;

        try {
            const event = JSON.parse(rawMessage);


            const io = getIo();
            io.emit('job:updated', event);

            logger.info({ jobId: event.jobId, status: event.status }, 'Broadcasted job update to dashboard');
        } catch (err) {
            logger.error({ err: err.message, rawMessage }, 'Failed to parse job_updates message');
        }
    });
}