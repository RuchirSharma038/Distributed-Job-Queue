import pino from 'pino';


const workerId = process.env.pm_id ? `Worker-${process.env.pm_id}` : `PID-${process.pid}`;

export const logger = pino({
    level: process.env.LOG_LEVEL || 'info',

    base: {
        worker_id: workerId
    },

    transport: process.env.NODE_ENV !== 'production' ? {
        target: 'pino-pretty',
        options: {
            colorize: true,
            translateTime: 'SYS:standard'
        }
    } : undefined
});