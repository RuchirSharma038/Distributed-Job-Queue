import pino from 'pino';

// PM2 injects 'pm_id' (e.g., 0, 1, 2) to identify the specific worker instance.
// If we run this without PM2, we fall back to the OS process ID.
const workerId = process.env.pm_id ? `Worker-${process.env.pm_id}` : `PID-${process.pid}`;

export const logger = pino({
    level: process.env.LOG_LEVEL || 'info',
    // The 'base' object is injected into EVERY single log line automatically
    base: {
        worker_id: workerId 
    },
    // In local development, format it nicely. 
    // In production, we remove this so it outputs pure JSON strings for Datadog/CloudWatch.
    transport: process.env.NODE_ENV !== 'production' ? {
        target: 'pino-pretty',
        options: {
            colorize: true,
            translateTime: 'SYS:standard'
        }
    } : undefined
});