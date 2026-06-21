import redis from "../config/redis.js";
import prisma from "../config/database.js";
import { logger } from "../config/logger.js";
import {
    QUEUE_ROUTING,
    DELAYED_QUEUE,
    DEFAULT_PRIORITY,
    getPriorityQueue,
} from "../config/constants.js";

const POLL_INTERVAL_MS = 5_000;
const TERMINAL_STATUSES = new Set(['completed', 'failed', 'dead']);

async function promoteReadyJobs() {
    const nowMs = Date.now();
    const readyJobIds = await redis.zrangebyscore(DELAYED_QUEUE, 0, nowMs);

    if (readyJobIds.length === 0) return;

    logger.info({ count: readyJobIds.length }, "Scheduler: jobs ready for promotion");

    for (const jobId of readyJobIds) {
        try {
            await promoteJob(jobId);
        } catch (err) {
            logger.error({ jobId, err: err.message }, "Scheduler: error promoting job — will retry on next poll");
        }
    }
}

async function promoteJob(jobId) {
    const job = await prisma.job.findUnique({
        where: { id: jobId },

        select: {
            id: true,
            type: true,
            status: true,
            priority: true,
            scheduled_at: true,
            retry_count: true,
        },
    });

    if (!job) {
        logger.warn({ jobId }, "Scheduler: job not found in DB — removing from sorted set");
        await redis.zrem(DELAYED_QUEUE, jobId);
        return;
    }

    if (TERMINAL_STATUSES.has(job.status)) {
        logger.warn({ jobId, status: job.status }, "Scheduler: terminal job in delayed queue — removing");
        await redis.zrem(DELAYED_QUEUE, jobId);
        return;
    }

    const baseQueue = QUEUE_ROUTING[job.type];

    if (!baseQueue) {
        logger.error({ jobId, type: job.type }, "Scheduler: no base queue mapping — cannot promote, removing");
        await redis.zrem(DELAYED_QUEUE, jobId);
        return;
    }


    const priority = job.priority ?? DEFAULT_PRIORITY;
    const targetQueue = getPriorityQueue(baseQueue, priority);


    const isScheduledJob = job.status === 'scheduled';
    const isRetryJob = job.status === 'retrying';

    if (isScheduledJob) {
        const updated = await prisma.job.updateMany({
            where: { id: jobId, status: 'scheduled' },
            data: { status: 'queued' },
        });
        if (updated.count === 0) {
            logger.warn({ jobId }, "Scheduler: Job state changed during promotion, aborting.");
            return;
        }
    } else if (isRetryJob) {
        const updated = await prisma.job.updateMany({
            where: { id: jobId, status: 'retrying' },
            data: { status: 'queued', next_retry_at: null },
        });
        if (updated.count === 0) {
            logger.warn({ jobId }, "Scheduler: Job state changed during promotion, aborting.");
            return;
        }
    }


    const pipeline = redis.multi();
    pipeline.lpush(targetQueue, jobId);
    pipeline.zrem(DELAYED_QUEUE, jobId);
    await pipeline.exec();

    if (isScheduledJob) {
        logger.info(
            { jobId, type: job.type, priority, targetQueue, scheduledAt: job.scheduled_at },
            "Scheduler: promoted SCHEDULED job → live priority queue"
        );
    } else if (isRetryJob) {
        logger.info(
            { jobId, type: job.type, priority, targetQueue, retryCount: job.retry_count },
            `Scheduler: promoted RETRY job (attempt ${job.retry_count}) → live priority queue`
        );
    } else {
        logger.info(
            { jobId, type: job.type, priority, targetQueue, status: job.status },
            "Scheduler: promoted job → live priority queue"
        );
    }
}


async function startScheduler() {
    logger.info({ pollIntervalMs: POLL_INTERVAL_MS }, "Scheduler started");

    // Run immediately on startup, then on interval
    await promoteReadyJobs();

    setInterval(async () => {
        try {
            await promoteReadyJobs();
        } catch (err) {
            logger.error({ err: err.message }, "Scheduler poll failed");
        }
    }, POLL_INTERVAL_MS);
}

startScheduler();
