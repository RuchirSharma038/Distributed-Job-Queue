import redis from "../../config/redis.js";
import prisma from "../../config/database.js";
import {
    QUEUE_ROUTING,
    DELAYED_QUEUE,
    DEAD_QUEUE,
    DEFAULT_PRIORITY,
    getPriorityQueue,
} from "../../config/constants.js";
import { logger } from "../../config/logger.js";

const RESET_DATA = {
    status: 'queued',
    retry_count: 0,
    error_message: null,
    next_retry_at: null,
    dead_at: null,
    started_at: null,
    completed_at: null,
};

export const createJobService = async (jobType, payload, runAt = null, priority = DEFAULT_PRIORITY) => {
    //Segregrate by intensity according to QUEUE_ROUTING

    const baseQueue = QUEUE_ROUTING[jobType];

    if (!baseQueue) {
        throw new Error(`Invalid Job Type: ${jobType}`);
    }
    const isScheduled = runAt !== null;
    const scheduledAt = isScheduled ? new Date(runAt) : null;
    const initialStatus = isScheduled ? 'scheduled' : 'queued';

    //Write to postgres
    const job = await prisma.job.create({
        data: {
            type: jobType,
            payload: payload,
            status: initialStatus,
            priority,
            scheduled_at: scheduledAt,
        }
    });



    if (isScheduled) {

        await redis.zadd(DELAYED_QUEUE, scheduledAt.getTime(), job.id);

        logger.info(
            { jobId: job.id, type: jobType, scheduledAt: scheduledAt.toISOString() },
            `[Job Scheduled] ID: ${job.id} → ${DELAYED_QUEUE} (runs at ${scheduledAt.toISOString()})`
        );
    } else {

        const targetQueue = getPriorityQueue(baseQueue, priority);
        await redis.lpush(targetQueue, job.id);

        logger.info(
            { jobId: job.id, type: jobType, priority, queue: targetQueue },
            `[Job Queued] ID: ${job.id} → ${targetQueue}`
        );
    }

    return job;

};


export const replayDeadJobsService = async () => {

    const jobIds = await redis.lrange(DEAD_QUEUE, 0, -1);

    let replayed = 0, skipped = 0;

    for (const jobId of jobIds) {
        const job = await prisma.job.findUnique({
            where: { id: jobId },
            select: { id: true, type: true, status: true, priority: true }
        });

        if (!job || job.status !== 'dead') {
            skipped++;
            continue;
        }

        const baseQueue = QUEUE_ROUTING[job.type];
        const targetQueue = getPriorityQueue(baseQueue, job.priority ?? DEFAULT_PRIORITY);

        if (!baseQueue) { skipped++; continue; }

        await prisma.job.update({
            where: { id: jobId },
            data: RESET_DATA
        });

        const pipeline = redis.multi();
        pipeline.lpush(targetQueue, jobId);
        pipeline.lrem(DEAD_QUEUE, 1, jobId);
        await pipeline.exec();

        replayed++;
    }
    return { replayed, skipped };

};

export const replaySingleJobService = async (jobId) => {
    const job = await prisma.job.findUnique({
        where: { id: jobId },
        select: { id: true, type: true, status: true, priority: true }
    });

    if (!job) return { notFound: true };
    if (job.status !== 'dead') return { wrongStatus: true, status: job.status };

    const baseQueue = QUEUE_ROUTING[job.type];
    const targetQueue = getPriorityQueue(baseQueue, job.priority ?? DEFAULT_PRIORITY);

    await prisma.job.update({ where: { id: jobId }, data: RESET_DATA });
    const pipeline = redis.multi();
    pipeline.lpush(targetQueue, jobId);
    pipeline.lrem(DEAD_QUEUE, 1, jobId);
    await pipeline.exec();

    return { targetQueue };

};