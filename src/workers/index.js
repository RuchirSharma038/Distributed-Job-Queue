import redis from "../config/redis.js";
import prisma from "../config/database.js";
import { logger } from "../config/logger.js";
import { handlers } from "./handlers/handlerMap.js";
import {
    DELAYED_QUEUE,
    DEAD_QUEUE,
    RETRY_BASE_DELAY_MS,
    IO_BRPOP_QUEUES,
    COMPUTE_BRPOP_QUEUES,
    getPriorityQueue,
    QUEUE_ROUTING,
    DEFAULT_PRIORITY,
} from "../config/constants.js";
import { JOB_UPDATES_CHANNEL } from "../config/redisSubscriber.js";

const WORKER_TYPE = process.env.WORKER_TYPE || 'io';
const LISTEN_QUEUES = WORKER_TYPE === 'compute' ? COMPUTE_BRPOP_QUEUES : IO_BRPOP_QUEUES;

// Redis Pub/Sub publisher

async function publishJobEvent(job, extraFields = {}) {
    try {
        const event = {
            jobId: job.id,
            type: job.type,
            status: job.status,
            priority: job.priority ?? DEFAULT_PRIORITY,
            retries: job.retry_count ?? 0,
            timestamp: new Date().toISOString(),
            ...extraFields,
        };

        await redis.publish(JOB_UPDATES_CHANNEL, JSON.stringify(event));
    } catch (err) {
        
        logger.warn({ err: err.message, jobId: job.id }, 'Failed to publish job event — dashboard may miss this update');
    }
}


// Child logger


function createJobLogger(job) {
    return logger.child({ jobId: job.id, type: job.type, priority: job.priority });
}


// Retry + DLQ helpers

function calcNextRetryTimestamp(retryCount) {
    return Date.now() + RETRY_BASE_DELAY_MS * Math.pow(2, retryCount - 1);
}

async function scheduleRetry(job, handlerError, log) {
    const newRetryCount = job.retry_count + 1;
    const nextRetryAt = new Date(calcNextRetryTimestamp(newRetryCount));

    const updated = await prisma.job.update({
        where: { id: job.id },
        data: {
            status: 'retrying',
            retry_count: newRetryCount,
            next_retry_at: nextRetryAt,
            error_message: handlerError.message,
        }
    });

    await redis.zadd(DELAYED_QUEUE, nextRetryAt.getTime(), job.id);

    // Publish
    await publishJobEvent(updated, {
        error: handlerError.message,
        nextRetryAt: nextRetryAt.toISOString(),
    });

    log.warn(
        { attempt: newRetryCount, max: job.max_retries, retryAt: nextRetryAt },
        `Job failed — scheduled retry ${newRetryCount}/${job.max_retries}`
    );
}

async function markDead(job, handlerError, log) {
    const updated = await prisma.job.update({
        where: { id: job.id },
        data: { status: 'dead', dead_at: new Date(), error_message: handlerError.message }
    });

    await redis.lpush(DEAD_QUEUE, job.id);
    await publishJobEvent(updated, { error: handlerError.message });

    log.error(
        { retries: job.retry_count, error: handlerError.message },
        `Job moved to Dead Letter Queue after ${job.retry_count} retries`
    );
}


// Main worker loop

async function startWorker() {
    logger.info({ workerType: WORKER_TYPE, queues: LISTEN_QUEUES, pid: process.pid }, "Worker started");

    while (true) {
        try {
            const result = await redis.brpop(...LISTEN_QUEUES, 0);
            if (!result) continue;

            const [pickedQueue, jobId] = result;
            const job = await prisma.job.findUnique({ where: { id: jobId } });

            if (!job) {
                logger.error({ jobId }, "Job ID in Redis but not in DB — skipping");
                continue;
            }

            const log = createJobLogger(job);
            log.info({ queue: pickedQueue }, "Job picked up");

            if (['completed', 'failed', 'dead'].includes(job.status)) {
                log.warn({ status: job.status }, "Skipping terminal job — possible duplicate push");
                continue;
            }

            // Publish running
            const runningJob = await prisma.job.update({
                where: { id: jobId },
                data: { status: 'running', started_at: new Date() }
            });
            await publishJobEvent(runningJob);

            const executeJob = handlers[job.type];

            try {
                if (!executeJob) {
                    const err = new Error(`No handler registered for job type: "${job.type}"`);
                    err.permanent = true;
                    throw err;
                }

                const jobResult = await executeJob(job.payload, job.id, log);

                const completedJob = await prisma.job.update({
                    where: { id: jobId },
                    data: {
                        status: 'completed',
                        completed_at: new Date(),
                        result_data: jobResult ?? {},
                    }
                });

                // Publish result
                await publishJobEvent(completedJob, { result: jobResult });
                log.info("Job completed successfully");

            } catch (handlerError) {
                if (handlerError.permanent) {
                    log.warn({ error: handlerError.message }, "Permanent error — sending to DLQ");
                    await markDead(job, handlerError, log);
                } else if (job.retry_count < job.max_retries) {
                    await scheduleRetry(job, handlerError, log);
                } else {
                    await markDead(job, handlerError, log);
                }
            }

        } catch (redisError) {
            logger.error({ err: redisError.message }, "Redis error — backing off 1s");
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
}

startWorker();