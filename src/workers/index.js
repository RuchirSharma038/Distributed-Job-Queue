import redis from "../config/redis.js";
import prisma from "../config/database.js";
import { logger } from "../config/logger.js";
import { handlers } from "./handlers/handlerMap.js";
import { QUEUE_ROUTING, DELAYED_QUEUE, RETRY_BASE_DELAY_MS, DEAD_QUEUE } from "../config/constants.js";

const QUEUE_NAME = process.env.QUEUE_NAME || "queue:io";

function createJobLogger(job) {
    return logger.child({ jobId: job.id, type: job.type });
}

// Retry scheduler

function calcNextRetryTimestamp(retryCount) {
    const delayMs = RETRY_BASE_DELAY_MS * Math.pow(2, retryCount - 1);
    return Date.now() + delayMs;
}

async function scheduleRetry(job, handlerError, log) {
    const newRetryCount = job.retry_count + 1;
    const nextRetryAt = new Date(calcNextRetryTimestamp(newRetryCount));

    //  Update Postgres: mark retrying, store the error, increment count
    await prisma.job.update({
        where: { id: job.id },
        data: {
            status: 'retrying',
            retry_count: newRetryCount,
            next_retry_at: nextRetryAt,
            error_message: handlerError.message,
        }
    });

    //  Push into Redis Sorted Set (score = ms timestamp)
    await redis.zadd(DELAYED_QUEUE, nextRetryAt.getTime(), job.id);

    log.warn(
        { attempt: newRetryCount, max: job.max_retries, retryAt: nextRetryAt },
        `Job failed — scheduled retry ${newRetryCount}/${job.max_retries}`
    );
}



async function markFailed(job, handlerError) {
    await prisma.job.update({
        where: { id: job.id },
        data: {
            status: 'dead',
            error_message: handlerError.message,
            dead_at: new Date(),
        }
    });

    //Push to Dead Queue
    await redis.lpush(DEAD_QUEUE, job.id);

    logger.error(
        { jobId: job.id, type: job.type, retries: job.retry_count },
        `Job moved to Dead Letter Queue after ${job.retry_count} retries: ${handlerError.message}`
    );
}


// Main worker loop


async function startWorker() {
    logger.info({ queue: QUEUE_NAME, pid: process.pid }, "Worker started");

    while (true) {
        try {
            // BRPOP blocks until a job is available
            const result = await redis.brpop(QUEUE_NAME, 0);
            if (!result) continue;

            const [queueName, jobId] = result;
            logger.info({ jobId, queue: queueName }, "Job picked up from queue");

            // Fetch the authoritative record from Postgres.
            const job = await prisma.job.findUnique({ where: { id: jobId } });

            if (!job) {
                logger.error({ jobId }, "Job ID found in Redis but missing from DB — skipping");
                continue;
            }

            const log = createJobLogger(job);
            log.info({ queue: queueName }, "Job picked up");


            if (job.status === 'completed' || job.status === 'failed' || job.status === 'dead') {
                log.warn({ status: job.status }, "Skipping terminal job — possible duplicate push");
                continue;
            }

            // Mark as running
            await prisma.job.update({
                where: { id: jobId },
                data: { status: 'running', started_at: new Date() }
            });


            // Execution + retry fork

            const executeJob = handlers[job.type];

            try {
                if (!executeJob) {
                    //Configuration error 
                    const err = new Error(`No handler registered for job type: "${job.type}"`);
                    err.permanent = true;
                    throw err;
                }


                const jobResult = await executeJob(job.payload, job.id, log);

                // Success path
                await prisma.job.update({
                    where: { id: jobId },
                    data: {
                        status: 'completed',
                        completed_at: new Date(),
                        result_data: jobResult ?? {},
                    }
                });
                log.info("Job completed successfully");

            } catch (handlerError) {

                if (handlerError.permanent) {
                    log.warn({ error: handlerError.message }, "Permanent error — skipping retries, sending to DLQ");
                    await markDead(job, handlerError, log);
                } else if (job.retry_count < job.max_retries) {
                    await scheduleRetry(job, handlerError, log);
                } else {
                    await markFailed(job, handlerError, log);
                }
            }

        } catch (redisError) {
            logger.error({ err: redisError.message }, "Redis connection error — backing off 1s");
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
}

startWorker();