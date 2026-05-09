import redis from "../config/redis.js";
import prisma from "../config/database.js";
import { logger } from "../config/logger.js";
import { handlers } from "./handlers/handlerMap.js";
import { QUEUE_ROUTING, DELAYED_QUEUE, RETRY_BASE_DELAY_MS } from "../config/constants.js";

const QUEUE_NAME = process.env.QUEUE_NAME || "queue:io";


// Retry scheduler

function calcNextRetryTimestamp(retryCount) {
    const delayMs = RETRY_BASE_DELAY_MS * Math.pow(2, retryCount - 1);
    return Date.now() + delayMs;
}

async function scheduleRetry(job, handlerError) {
    const newRetryCount = job.retry_count + 1;
    const nextRetryAt   = new Date(calcNextRetryTimestamp(newRetryCount));

    //  Update Postgres: mark retrying, store the error, increment count
    await prisma.job.update({
        where: { id: job.id },
        data: {
            status:        'retrying',
            retry_count:   newRetryCount,
            next_retry_at: nextRetryAt,
            error_message: handlerError.message,
        }
    });

    //  Push into Redis Sorted Set (score = ms timestamp)
    await redis.zadd(DELAYED_QUEUE, nextRetryAt.getTime(), job.id);

    logger.warn(
        { jobId: job.id, type: job.type, attempt: newRetryCount, retryAt: nextRetryAt },
        `Job failed — scheduled retry ${newRetryCount}/${job.max_retries}`
    );
}

async function markFailed(job, handlerError) {
    await prisma.job.update({
        where: { id: job.id },
        data: {
            status:        'failed',
            error_message: handlerError.message,
        }
    });
    logger.error(
        { jobId: job.id, type: job.type, retries: job.retry_count },
        `Job permanently failed after ${job.retry_count} retries: ${handlerError.message}`
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

            // Guard: if the job is already completed or permanently failed
            if (job.status === 'completed' || job.status === 'failed') {
                logger.warn({ jobId, status: job.status }, "Skipping already-terminal job — possible duplicate push");
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
                    throw new Error(`No handler registered for job type: "${job.type}"`);
                }

               
                const jobResult = await executeJob(job.payload, job.id);

                // Success path
                await prisma.job.update({
                    where: { id: jobId },
                    data: {
                        status:       'completed',
                        completed_at: new Date(),
                        result_data:  jobResult ?? {},
                    }
                });
                logger.info({ jobId, type: job.type }, "Job completed successfully");

            } catch (handlerError) {
                const isRetryable = !handlerError.permanent
                                 && job.retry_count < job.max_retries;

                if (isRetryable) {
                    await scheduleRetry(job, handlerError);
                } else {
                    await markFailed(job, handlerError);
                }
            }

        } catch (redisError) {
            logger.error({ err: redisError.message }, "Redis connection error — backing off 1s");
            await new Promise(resolve => setTimeout(resolve, 1000));
        }
    }
}

startWorker();