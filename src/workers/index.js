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
    DEFAULT_PRIORITY,
} from "../config/constants.js";
import { JOB_UPDATES_CHANNEL } from "../config/redisSubscriber.js";


// Configuration


const WORKER_TYPE = process.env.WORKER_TYPE || 'io';
const LISTEN_QUEUES = WORKER_TYPE === 'compute' ? COMPUTE_BRPOP_QUEUES : IO_BRPOP_QUEUES;


const CONCURRENCY = process.env.WORKER_CONCURRENCY
    ? parseInt(process.env.WORKER_CONCURRENCY)
    : WORKER_TYPE === 'io' ? 10 : 1;


const queueClient = redis.duplicate();


// Semaphore 

class Semaphore {
    constructor(max) {
        this._max = max;
        this._active = 0;
        this._waiters = [];
    }

    get active() { return this._active; }


    acquire() {
        if (this._active < this._max) {
            this._active++;
            return Promise.resolve();
        }
        // At capacity 
        return new Promise(resolve => this._waiters.push(resolve));
    }


    release() {
        if (this._waiters.length > 0) {
            // Hand the slot directly to the next waiter 
            this._waiters.shift()();
        } else {
            this._active--;
        }
    }
}

const sem = new Semaphore(CONCURRENCY);


// Helpers 


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
        logger.warn({ err: err.message, jobId: job.id },
            'Failed to publish job event — dashboard may miss this update');
    }
}

function createJobLogger(job) {
    return logger.child({ jobId: job.id, type: job.type, priority: job.priority });
}

function calcNextRetryTimestamp(retryCount) {
    return Date.now() + RETRY_BASE_DELAY_MS * Math.pow(2, retryCount - 1);
}

async function scheduleRetry(job, handlerError, log) {
    const newRetryCount = job.retry_count + 1;
    const nextRetryAt = new Date(calcNextRetryTimestamp(newRetryCount));

    const claim = await prisma.job.updateMany({
        where: { id: job.id, status: 'running' },
        data: {
            status: 'retrying',
            retry_count: newRetryCount,
            next_retry_at: nextRetryAt,
            error_message: handlerError.message,
        }
    });
    if (claim.count === 0) {
        log.warn(
            { jobId: job.id },
            "scheduleRetry: job no longer 'running' — already reconciled/swept elsewhere, discarding this worker's retry attempt"
        );
        return;
    }

    await redis.zadd(DELAYED_QUEUE, nextRetryAt.getTime(), job.id);
    const updated = { ...job, status: 'retrying', retry_count: newRetryCount };
    await publishJobEvent(updated, {
        error: handlerError.message,
        nextRetryAt: nextRetryAt.toISOString(),
    });

    log.warn({ attempt: newRetryCount, max: job.max_retries, retryAt: nextRetryAt },
        `Job failed — scheduled retry ${newRetryCount}/${job.max_retries}`);
}

async function markDead(job, handlerError, log) {
    const claim = await prisma.job.updateMany({
        where: { id: job.id, status: 'running' },
        data: { status: 'dead', dead_at: new Date(), error_message: handlerError.message }
    });
    if (claim.count === 0) {
        log.warn(
            { jobId: job.id },
            "markDead: job no longer 'running' — already reconciled/swept elsewhere, discarding this worker's dead-letter attempt"
        );
        return;
    }

    const pipeline = redis.multi();
    pipeline.lpush(DEAD_QUEUE, job.id);
    pipeline.ltrim(DEAD_QUEUE, 0, 9999);
    await pipeline.exec();
    const updated = { ...job, status: 'dead' };
    await publishJobEvent(updated, { error: handlerError.message });

    log.error({ retries: job.retry_count, error: handlerError.message },
        `Job moved to Dead Letter Queue after ${job.retry_count} retries`);
}


async function processOneJob(pickedQueue, jobId) {
    const job = await prisma.job.findUnique({ where: { id: jobId } });

    if (!job) {
        logger.error({ jobId }, "Job ID in Redis but not in DB — skipping");
        return;
    }

    const log = createJobLogger(job);
    log.info({ queue: pickedQueue }, "Job picked up");

    if (['completed', 'failed', 'dead'].includes(job.status)) {
        log.warn({ status: job.status }, "Skipping terminal job — possible duplicate push");
        return;
    }

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
        const claim = await prisma.job.updateMany({
            where: { id: jobId, status: 'running' },
            data: {
                status: 'completed',
                completed_at: new Date(),
                result_data: jobResult ?? {},
            }
        });

        if (claim.count === 0) {
            log.warn(
                { jobId },
                "Job finished, but was already swept/reconciled by another process — discarding this worker's result rather than overwriting"
            );
        }


        const completedJob = { ...job, status: 'completed', result_data: jobResult };
        await publishJobEvent(completedJob, { result: jobResult });
        log.info("Job completed successfully");

    } catch (handlerError) {

        try {
            if (handlerError.permanent) {
                await markDead(job, handlerError, log);
            } else if (job.retry_count < job.max_retries) {
                await scheduleRetry(job, handlerError, log);
            } else {
                await markDead(job, handlerError, log);
            }
        } catch (recoveryError) {
            logger.error({
                jobId: job.id,
                originalError: handlerError.message,
                recoveryError: recoveryError.message,
            }, "CRITICAL: failed to record job failure — job will rely on zombie hunter for recovery");
        }
    }
}


// Main worker loop


const BRPOP_TIMEOUT_S = 2; // seconds to wait before looping 

async function startWorker() {
    logger.info({
        workerType: WORKER_TYPE,
        queues: LISTEN_QUEUES,
        pid: process.pid,
        concurrency: CONCURRENCY,
    }, "Worker started");

    while (true) {
        //  wait for a concurrency slot
        await sem.acquire();

        //  pop a job from Redis
        let result;
        try {
            result = await queueClient.brpop(...LISTEN_QUEUES, BRPOP_TIMEOUT_S);
        } catch (redisError) {
            // release the slot we already acquired 
            sem.release();
            logger.error({ err: redisError.message },
                "Redis error on BRPOP — backing off 1s");
            await new Promise(r => setTimeout(r, 1000));
            continue;
        }

        // no job within timeout window
        if (!result) {
            sem.release(); // Return the slot 
            continue;
        }

        //  fire job in background
        const [pickedQueue, jobId] = result;

        processOneJob(pickedQueue, jobId)
            .catch(err => logger.error({ err: err.message, jobId },
                "Unhandled error in background job"))
            .finally(() => sem.release()); // return the slot
    }
}

startWorker();