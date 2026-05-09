// src/workers/scheduler.js
import redis from "../config/redis.js";
import prisma from "../config/database.js";
import { logger } from "../config/logger.js";
import { QUEUE_ROUTING, DELAYED_QUEUE } from "../config/constants.js";

const POLL_INTERVAL_MS = 5_000; // Check every 5 seconds

async function promoteReadyJobs() {
    const nowMs = Date.now();

    // ZRANGEBYSCORE returns all members with score between 0 and now.
    const readyJobIds = await redis.zrangebyscore(DELAYED_QUEUE, 0, nowMs);

    if (readyJobIds.length === 0) return;

    logger.info({ count: readyJobIds.length }, "Promoting delayed jobs to live queues");

    for (const jobId of readyJobIds) {
        try {
            // Fetch the job to find out which queue it belongs to.
            const job = await prisma.job.findUnique({
                where: { id: jobId },
                select: { id: true, type: true, status: true }
            });

            if (!job) {
                logger.warn({ jobId }, "Delayed job not found in DB — removing from sorted set");
                await redis.zrem(DELAYED_QUEUE, jobId);
                continue;
            }

            // Safety check: if the job was somehow cancelled or failed permanently
            // between scheduling and now, don't re-queue it.
            if (job.status === 'failed' || job.status === 'completed') {
                logger.warn({ jobId, status: job.status }, "Delayed job is already terminal — removing from sorted set");
                await redis.zrem(DELAYED_QUEUE, jobId);
                continue;
            }

            const targetQueue = QUEUE_ROUTING[job.type];

            if (!targetQueue) {
                logger.error({ jobId, type: job.type }, "No queue mapping for job type — cannot promote");
                await redis.zrem(DELAYED_QUEUE, jobId);
                continue;
            }

            // Atomic-ish promotion:
            // LPUSH first (job is available to workers), then ZREM.
            await redis.lpush(targetQueue, jobId);
            await redis.zrem(DELAYED_QUEUE, jobId);

            logger.info({ jobId, type: job.type, targetQueue }, "Job promoted from delayed to live queue");

        } catch (err) {
            // Don't let one bad job stop the rest from being promoted.
            logger.error({ jobId, err: err.message }, "Error promoting delayed job — will retry on next poll");
        }
    }
}

async function startScheduler() {
    logger.info({ pollIntervalMs: POLL_INTERVAL_MS }, "Scheduler started");

    // Run immediately on startup, then on interval.
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
