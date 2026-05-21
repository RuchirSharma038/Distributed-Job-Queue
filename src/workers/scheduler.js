import redis from "../config/redis.js";
import prisma from "../config/database.js";
import { logger } from "../config/logger.js";
import { QUEUE_ROUTING, DELAYED_QUEUE } from "../config/constants.js";

const POLL_INTERVAL_MS = 5_000; // Check every 5 seconds
const TERMINAL_STATUSES = new Set(['completed', 'failed', 'dead']);

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
                select: { id: true, type: true, status: true, scheduled_at: true, retry_count: true }
            });

            if (!job) {
                logger.warn({ jobId }, "Delayed job not found in DB — removing from sorted set");
                await redis.zrem(DELAYED_QUEUE, jobId);
                continue;
            }

            // Safety check
            if (TERMINAL_STATUSES.has(job.status)) {
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


            if (job.status === 'scheduled') {
                await prisma.job.update({
                    where: { id: jobId },
                    data: { status: 'queued' },
                });
            } else if (job.status === 'retrying') {
                await prisma.job.update({
                    where: { id: jobId },
                    data: { next_retry_at: null },
                });
            }


            // Atomic-ish promotion

            await redis.lpush(targetQueue, jobId);
            await redis.zrem(DELAYED_QUEUE, jobId);

            if (job.status === 'scheduled') {
                logger.info(
                    { jobId, type: job.type, targetQueue, scheduledAt: job.scheduled_at },
                    "Scheduler: promoted SCHEDULED job → live queue"
                );
            } else if (job.status === 'retrying') {
                logger.info(
                    { jobId, type: job.type, targetQueue, retryCount: job.retry_count },
                    `Scheduler: promoted RETRY job (attempt ${job.retry_count}) → live queue`
                );
            } else {

                logger.info(
                    { jobId, type: job.type, targetQueue, status: job.status },
                    "Scheduler: promoted job → live queue"
                );
            }

        } catch (err) {
            logger.error({ jobId, err: err.message }, "Scheduler: error promoting job — will retry on next poll");
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
