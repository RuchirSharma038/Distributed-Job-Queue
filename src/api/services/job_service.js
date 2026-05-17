import redis from "../../config/redis.js";
import prisma from "../../config/database.js";
import { QUEUE_ROUTING, DEAD_QUEUE } from "../../config/constants.js";

const RESET_DATA = {
    status: 'queued',
    retry_count: 0,
    error_message: null,
    next_retry_at: null,
    dead_at: null,
    started_at: null,
    completed_at: null,
};

export const createJobService = async (jobType, payload) => {
    //Segregrate by intensity according to QUEUE_ROUTING

    const targetQueue = QUEUE_ROUTING[jobType];

    if (!targetQueue) {
        throw new Error(`Invalid Job Type: ${jobType}`);
    }

    //Write to postgres
    const job = await prisma.job.create({
        data: {
            type: jobType,
            payload: payload,
            status: 'queued'
        }
    });


    //Write to redis
    await redis.lpush(targetQueue, job.id);

    console.log(`[Job Queued] ID: ${job.id} -> ${targetQueue}`);

    return job;

};


export const replayDeadJobsService = async () => {

    const jobIds = await redis.lrange(DEAD_QUEUE, 0, -1);

    let replayed = 0, skipped = 0;

    for (const jobId of jobIds) {
        const job = await prisma.job.findUnique({
            where: { id: jobId },
            select: { id: true, type: true, status: true }
        });

        if (!job || job.status !== 'dead') {
            skipped++;
            continue;
        }

        const targetQueue = QUEUE_ROUTING[job.type];
        if (!targetQueue) {
            skipped++;
            continue;
        }

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
        select: { id: true, type: true, status: true }
    });

    if (!job) return { notFound: true };
    if (job.status !== 'dead') return { wrongStatus: true, status: job.status };

    const targetQueue = QUEUE_ROUTING[job.type];

    await prisma.job.update({ where: { id: jobId }, data: RESET_DATA });
    const pipeline = redis.multi();
    pipeline.lpush(targetQueue, jobId);
    pipeline.lrem(DEAD_QUEUE, 1, jobId);
    await pipeline.exec();

    return { targetQueue };

};