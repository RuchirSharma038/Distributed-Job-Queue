import 'dotenv/config';
import redis from '../config/redis.js';
import prisma from '../config/database.js';
import { QUEUE_ROUTING, DEAD_QUEUE, DEFAULT_PRIORITY, getPriorityQueue } from '../config/constants.js';
import { logger } from '../config/logger.js';

const args = process.argv.slice(2);
const isDryRun = !args.includes('--execute');
const specificId = (() => {
    const i = args.indexOf('--id');
    return i !== -1 ? args[i + 1] : null;
})();

async function replaySingleJob(jobId, dryRun) {
    const job = await prisma.job.findUnique({
        where: { id: jobId },
        select: {
            id: true, type: true, status: true, priority: true,
            retry_count: true, error_message: true
        },
    });

    if (!job) {
        logger.warn({ jobId }, '[SKIP] not found in database');
        return { result: 'skipped', reason: 'not found in DB' };
    }

    if (job.status !== 'dead') {
        logger.warn({ jobId, status: job.status }, '[SKIP] status is not dead');
        return { result: 'skipped', reason: `status is '${job.status}'` };
    }

    const baseQueue = QUEUE_ROUTING[job.type];
    if (!baseQueue) {
        logger.error({ jobId, type: job.type }, '[ERROR] no queue mapping for job type');
        return { result: 'error', reason: `no queue mapping for type '${job.type}'` };
    }


    const targetQueue = getPriorityQueue(baseQueue, job.priority ?? DEFAULT_PRIORITY);

    logger.info({
        jobId, mode: dryRun ? 'DRY RUN' : 'REPLAY',
        type: job.type, retryCount: job.retry_count,
        lastError: job.error_message, targetQueue,
    }, dryRun ? '[DRY RUN] would replay' : '[REPLAY] replaying');

    if (dryRun) return { result: 'would-replay', jobId, targetQueue };

    //  CAS guard 
    const reset = await prisma.job.updateMany({
        where: { id: jobId, status: 'dead' },
        data: {
            status: 'queued',
            retry_count: 0,
            error_message: null,
            next_retry_at: null,
            dead_at: null,
            started_at: null,
            completed_at: null,
        },
    });

    if (reset.count === 0) {
        logger.warn({ jobId }, '[SKIP] job status changed between read and reset — skipping');
        return { result: 'skipped', reason: 'status changed before reset' };
    }

    //  atomic LPUSH + LREM in one pipeline
    const pipeline = redis.multi();
    pipeline.lpush(targetQueue, jobId);
    pipeline.lrem(DEAD_QUEUE, 1, jobId);
    await pipeline.exec();


    logger.info({ jobId, targetQueue }, '[REPLAYED]');
    return { result: 'replayed', jobId, targetQueue };
}

async function main() {
    logger.info({ mode: isDryRun ? 'DRY RUN' : 'LIVE', specificId },
        'Dead Letter Queue Replay Tool starting');

    if (isDryRun) {
        logger.warn('Running in DRY RUN mode — pass --execute to actually replay jobs');
    }

    try {
        let jobIds;

        if (specificId) {
            logger.info({ jobId: specificId }, 'Target: single job');
            jobIds = [specificId];
        } else {
            jobIds = await redis.lrange(DEAD_QUEUE, 0, -1);
            logger.info({ depth: jobIds.length }, 'Dead queue depth');

            if (jobIds.length === 0) {
                // Postgres fallback
                const orphans = await prisma.job.findMany({
                    where: { status: 'dead' },
                    select: { id: true },
                });
                if (orphans.length > 0) {
                    logger.info({ count: orphans.length },
                        'queue:dead is empty but found orphaned dead jobs in Postgres — replaying those');
                    jobIds = orphans.map(j => j.id);
                } else {
                    logger.info('Nothing to replay — graveyard is empty');
                    return;
                }
            }
        }

        const results = { replayed: 0, skipped: 0, error: 0, 'would-replay': 0 };

        for (const jobId of jobIds) {
            try {
                const outcome = await replaySingleJob(jobId, isDryRun);
                results[outcome.result] = (results[outcome.result] ?? 0) + 1;
            } catch (err) {
               
                logger.error({ jobId, err: err.message }, '[ERROR] Failed to execute replay for job');
                results.error += 1;
            }
        }


        logger.info({ results }, 'Replay complete');

        if (isDryRun && results['would-replay'] > 0) {
            logger.warn('Dry run complete — pass --execute to commit these replays');
        }

    } finally {
        await prisma.$disconnect();
        redis.disconnect();
    }
}

main().catch(err => {
    logger.error({ err: err.message }, 'Fatal error in replay script');
    process.exit(1);
});