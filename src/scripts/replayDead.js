import 'dotenv/config';

import redis from '../config/redis.js';
import prisma from '../config/database.js';
import { QUEUE_ROUTING, DEAD_QUEUE } from '../config/constants.js';
import { logger } from '../config/logger.js';


// CLI argument handler (in case if we want to execute a specific job)

const args = process.argv.slice(2);

const isDryRun = !args.includes('--execute');

const specificId = (() => {
    const i = args.indexOf('--id');
    return i !== -1 ? args[i + 1] : null;
})();


// Replay logic

// Replaying a single Job
async function replaySingleJob(jobId, dryRun) {
    const job = await prisma.job.findUnique({
        where: { id: jobId },
        select: { id: true, type: true, status: true, retry_count: true, error_message: true }
    });

    if (!job) {
        logger.warn(`  [SKIP] ${jobId} — not found in database`);
        return { result: 'skipped', reason: 'not found in DB' };
    }

    if (job.status !== 'dead') {
        logger.warn(`  [SKIP] ${jobId} — status is '${job.status}', not 'dead'`);
        return { result: 'skipped', reason: `status is '${job.status}'` };
    }
    const targetQueue = QUEUE_ROUTING[job.type];

    if (!targetQueue) {
        console.error(`  [ERROR] ${jobId} — unknown type '${job.type}', no queue mapping`);
        return { result: 'error', reason: `no queue mapping for type '${job.type}'` };
    }
    logger.info(`  [${dryRun ? 'DRY RUN' : 'REPLAY'}] ${jobId}`);
    logger.info(`    type:        ${job.type}`);
    logger.info(`    was retried: ${job.retry_count}x`);
    logger.info(`    last error:  ${job.error_message}`);
    logger.info(`    target:      ${targetQueue}`);

    if (dryRun) {
        return { result: 'would-replay', jobId, targetQueue };
    }

    //Reset postgres state
    await prisma.job.update({
        where: { id: jobId },
        data: {
            status: 'queued',
            retry_count: 0,
            error_message: null,
            next_retry_at: null,
            dead_at: null,
            started_at: null,
            completed_at: null,
        }
    });


    // Push to live queue
    await redis.lpush(targetQueue, jobId);

    await redis.lrem(DEAD_QUEUE, 1, jobId);

    logger.log(`Replayed to ${targetQueue}`);

    return { result: 'replayed', jobId, targetQueue };

}


async function main() {
    logger.log('   Dead Letter Queue Replay Tool    ');

    if (isDryRun) {
        logger.warn('NOTICE: Running in DRY RUN mode. Pass --execute to actually replay jobs.\n');
    }

    try {
        let jobIds;
        if (specificId) {
            logger.info(`Target: single job ${specificId}\n`);
            jobIds = [specificId];
        } else {
            jobIds = await redis.lrange(DEAD_QUEUE, 0, -1);
            logger.info(`Dead queue depth: ${jobIds.length} jobs\n`);

            if (jobIds.length === 0) {
                const orphanedDeadJobs = await prisma.job.findMany({
                    where: { status: 'dead' },
                    select: { id: true }
                });
                if (orphanedDeadJobs.length > 0) {
                    logger.info(`queue:dead is empty, but found ${orphanedDeadJobs.length} orphaned 'dead' jobs in Postgres.`);
                    logger.info(`These are jobs where the DB write succeeded but Redis LPUSH failed.`);
                    jobIds = orphanedDeadJobs.map(j => j.id);
                } else {
                    logger.info('Nothing to replay. Graveyard is empty.');
                    return;
                }
            }
        }

        const results = {replayed :0, skipped:0, error:0, 'would-replay':0};

        for(const jobId of jobIds){
            const outcome = await replaySingleJob(jobId, isDryRun);
            results[outcome.result]= (results[outcome.result]??0 )+1;
        }


    }finally{
        await prisma.$disconnect();
        redis.disconnect();
    }
}
main().catch(err=>{
    logger.error('\nFatal error:', err.message);
    process.exit(1);
})
