import 'dotenv/config';
import redis from '../config/redis.js';
import prisma from '../config/database.js';
import {
    QUEUE_ROUTING,
    DELAYED_QUEUE,
    DEAD_QUEUE,
    RETRY_BASE_DELAY_MS,
} from '../config/constants.js';
import { logger } from '../config/logger.js';

const args = process.argv.slice(2);
const isDryRun = !args.includes('--execute');

const timeoutIdx = args.indexOf('--timeout');
const TIMEOUT_MINUTES = timeoutIdx !== -1 ? parseInt(args[timeoutIdx + 1], 10) : 15;

if (isNaN(TIMEOUT_MINUTES) || TIMEOUT_MINUTES < 1) {
    logger.fatal('Error: --timeout must be a positive integer (minutes)');
    process.exit(1);
}


//Retry helpers

function calcNextRetryTimestamp(retryCount) {
    const delayMs = RETRY_BASE_DELAY_MS * Math.pow(2, retryCount - 1);
    return Date.now() + delayMs;
}

//Process a single claimed zombie

async function processZombie(job, dryRun) {
    const aliveForMs = Date.now() - new Date(job.started_at).getTime();

    const aliveForMinutes = (aliveForMs / 60000).toFixed(1);

    const logContext = {
        jobId: job.id,
        type: job.type,
        stuckForMinutes: aliveForMinutes,
        retries: `${job.retry_count}/${job.max_retries}`
    };

    if (dryRun) {
        const wouldDo = job.retry_count < job.max_retries ? 'schedule retry' : 'move to DLQ';
        logger.info({ ...logContext, wouldDo }, "Dry Run: Evaluated zombie job");
        return { result: 'would-act' };
    }

    const targetQueue = QUEUE_ROUTING[job.type];

    if (!targetQueue) {
        await prisma.job.update({
            where: { id: job.id },
            data: {
                status: 'dead',
                dead_at: new Date(),
                error_message: `Zombie sweep: unknown job type '${job.type}'`,
            }
        });

        const pipeline = redis.multi();
        pipeline.lpush(DEAD_QUEUE, job.id);
        pipeline.ltrim(DEAD_QUEUE, 0, 9999);
        await pipeline.exec();

        logger.error(logContext, "Zombie swept to DLQ (Unknown job type)");
        return { result: 'dead' };
    }

    if (job.retry_count < job.max_retries) {

        const newRetryCount = job.retry_count + 1;

        const nextRetryAt = new Date(calcNextRetryTimestamp(newRetryCount));

        await prisma.job.update({
            where: { id: job.id },
            data: {
                status: 'retrying',
                retry_count: newRetryCount,
                next_retry_at: nextRetryAt,
                error_message: `Zombie sweep: worker process killed after ${aliveForMinutes}min (attempt ${newRetryCount})`,
            }
        });

        await redis.zadd(DELAYED_QUEUE, nextRetryAt.getTime(), job.id);

        logger.warn({ ...logContext, targetQueue, nextRetryAt }, "Zombie recovered and sent to delayed queue");
        return { result: 'retrying' };

    } else {
        await prisma.job.update({
            where: { id: job.id },
            data: {
                status: 'dead',
                dead_at: new Date(),
                error_message: `Zombie sweep: worker process killed after ${aliveForMinutes}min (retries exhausted)`,
            }
        });
        const pipeline = redis.multi();
        pipeline.lpush(DEAD_QUEUE, job.id);
        pipeline.ltrim(DEAD_QUEUE, 0, 9999); 
        await pipeline.exec();

        logger.error(logContext, "Zombie swept to DLQ (Retries exhausted)");
        return { result: 'dead' };
    }


}


// Main

async function main() {
    logger.info({
        mode: isDryRun ? 'DRY RUN' : 'LIVE EXECUTION',
        timeoutMinutes: TIMEOUT_MINUTES
    }, "Starting Zombie Hunter Sweep");

    const threshold = new Date(Date.now() - TIMEOUT_MINUTES * 60 * 1000);


    try {
        // Step 1 -> Atomic claimed
        if (!isDryRun) {
            const claimed = await prisma.job.updateMany({
                where: {
                    status: { in: ['running', 'sweeping'] },
                    started_at: { lt: threshold },
                },
                data: { status: 'sweeping' },
            });

            if (claimed.count === 0) {
                logger.info("No zombies found to claim. System looks healthy.");
                return;
            }

            logger.info({ claimedCount: claimed.count }, "Successfully locked zombie jobs for sweep");
        }

        //Step 2 -> fetch the claimed set
        const zombies = await prisma.job.findMany({
            where: isDryRun
                ? { status: { in: ['running', 'sweeping'] }, started_at: { lt: threshold } }
                : { status: 'sweeping' },
        });

        if (isDryRun && zombies.length === 0) {
            logger.info("No zombies found. System looks healthy.");
            return;
        }




        // Step 3-> process each zombie
        const tally = { retrying: 0, dead: 0, 'would-act': 0 };

        for (const zombie of zombies) {
            const outcome = await processZombie(zombie, isDryRun);
            tally[outcome.result] = (tally[outcome.result] ?? 0) + 1;
        }

        logger.info({ summary: tally }, "Zombie Hunter Sweep Completed");

        if (isDryRun) {
            logger.warn("Reminder: Run with --execute to commit these changes.");
        }

    } finally {
        await prisma.$disconnect();
        redis.disconnect();
    }
}

main().catch(err => {
    logger.fatal({ err: err.message }, "Zombie Hunter failed with fatal error");
    process.exit(1);
})