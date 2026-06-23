import redis from "../../config/redis.js";
import prisma from "../../config/database.js";
import { logger } from "../../config/logger.js";
import {
    QUEUE_ROUTING,
    DELAYED_QUEUE,
    DEAD_QUEUE,
    RETRY_BASE_DELAY_MS,
    DEFAULT_PRIORITY,
    getPriorityQueue,
} from "../../config/constants.js";

const TERMINAL_STATUSES = new Set(['completed', 'failed', 'dead']);

function calcNextRetryTimestamp(retryCount) {
    return Date.now() + RETRY_BASE_DELAY_MS * Math.pow(2, retryCount - 1);
}

export async function sweepZombies(timeoutMinutes = 15, dryRun = false) {
    const threshold = new Date(Date.now() - timeoutMinutes * 60 * 1000);

    //  Atomic claim 
   
    const claimed = await prisma.job.updateMany({
        where: {
            status: { in: ['running', 'sweeping', 'reconciling'] },
            started_at: { lt: threshold },
        },
        data: { status: 'sweeping' },
    });

    if (claimed.count === 0) {
        logger.info({ dryRun, threshold }, 'Zombie sweep: no candidates found');
        return { swept: 0, retried: 0, killed: 0 };
    }

    // fetch claimed jobs and route each one 
    const zombies = await prisma.job.findMany({
        where: { status: 'sweeping' },
    });

    let retried = 0;
    let killed = 0;

    for (const zombie of zombies) {
        const isReconcilerZombie = zombie.status === 'sweeping' &&
            zombie.started_at < threshold;

        if (dryRun) {
            logger.warn({ jobId: zombie.id, retryCount: zombie.retry_count },
                'Zombie sweep (dry-run): would process zombie');
            continue;
        }

        if (zombie.retry_count < zombie.max_retries) {
            const newRetryCount = zombie.retry_count + 1;
            const nextRetryAt = new Date(
                Date.now() + RETRY_BASE_DELAY_MS * Math.pow(2, newRetryCount - 1)
            );

            await prisma.job.update({
                where: { id: zombie.id },
                data: {
                    status: 'retrying',
                    retry_count: newRetryCount,
                    next_retry_at: nextRetryAt,
                    error_message: 'Worker or reconciler process died mid-execution',
                },
            });
            await redis.zadd(DELAYED_QUEUE, nextRetryAt.getTime(), zombie.id);
            retried++;

            logger.warn({ jobId: zombie.id, attempt: newRetryCount },
                'Zombie sweep: job re-queued for retry');
        } else {
            await prisma.job.update({
                where: { id: zombie.id },
                data: {
                    status: 'dead',
                    dead_at: new Date(),
                    error_message: 'Exhausted retries — worker or reconciler process died mid-execution',
                },
            });
            await redis.lpush(DEAD_QUEUE, zombie.id);
            killed++;

            logger.error({ jobId: zombie.id, retries: zombie.retry_count },
                'Zombie sweep: job moved to DLQ after retry exhaustion');
        }
    }

    logger.info({ swept: claimed.count, retried, killed, dryRun },
        'Zombie sweep complete');

    return { swept: claimed.count, retried, killed };

}