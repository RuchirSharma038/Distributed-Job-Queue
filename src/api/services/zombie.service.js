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


    if (!dryRun) {
        await prisma.job.updateMany({
            where: {
                status: { in: ['running', 'sweeping'] },
                started_at: { lt: threshold },
            },
            data: { status: 'sweeping' },
        });
    }

    const zombies = await prisma.job.findMany({
        where: dryRun
            ? { status: { in: ['running', 'sweeping'] }, started_at: { lt: threshold } }
            : { status: 'sweeping' },
    });

    const results = { found: zombies.length, retrying: 0, dead: 0, skipped: 0 };

    for (const zombie of zombies) {
        const aliveForMs = Date.now() - new Date(zombie.started_at).getTime();
        const aliveForMinutes = (aliveForMs / 60_000).toFixed(1);

        if (dryRun) { results.skipped++; continue; }

        const baseQueue = QUEUE_ROUTING[zombie.type];

        if (!baseQueue) {
            await prisma.job.update({
                where: { id: zombie.id },
                data: {
                    status: 'dead',
                    dead_at: new Date(),
                    error_message: `Zombie sweep: unknown job type '${zombie.type}'`,
                }
            });
            const pipeline = redis.multi();
            pipeline.lpush(DEAD_QUEUE, zombie.id);
            pipeline.ltrim(DEAD_QUEUE, 0, 9999);
            await pipeline.exec();
            results.dead++;
            continue;
        }

        if (zombie.retry_count < zombie.max_retries) {
            const newRetryCount = zombie.retry_count + 1;
            const nextRetryAt = new Date(calcNextRetryTimestamp(newRetryCount));

            await prisma.job.update({
                where: { id: zombie.id },
                data: {
                    status: 'retrying',
                    retry_count: newRetryCount,
                    next_retry_at: nextRetryAt,
                    error_message: `Zombie sweep: worker killed after ${aliveForMinutes}min (attempt ${newRetryCount})`,
                }
            });

            await redis.zadd(DELAYED_QUEUE, nextRetryAt.getTime(), zombie.id);
            results.retrying++;
        } else {
            await prisma.job.update({
                where: { id: zombie.id },
                data: {
                    status: 'dead',
                    dead_at: new Date(),
                    error_message: `Zombie sweep: worker killed after ${aliveForMinutes}min (retries exhausted)`,
                }
            });
            const pipeline = redis.multi();
            pipeline.lpush(DEAD_QUEUE, zombie.id);
            pipeline.ltrim(DEAD_QUEUE, 0, 9999); // Keep only the newest 10,000 items
            await pipeline.exec();
            results.dead++;
        }
    }

    logger.info({ ...results, dryRun, timeoutMinutes }, 'Zombie sweep complete');
    return results;
}