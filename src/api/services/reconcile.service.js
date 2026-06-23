import redis from "../../config/redis.js";
import prisma from "../../config/database.js";
import { logger } from "../../config/logger.js";
import {
    QUEUE_ROUTING,
    DELAYED_QUEUE,
    DEAD_QUEUE,
    DEFAULT_PRIORITY,
    getPriorityQueue,
} from "../../config/constants.js";

const STUCK_THRESHOLD_MS = 2 * 60 * 1000; // 2 minutes

async function reconcileStuckQueued(dryRun) {
    const threshold = new Date(Date.now() - STUCK_THRESHOLD_MS);

    const stuck = await prisma.job.findMany({
        where: { status: 'queued', created_at: { lt: threshold } },
        select: { id: true, type: true, priority: true },
    });

    let actedOn = 0;

    for (const job of stuck) {
        const baseQueue = QUEUE_ROUTING[job.type];
        if (!baseQueue) continue;
        const targetQueue = getPriorityQueue(baseQueue, job.priority ?? DEFAULT_PRIORITY);

        if (dryRun) {
            logger.warn({ jobId: job.id, targetQueue }, 'reconcile (dry-run): stuck queued job, would re-push');
            actedOn++;
            continue;
        }

        // Acquire mutex
        const locked = await prisma.job.updateMany({
            where: { id: job.id, status: 'queued' },
            data: { status: 'reconciling' },
        });

        if (locked.count === 0) {
            logger.info({ jobId: job.id },
                'reconcile: job status changed before lock — skipping stale candidate');
            continue;
        }

        // Redis write
        try {
            await redis.lpush(targetQueue, job.id);
        } catch (redisErr) {
            logger.error({ jobId: job.id, err: redisErr.message },
                'reconcile: redis push failed — releasing mutex back to queued');
            await prisma.job.updateMany({
                where: { id: job.id, status: 'reconciling' },
                data: { status: 'queued' },
            });
            continue;
        }

        //Release mutex
        const released = await prisma.job.updateMany({
            where: { id: job.id, status: 'reconciling' },
            data: { status: 'queued' },
        });
        if (released.count === 0) {
            logger.info({ jobId: job.id },
                'reconcile: worker claimed job during mutex window — correct, no action needed');
        } else {
            logger.warn({ jobId: job.id, targetQueue },
                'reconcile: stuck queued job re-pushed under mutex');
        }

        actedOn++;
    }

    return actedOn;
}

async function reconcileStuckScheduled(dryRun) {
    const threshold = new Date(Date.now() - STUCK_THRESHOLD_MS);

    const stuck = await prisma.job.findMany({
        where: { status: 'scheduled', created_at: { lt: threshold } },
        select: { id: true, scheduled_at: true },
    });
    let actedOn = 0;
    for (const job of stuck) {
        const score = job.scheduled_at ? job.scheduled_at.getTime() : Date.now();
        if (dryRun) {
            logger.warn({ jobId: job.id }, 'reconcile (dry-run): stuck scheduled job, would re-add');
            actedOn++;
            continue;
        }

        const locked = await prisma.job.updateMany({
            where: { id: job.id, status: 'scheduled' },
            data: { status: 'reconciling' },
        });

        if (locked.count === 0) {
            logger.info({ jobId: job.id },
                'reconcile: job status changed before lock — skipping');
            continue;
        }

        try {
            await redis.zadd(DELAYED_QUEUE, score, job.id);
        } catch (redisErr) {
            logger.error({ jobId: job.id, err: redisErr.message },
                'reconcile: redis zadd failed — releasing mutex back to scheduled');
            await prisma.job.updateMany({
                where: { id: job.id, status: 'reconciling' },
                data: { status: 'scheduled' },
            });
            continue;
        }

        const released = await prisma.job.updateMany({
            where: { id: job.id, status: 'reconciling' },
            data: { status: 'scheduled' },
        });

        if (released.count === 0) {
            logger.info({ jobId: job.id },
                'reconcile: scheduler or another process handled this job during mutex');
        } else {
            logger.warn({ jobId: job.id },
                'reconcile: stuck scheduled job re-added to delayed queue');
        }


        actedOn++;
    }

    return actedOn;
}

async function reconcileStuckRetrying(dryRun) {
    const threshold = new Date(Date.now() - STUCK_THRESHOLD_MS);

    const stuck = await prisma.job.findMany({
        where: {
            status: 'retrying',
            OR: [{ next_retry_at: null }, { next_retry_at: { lt: threshold } }],
        },
        select: { id: true, next_retry_at: true, retry_count: true },
    });
    let actedOn = 0;
    for (const job of stuck) {
        const score = job.next_retry_at && job.next_retry_at.getTime() > Date.now()
            ? job.next_retry_at.getTime()
            : Date.now();

        if (dryRun) {
            logger.warn({ jobId: job.id, retryCount: job.retry_count }, 'reconcile (dry-run): stuck retrying job, would re-add');
            actedOn++;
            continue;
        }

        const locked = await prisma.job.updateMany({
            where: { id: job.id, status: 'retrying' },
            data: { status: 'reconciling' },
        });

        if (locked.count === 0) {
            logger.info({ jobId: job.id },
                'reconcile: job status changed before lock — skipping');
            continue;
        }

        try {
            await redis.zadd(DELAYED_QUEUE, score, job.id);
        } catch (redisErr) {
            logger.error({ jobId: job.id, err: redisErr.message },
                'reconcile: redis zadd failed — releasing mutex back to retrying');
            await prisma.job.updateMany({
                where: { id: job.id, status: 'reconciling' },
                data: { status: 'retrying' },
            });
            continue;
        }

        const released = await prisma.job.updateMany({
            where: { id: job.id, status: 'reconciling' },
            data: { status: 'retrying' },
        });

        if (released.count === 0) {
            logger.info({ jobId: job.id },
                'reconcile: worker claimed job during mutex window — correct');
        } else {
            logger.warn({ jobId: job.id, retryCount: job.retry_count },
                'reconcile: stuck retrying job re-added to delayed queue');
        }
        actedOn++;

    }

    return actedOn;
}

async function reconcilePhantomDeadEntries(dryRun) {
    const deadQueueIds = await redis.lrange(DEAD_QUEUE, 0, -1);
    if (deadQueueIds.length === 0) return 0;

    const records = await prisma.job.findMany({
        where: { id: { in: deadQueueIds } },
        select: { id: true, status: true },
    });
    const statusById = new Map(records.map(r => [r.id, r.status]));

    let phantomCount = 0;
    for (const id of deadQueueIds) {
        const initialStatus = statusById.get(id);
        if (initialStatus === 'dead') continue;

        if (dryRun) {
            logger.warn({ jobId: id, actualStatus: initialStatus ?? 'NOT_FOUND' }, 'reconcile (dry-run): would remove phantom queue:dead entry');
            phantomCount++;
            continue;
        }
        const fresh = await prisma.job.findUnique({ where: { id }, select: { status: true } });

        if (!fresh) {

            logger.warn({ jobId: id }, 'reconcile: removing phantom queue:dead entry — no Postgres record');
            await redis.lrem(DEAD_QUEUE, 1, id);
            phantomCount++;
            continue;
        }

        if (fresh.status === 'dead') {

            logger.info({ jobId: id }, 'reconcile: job became dead since batch read — not a phantom, skipping');
            continue;
        }
        logger.warn({ jobId: id, actualStatus: fresh.status }, 'reconcile: removing phantom queue:dead entry');
        await redis.lrem(DEAD_QUEUE, 1, id);
        phantomCount++;
    }
    return phantomCount;
}


// Public entrypoint 

export async function runReconciliation(dryRun = false) {
    const queuedCount = await reconcileStuckQueued(dryRun);
    const scheduledCount = await reconcileStuckScheduled(dryRun);
    const retryingCount = await reconcileStuckRetrying(dryRun);
    const phantomCount = await reconcilePhantomDeadEntries(dryRun);

    const results = {
        stuckQueued: queuedCount,
        stuckScheduled: scheduledCount,
        stuckRetrying: retryingCount,
        phantomDead: phantomCount,
        total: queuedCount + scheduledCount + retryingCount + phantomCount,
    };

    logger.info({ ...results, dryRun }, 'Reconciliation complete');
    return results;
}


export async function getReconciliationStatus() {
    const threshold = new Date(Date.now() - STUCK_THRESHOLD_MS);

    const [stuckQueued, stuckScheduled, stuckRetrying, stuckReconciling] = await Promise.all([
        prisma.job.count({ where: { status: 'queued', created_at: { lt: threshold } } }),
        prisma.job.count({ where: { status: 'scheduled', created_at: { lt: threshold } } }),
        prisma.job.count({
            where: {
                status: 'retrying',
                OR: [{ next_retry_at: null }, { next_retry_at: { lt: threshold } }],
            }
        }),
        prisma.job.count({ where: { status: 'reconciling' } }),
    ]);

    const total = stuckQueued + stuckScheduled + stuckRetrying;

    return {
        stuckQueued,
        stuckScheduled,
        stuckRetrying,
        stuckReconciling,
        total,
        healthy: total === 0,
    };
}