import prisma from "../../config/database.js";
import redis  from "../../config/redis.js";
import {
    IO_BRPOP_QUEUES,
    COMPUTE_BRPOP_QUEUES,
    DELAYED_QUEUE,
    DEAD_QUEUE,
} from "../../config/constants.js";


// Thresholds 
const SCALE_THRESHOLDS = {
    io:      20,
    compute: 50,
};

const DEAD_QUEUE_ALERT_THRESHOLD = 10;


// getSystemMetricsService used by GET /api/system/health


export const getSystemMetricsService = async () => {
    const [dbMetrics, redisMetrics] = await Promise.all([
        getDbMetrics(),
        getRedisMetrics(),
    ]);

    const alerts        = buildAlerts(redisMetrics, dbMetrics);
    const overallStatus = alerts.some(a => a.severity === 'critical') ? 'degraded'
                        : alerts.some(a => a.severity === 'warning')  ? 'warning'
                        : 'healthy';

    return {
        status:   overallStatus,
        database: dbMetrics,
        queues:   redisMetrics,
        alerts,
        scaling:  buildScalingAdvice(redisMetrics),
    };
};


// getStatsService  used by GET /api/system/stats

export const getStatsService = async () => {
    const allQueues = [...IO_BRPOP_QUEUES, ...COMPUTE_BRPOP_QUEUES, DELAYED_QUEUE, DEAD_QUEUE];

    const [statusCounts, ...queueDepths] = await Promise.all([
        prisma.job.groupBy({ by: ['status'], _count: { id: true } }),
        ...allQueues.map(q =>
            q === DELAYED_QUEUE ? redis.zcard(q) : redis.llen(q)
        ),
    ]);

    const jobs = { queued: 0, running: 0, retrying: 0, completed: 0, dead: 0, scheduled: 0, total: 0 };
    for (const row of statusCounts) {
        const s = row.status;
        jobs[s]    = (jobs[s] ?? 0) + row._count.id;
        jobs.total += row._count.id;
    }

    const queues = {};
    allQueues.forEach((name, i) => { queues[name] = queueDepths[i]; });

    const deadJobs = await prisma.job.findMany({
        where:   { status: 'dead' },
        orderBy: { dead_at: 'desc' },
        take:    50,
        select: {
            id:            true,
            type:          true,
            priority:      true,
            error_message: true,
            retry_count:   true,
            dead_at:       true,
            created_at:    true,
        },
    });

    return { jobs, queues, deadJobs };
};

// Internal helpers

async function getDbMetrics() {
    const dbStart = Date.now();

    const statusCounts = await prisma.job.groupBy({
        by:     ['status'],
        _count: { id: true },
    });

    const byStatus = {};
    let total = 0;
    for (const row of statusCounts) {
        byStatus[row.status] = row._count.id;
        total               += row._count.id;
    }

    const oneHourAgo        = new Date(Date.now() - 60 * 60 * 1000);
    const completedLastHour = await prisma.job.count({
        where: { status: 'completed', completed_at: { gte: oneHourAgo } },
    });

    const avgResult = await prisma.$queryRaw`
        SELECT AVG(EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000) AS avg_ms
        FROM   "Job"
        WHERE  status       = 'completed'
        AND    started_at   IS NOT NULL
        AND    completed_at IS NOT NULL
        AND    completed_at >= NOW() - INTERVAL '1 hour'
    `;

    return {
        responseTimeMs: Date.now() - dbStart,
        jobs: {
            queued:    byStatus.queued     ?? 0,
            scheduled: byStatus.scheduled  ?? 0,
            running:   byStatus.running    ?? 0,
            retrying:  byStatus.retrying   ?? 0,
            completed: byStatus.completed  ?? 0,
            dead:      byStatus.dead       ?? 0,
            sweeping:  byStatus.sweeping   ?? 0,
            total,
        },
        throughput: {
            completedLastHour,
            avgProcessingMs: avgResult[0]?.avg_ms
                ? Math.round(Number(avgResult[0].avg_ms))
                : null,
        },
    };
}

async function getRedisMetrics() {
    const redisStart = Date.now();

    const [
        ioHigh, ioDefault, ioLow,
        compHigh, compDefault, compLow,
        dead, delayed,
    ] = await Promise.all([
        redis.llen('queue:io:high'),
        redis.llen('queue:io:default'),
        redis.llen('queue:io:low'),
        redis.llen('queue:compute:high'),
        redis.llen('queue:compute:default'),
        redis.llen('queue:compute:low'),
        redis.llen(DEAD_QUEUE),
        redis.zcard(DELAYED_QUEUE),
    ]);

    const ioTotal      = ioHigh + ioDefault + ioLow;
    const computeTotal = compHigh + compDefault + compLow;

    return {
        responseTimeMs: Date.now() - redisStart,

        // Per priority breakdown 
        'queue:io:high':          { depth: ioHigh },
        'queue:io:default':       { depth: ioDefault },
        'queue:io:low':           { depth: ioLow },
        'queue:io:total':         { depth: ioTotal },   

        'queue:compute:high':     { depth: compHigh },
        'queue:compute:default':  { depth: compDefault },
        'queue:compute:low':      { depth: compLow },
        'queue:compute:total':    { depth: computeTotal },

        'queue:delayed': {
            depth: delayed,
            type:  'sorted_set',
            note:  'Jobs waiting for retry backoff or scheduled execution',
        },
        'queue:dead': {
            depth: dead,
            type:  'list',
            note:  'Jobs awaiting manual replay',
        },
    };
}

function buildAlerts(redisMetrics, dbMetrics) {
    const alerts = [];

    const ioTotal      = redisMetrics['queue:io:total']?.depth      ?? 0;
    const computeTotal = redisMetrics['queue:compute:total']?.depth ?? 0;

    if (ioTotal > SCALE_THRESHOLDS.io) {
        alerts.push({
            severity: ioTotal > SCALE_THRESHOLDS.io * 3 ? 'critical' : 'warning',
            queue:    'queue:io',
            depth:    ioTotal,
            message:  `IO queue total depth (${ioTotal}) exceeds threshold (${SCALE_THRESHOLDS.io})`,
        });
    }

    if (computeTotal > SCALE_THRESHOLDS.compute) {
        alerts.push({
            severity: computeTotal > SCALE_THRESHOLDS.compute * 3 ? 'critical' : 'warning',
            queue:    'queue:compute',
            depth:    computeTotal,
            message:  `Compute queue total depth (${computeTotal}) exceeds threshold (${SCALE_THRESHOLDS.compute})`,
        });
    }

    const deadDepth = redisMetrics['queue:dead']?.depth ?? 0;
    if (deadDepth > DEAD_QUEUE_ALERT_THRESHOLD) {
        alerts.push({
            severity: 'warning',
            queue:    'queue:dead',
            depth:    deadDepth,
            message:  `${deadDepth} jobs in Dead Letter Queue — POST /api/jobs/replay-dead after investigating`,
        });
    }

    if ((dbMetrics.jobs.running ?? 0) > 0) {
        alerts.push({
            severity: 'info',
            message:  `${dbMetrics.jobs.running} job(s) currently running. Zombie Hunter clears stuck jobs after 15 min.`,
        });
    }

    return alerts;
}

function buildScalingAdvice(redisMetrics) {
    const advice = [];

    const ioTotal      = redisMetrics['queue:io:total']?.depth      ?? 0;
    const computeTotal = redisMetrics['queue:compute:total']?.depth ?? 0;

    if (ioTotal > SCALE_THRESHOLDS.io) {
        advice.push({
            action:  'scale_up',
            target:  'worker-io',
            reason:  `Total IO queue depth is ${ioTotal}. Add IO workers.`,
            command: 'pm2 scale worker-io +2',
        });
    } else if (ioTotal === 0) {
        advice.push({
            action:  'scale_down',
            target:  'worker-io',
            reason:  'IO queues are empty. Could reduce workers to save resources.',
            command: 'pm2 scale worker-io 1',
        });
    }

    if (computeTotal > SCALE_THRESHOLDS.compute) {
        advice.push({
            action:  'scale_up',
            target:  'worker-compute',
            reason:  `Total compute queue depth is ${computeTotal}. Add compute workers.`,
            command: 'pm2 scale worker-compute +2',
        });
    }

    if (advice.length === 0) {
        advice.push({ action: 'none', reason: 'Queue depths are within normal thresholds.' });
    }

    return advice;
}