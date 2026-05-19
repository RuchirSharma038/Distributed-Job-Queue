import prisma from "../../config/database.js";
import redis from "../../config/redis.js";
import { QUEUE_ROUTING, DELAYED_QUEUE, DEAD_QUEUE } from "../../config/constants.js";

const SCALE_THRESHOLDS = {
    'queue:io': 20,
    'queue:compute': 50,
};

const DEAD_QUEUE_ALERT_THRESHOLD = 10;

export const getSystemMetricsService = async () => {
    const [dbMetrics, redisMetrics] = await Promise.all([
        getDbMetrics(),
        getRedisMetrics(),
    ]);

    const alerts = buildAlerts(redisMetrics, dbMetrics);
    const overallStatus = alerts.some(a => a.severity === 'critical') ? 'degraded'
        : alerts.some(a => a.severity === 'warning') ? 'warning'
        : 'healthy';

    return {
        status: overallStatus,
        database: dbMetrics,
        queues: redisMetrics,
        alerts,
        scaling: buildScalingAdvice(redisMetrics),
    };
};

async function getDbMetrics() {
    const dbStart = Date.now();

    const statusCounts = await prisma.job.groupBy({
        by: ['status'],
        _count: { id: true },
    });

    const byStatus = {};
    let total = 0;
    for (const row of statusCounts) {
        byStatus[row.status] = row._count.id;
        total += row._count.id;
    }

    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
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
            queued: byStatus.queued ?? 0,
            running: byStatus.running ?? 0,
            retrying: byStatus.retrying ?? 0,
            completed: byStatus.completed ?? 0,
            failed: byStatus.failed ?? 0,
            dead: byStatus.dead ?? 0,
            sweeping: byStatus.sweeping ?? 0,   
            total,
        },
        throughput: {
            completedLastHour,
            avgProcessingMs: avgResult[0]?.avg_ms ? Math.round(Number(avgResult[0].avg_ms)) : null,
        },
    };
}

async function getRedisMetrics() {
    const redisStart = Date.now();

    const [ioDepth, computeDepth, deadDepth, delayedDepth] = await Promise.all([
        redis.llen('queue:io'),
        redis.llen('queue:compute'),
        redis.llen(DEAD_QUEUE),
        redis.zcard(DELAYED_QUEUE),   
    ]);

    return {
        responseTimeMs: Date.now() - redisStart,
        'queue:io': { depth: ioDepth, type: 'list' },
        'queue:compute': { depth: computeDepth, type: 'list' },
        'queue:delayed': { depth: delayedDepth, type: 'sorted_set', note: 'Awaiting backoff' },
        'queue:dead': { depth: deadDepth, type: 'list', note: 'Awaiting manual replay' },
    };
}

function buildAlerts(redisMetrics, dbMetrics) {
    const alerts = [];

    for (const [queueName, threshold] of Object.entries(SCALE_THRESHOLDS)) {
        const depth = redisMetrics[queueName]?.depth ?? 0;
        if (depth > threshold) {
            alerts.push({
                severity: depth > threshold * 3 ? 'critical' : 'warning',
                queue: queueName,
                depth,
                message: `${queueName} depth (${depth}) exceeds threshold (${threshold})`,
            });
        }
    }

    const deadDepth = redisMetrics['queue:dead']?.depth ?? 0;
    if (deadDepth > DEAD_QUEUE_ALERT_THRESHOLD) {
        alerts.push({
            severity: 'warning',
            queue: 'queue:dead',
            depth: deadDepth,
            message: `${deadDepth} jobs in Dead Letter Queue — run replay-dead after investigating`,
        });
    }

    if ((dbMetrics.jobs.running ?? 0) > 0) {
        alerts.push({
            severity: 'info',
            message: `${dbMetrics.jobs.running} job(s) currently running.`,
        });
    }

    return alerts;
}

function buildScalingAdvice(redisMetrics) {
    const advice = [];
    const ioDepth = redisMetrics['queue:io']?.depth ?? 0;
    const computeDepth = redisMetrics['queue:compute']?.depth ?? 0;

    if (ioDepth > SCALE_THRESHOLDS['queue:io']) {
        advice.push({
            action: 'scale_up', target: 'worker-io',
            reason: `Depth is ${ioDepth}. Add workers.`, command: 'pm2 scale worker-io +2',
        });
    }

    if (computeDepth > SCALE_THRESHOLDS['queue:compute']) {
        advice.push({
            action: 'scale_up', target: 'worker-compute',
            reason: `Depth is ${computeDepth}. Add workers.`, command: 'pm2 scale worker-compute +2',
        });
    }

    if (advice.length === 0) {
        advice.push({ action: 'none', reason: 'Queue depths are normal.' });
    }

    return advice;
}