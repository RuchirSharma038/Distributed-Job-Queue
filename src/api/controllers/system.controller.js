// src/api/controllers/system.controller.js
// All business logic lives in system_service.js and zombie.service.js.
// This file is purely the HTTP layer — parse request, call service, send response.

import { logger } from "../../config/logger.js";
import { getSystemMetricsService, getStatsService } from "../services/system_service.js";
import { sweepZombies } from "../services/zombie.service.js";

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/system/health
// Full system health: DB metrics, queue depths, alerts, scaling advice.
// ─────────────────────────────────────────────────────────────────────────────

export const getSystemHealth = async (req, res) => {
    const start = Date.now();
    try {
        const metrics = await getSystemMetricsService();

        res.status(200).json({
            ...metrics,
            timestamp:      new Date().toISOString(),
            responseTimeMs: Date.now() - start,
        });
    } catch (err) {
        logger.error({ err: err.message }, 'getSystemHealth error');
        // Return structured 503 so monitoring systems can parse the failure
        res.status(503).json({
            status:         'unhealthy',
            timestamp:      new Date().toISOString(),
            responseTimeMs: Date.now() - start,
            error:          err.message,
        });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// GET /api/system/stats
// Lighter endpoint used by the dashboard: job counts, queue depths, dead jobs.
// ─────────────────────────────────────────────────────────────────────────────

export const getStats = async (req, res) => {
    try {
        const stats = await getStatsService();
        res.status(200).json(stats);
    } catch (err) {
        logger.error({ err: err.message }, 'getStats error');
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// ─────────────────────────────────────────────────────────────────────────────
// POST /api/system/zombie-sweep
// Triggers the zombie hunter inline so the dashboard button works without
// needing to exec a child process or rely on the PM2 cron schedule.
// ─────────────────────────────────────────────────────────────────────────────

export const runZombieSweep = async (req, res) => {
    try {
        const timeoutMinutes = req.body?.timeoutMinutes ?? 15;
        const results        = await sweepZombies(timeoutMinutes, false);
        res.status(200).json({ message: 'Zombie sweep complete', ...results });
    } catch (err) {
        logger.error({ err: err.message }, 'runZombieSweep error');
        res.status(500).json({ error: 'Internal Server Error' });
    }
};