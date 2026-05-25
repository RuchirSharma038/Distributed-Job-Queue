import { logger } from "../../config/logger.js";
import { getSystemMetricsService, getStatsService } from "../services/system_service.js";
import { sweepZombies } from "../services/zombie.service.js";


// GET /api/system/health

export const getSystemHealth = async (req, res) => {
    const start = Date.now();
    try {
        const metrics = await getSystemMetricsService();

        res.status(200).json({
            ...metrics,
            timestamp: new Date().toISOString(),
            responseTimeMs: Date.now() - start,
        });
    } catch (err) {
        logger.error({ err: err.message }, 'getSystemHealth error');

        res.status(503).json({
            status: 'unhealthy',
            timestamp: new Date().toISOString(),
            responseTimeMs: Date.now() - start,
            error: err.message,
        });
    }
};

// GET /api/system/stats

export const getStats = async (req, res) => {
    try {
        const stats = await getStatsService();
        res.status(200).json(stats);
    } catch (err) {
        logger.error({ err: err.message }, 'getStats error');
        res.status(500).json({ error: 'Internal Server Error' });
    }
};

// POST /api/system/zombie-sweep

export const runZombieSweep = async (req, res) => {
    try {
        const timeoutMinutes = req.body?.timeoutMinutes ?? 15;
        const results = await sweepZombies(timeoutMinutes, false);
        res.status(200).json({ message: 'Zombie sweep complete', ...results });
    } catch (err) {
        logger.error({ err: err.message }, 'runZombieSweep error');
        res.status(500).json({ error: 'Internal Server Error' });
    }
};