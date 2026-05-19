import { getSystemMetricsService } from "../services/system_service.js";
import { logger } from "../../config/logger.js"; 

export const getSystemHealth = async (req, res) => {
    const requestStart = Date.now();

    try {
        //  call the service
        const metrics = await getSystemMetricsService();

        res.status(200).json({
            status: metrics.status,
            timestamp: new Date().toISOString(),
            responseTimeMs: Date.now() - requestStart,
            database: metrics.database,
            queues: metrics.queues,
            alerts: metrics.alerts,
            scaling: metrics.scaling,
        });

    } catch (err) {
        logger.error({ err: err.message }, "System health check failed");
        
        // broken health check
        res.status(503).json({
            status: 'unhealthy',
            timestamp: new Date().toISOString(),
            responseTimeMs: Date.now() - requestStart,
            error: "Failed to retrieve system metrics",
        });
    }
};