import redis from "../config/redis.js";
import prisma from "../config/database.js";
import { handlers } from "./handlers/handlerMap.js";
import { logger } from "../config/logger.js";

const QUEUE_NAME = process.env.QUEUE_NAME || "queue:io";
async function startWorker() {
    logger.info({ queue: QUEUE_NAME }, `Worker started on PID ${process.pid}`);
    while (true) {
        try {
            //  Listen dynamically to whatever queue PM2 assigned this worker
            const result = await redis.brpop(QUEUE_NAME, 0);

            if (!result) {
                continue;
            }


            const [queueName, jobId] = result;
            logger.info({ jobId, queue: queueName }, "Job picked up");

            //  Fetch the "Source of Truth" from PostgreSQL
            const job = await prisma.job.findUnique({
                where: { id: jobId }
            });
            if (!job) {
                logger.error({ jobId }, "Job ID in Redis but not in DB — skipping");
                continue;
            }

            // State Management: Mark as running
            await prisma.job.update({
                where: { id: jobId },
                data: { status: 'running', started_at: new Date() }
            });

            // The Strategy Pattern (Replacing the setTimeout mock)
            const executeJob = handlers[job.type];



            try {
                if (!executeJob) {
                    throw new Error(`Fatal: No handler registered for ${job.type}`);
                }
                // Pass the JSON payload from the DB into the specific handler
                const jobResult = await executeJob(job.payload);

                //  Success State
                await prisma.job.update({
                    where: { id: jobId },
                    data: { status: 'completed', completed_at: new Date(), result_data: jobResult ?? {} }
                });
                jobLogger.info({ jobId, type: job.type }, ` Job completed successfully.`);

            } catch (handlerError) {
                //  Failure State (e.g., Ethereal login failed, Image was corrupt)
                await prisma.job.update({
                    where: { id: jobId },
                    data: { status: 'failed', error_message: handlerError.message }
                });
                logger.error({ jobId, type: job.type, err: handlerError.message }, "Job failed");
            }



        } catch (redisError) {
            logger.error({ err: redisError.message }, "Redis connection error — retrying in 1s");
            //Short sleep to simulate error resolution
            await new Promise(resolve => setTimeout(resolve, 1000));

        }
    }

}
startWorker();