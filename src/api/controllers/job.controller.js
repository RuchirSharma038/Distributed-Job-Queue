import {
    validatePayload,
    validateRunAt,
    validatePriority,
} from "../validators/jobValidators.js";

import {
    createJobService,
    replayDeadJobsService,
    replaySingleJobService
} from "../services/job_service.js";

import prisma from "../../config/database.js";

import { logger } from "../../config/logger.js";


export const createJob = async (req, res) => {
    try {
        const { jobType, payload, runAt, priority } = req.body;
        if (!jobType || !payload) {
            return res.status(400).json({ error: "Missing jobType or payload" });

        }
        const payloadError = validatePayload(jobType, payload);
        if (payloadError) return res.status(400).json({ error: payloadError });

        const runAtError = validateRunAt(runAt);
        if (runAtError) return res.status(400).json({ error: runAtError });

        const priorityError = validatePriority(priority);
        if (priorityError) return res.status(400).json({ error: priorityError });


        const resolvedPriority = priority ?? DEFAULT_PRIORITY;
        const job = await createJobService(jobType, payload, runAt ?? null, resolvedPriority);

        const isScheduled = runAt != null;

        res.status(202).json({
            message: isScheduled
                ? `Job scheduled for ${new Date(runAt).toISOString()}`
                : 'Job accepted for processing',
            jobId: job.id,
            status: job.status,
            priority: resolvedPriority,
            pollUrl: `/api/jobs/${job.id}`,
            ...(isScheduled && { scheduledAt: new Date(runAt).toISOString() }),
        });
    } catch (err) {
        logger.error({ err: err.message }, "Queue Error in createJob controller");
        res.status(500).json({ error: "Internal Server Error" });
    }
};


// GET job by Id 

export const getJobByID = async (req, res) => {
    try {
        const { id } = req.params;

        const job = await prisma.job.findUnique({
            where: { id },

            select: {
                id: true,
                type: true,
                status: true,
                priority: true,
                retry_count: true,
                max_retries: true,
                error_message: true,
                result_data: true,
                scheduled_at: true,
                created_at: true,
                started_at: true,
                completed_at: true,
                dead_at: true,
                next_retry_at: true,

            }
        });

        if (!job) {
            return res.status(404).json({ error: `Job ${id} not found` });
        }

        const response = {
            jobId: job.id,
            type: job.type,
            status: job.status,
            priority: job.priority,
            retries: {
                count: job.retry_count,
                max: job.max_retries,
            },
            timestamps: {
                created: job.created_at,
                scheduled: job.scheduled_at ?? null,
                started: job.started_at ?? null,
                completed: job.completed_at ?? null,
                dead: job.dead_at ?? null,
            },
        }

        switch (job.status) {

            case 'scheduled': {

                const msUntilRun = new Date(job.scheduled_at).getTime() - Date.now();
                response.scheduledAt = job.scheduled_at;
                response.runsInMs = Math.max(0, msUntilRun);
                response.pollAgainInMs = Math.min(msUntilRun, 10_000); // poll max every 10s
                response.message = `Job is scheduled to run at ${job.scheduled_at.toISOString()}`;
                break;
            }

            case 'queued':
                response.pollAgainInMs = 2000;
                response.message = 'Job is waiting in queue';
                break;

            case 'running': {
                const runningForMs = Date.now() - new Date(job.started_at).getTime();
                response.pollAgainInMs = 3000;
                response.runningForMs = runningForMs;
                response.message = `Job has been processing for ${(runningForMs / 1000).toFixed(1)}s`;
                break;
            }

            case 'retrying':
                response.pollAgainInMs = job.next_retry_at ? Math.max(0, new Date(job.next_retry_at).getTime() - Date.now()) + 1000 : 5000;

                response.nextRetryAt = job.next_retry_at;
                response.message = `Attempt ${job.retry_count} failed — retrying at ${job.next_retry_at}`;
                response.lastError = job.error_message;
                break;

            case 'completed': {
                const processingMs = job.started_at && job.completed_at
                    ? new Date(job.completed_at).getTime() - new Date(job.started_at).getTime()
                    : null;
                response.processingTimeMs = processingMs;
                response.result = job.result_data; // The file URLs, scraped data, etc.
                response.message = 'Job completed successfully';
                break;
            }

            case 'failed':
                response.error = job.error_message;
                response.message = 'Job failed permanently';
                break;

            case 'dead':
                response.error = job.error_message;
                response.replayUrl = `/api/jobs/${job.id}/replay`;
                response.message = `Job exhausted ${job.retry_count} retries and was moved to the Dead Letter Queue. POST to replayUrl to try again.`;
                break;

            default:
                response.message = `Unknown status: ${job.status}`;


        }
        return res.status(200).json(response);
    } catch (err) {
        logger.error({ err: err.message, jobId: req.params.id }, "Job Fetch Error in controller");
        res.status(500).json({ error: "Internal Server Erorr" });
    }
};

export const replayDeadJobs = async (req, res) => {
    try {
        const result = await replayDeadJobsService();
        res.status(200).json({
            message: `Replay complete`,
            ...result
        });
    } catch (err) {
        logger.error({ err: err.message }, "Replay Error in controller");
        res.status(500).json({ error: "Internal Server Error" });
    }
};

export const replaySingleJob = async (req, res) => {
    try {
        const { id } = req.params;
        const result = await replaySingleJobService(id);
        if (result.notFound) return res.status(404).json({ error: "Job not found" });
        if (result.wrongStatus) return res.status(409).json({ error: `Job status is '${result.status}', not 'dead'` });
        res.status(200).json({ message: "Job re-queued", jobId: id, targetQueue: result.targetQueue });
    } catch (err) {
        logger.error({ err: err.message, jobId: req.params.id }, "Single Replay Error in controller");
        res.status(500).json({ error: "Internal Server Error" });
    }
};