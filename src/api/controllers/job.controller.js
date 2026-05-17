import { validatePayload } from "../validators/jobValidators.js";
import { 
    createJobService, 
    replayDeadJobsService, 
    replaySingleJobService 
} from "../services/job_service.js";


export const createJob = async (req, res) => {
    try {
        const { jobType, payload } = req.body;
        if (!jobType || !payload) {
            return res.status(400).json({ error: "Missing jobType or payload" });

        }
        const validationError = validatePayload(jobType, payload);

        if (validationError) {
            
            return res.status(400).json({ error: validationError });
        }
        const job = await createJobService(jobType, payload);

        res.status(202).json({
            message: "Job accepted for processing",
            jobId: job.id
        });
    } catch (err) {
        console.error("Queue Error:", err);
        res.status(500).json({ error: "Internal Server Error" });
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
        console.error("Replay Error:", err);
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
        console.error("Single Replay Error:", err);
        res.status(500).json({ error: "Internal Server Error" });
    }
};