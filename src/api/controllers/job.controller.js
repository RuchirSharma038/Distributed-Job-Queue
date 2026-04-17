import { createJobService } from "../services/job_service.js";
import { validatePayload } from "../validators/jobValidators.js";
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
}