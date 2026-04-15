import { createJobService } from "../services/job_service.js";

export const createJob = async (req, res) => {
    try {
        const { jobType, payload } = req.body;
        if (!jobType || !payload) {
            return res.status(400).json({ error: "Missing jobType or payload" });

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