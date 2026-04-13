import redis from "../../config/redis.js";
import { v4 as uuidv4 } from 'uuid';
export const createJob = async (req, res) => {
    try {
        const { jobType, payload } = req.body;
        if (!jobType || !payload) {
            return res.status(400).json({ error: "Missing jobType or payload" });
        }

        const jobId = uuidv4();
        const jobData = { id: jobId, jobType, payload, status: 'queued' };

        await redis.lpush('main-queue', JSON.stringify(jobData));
        console.log("Pushed data");

        res.status(202).json({
            message: "Job accepted for processing",
            jobId: jobId
        });


    } catch (error) {
        console.error("Redis Error:", error);
        res.status(500).json({ error: "Internal Server Error" })
    }
}