import express from "express";
import {createJob, getJobByID,replayDeadJobs, replaySingleJob } from "../controllers/job.controller.js";
const router = express.Router();

router.post('/jobs',createJob);
router.post('/jobs/replay-dead', replayDeadJobs);
router.get('/jobs/:id',getJobByID);
router.post('/jobs/:id/replay',  replaySingleJob);


export default router;