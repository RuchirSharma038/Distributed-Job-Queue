import express from "express";
import {
    getSystemHealth, getStats,
    runZombieSweep,
} from "../controllers/system.controller.js";

const router = express.Router();

router.get('/health', getSystemHealth);

export default router;