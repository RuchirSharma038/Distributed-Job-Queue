import express from "express";
import {
    getSystemHealth, getStats,
    runZombieSweep,
    runReconcile
} from "../controllers/system.controller.js";

const router = express.Router();

router.get('/health', getSystemHealth);
router.get('/stats', getStats);
router.post('/zombie-sweep', runZombieSweep);
router.post('/reconcile',    runReconcile); 

export default router;