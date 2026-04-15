// src/scripts/flush.js
import 'dotenv/config';
import prisma from '../config/database.js';
import redis from "../config/redis.js";

async function flushSystem() {
    console.log("Sweeping the system...");

    try {
        //  WIPE POSTGRESQL 
        const deletedJobs = await prisma.job.deleteMany({});
        console.log(` Deleted ${deletedJobs.count} old jobs from PostgreSQL.`);

        //  WIPE REDIS 
        await redis.del('queue:io', 'queue:compute');
        console.log(` Emptied queue:io and queue:compute in Redis.`);

    } catch (error) {
        console.error("Failed to flush system:", error);
    } finally {
        //  CLEAN UP CONNECTIONS
        
        await prisma.$disconnect();
        redis.disconnect();
        console.log(" System completely reset!");
    }
}

flushSystem();