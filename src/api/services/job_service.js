import redis from "../../config/redis.js";
import prisma from "../../config/database.js";

const QUEUE_ROUTING = {
    'SEND_EMAIL': 'queue:io',
    'SCRAPE_WEBSITE': 'queue:io',
    'PROCESS_IMAGE': 'queue:compute',
    'GENERATE_PDF': 'queue:compute'
};

export const createJobService = async (jobType, payload) => {
    //Segregrate by intensity according to QUEUE_ROUTING

    const targetQueue = QUEUE_ROUTING[jobType];
    
    if (!targetQueue) {
        throw new Error(`Invalid Job Type: ${jobType}`);
    }

    //Write to postgres
    const job = await prisma.job.create({
        data: {
            type: jobType,
            payload: payload,
            status: 'queued'
        }
    });


    //Write to redis
    await redis.lpush(targetQueue, job.id);

    console.log(`[Job Queued] ID: ${job.id} -> ${targetQueue}`);

    return job;


    
}