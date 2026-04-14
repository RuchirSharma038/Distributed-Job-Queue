import redis from "../config/redis.js";

async function startWorker() {
    console.log("Worker started!");
    while (true) {
        try {
            const result = await redis.brpop("main-queue", 0);
            if (result) {
                const [queueName, data] = result;
                const job = JSON.parse(data);
                console.log(`Received job: ${job.id} | Type: ${job.jobType}`);
                console.log(`Processing from ${queueName}: ${data}`);

                //To simulate actual worker doing work
                await new Promise(resolve => setTimeout(resolve, 3000));
                job.status = "Completed";

                console.log(`Successfully processed Job ID: ${job.id} (Status: ${job.status})`);
            }

        } catch (error) {
            console.error("Redis Error:", error);
            //Short sleep to simulate error resolution
            await new Promise(resolve => setTimeout(resolve, 1000));

        }
    }

}
startWorker();