import redis from '../../config/redis.js';

const processChaos = async (payload, jobId) => {
    console.log(`[Chaos Handler] Executing job ${jobId}`);
    
    // Check how many times we've attempted this specific job
    const attemptKey = `chaos_attempts:${jobId}`;
    const attempts = await redis.incr(attemptKey);
    
    if (attempts < 3) {
        // Fail the first 2 times
        console.log(`[Chaos Handler] Intentionally failing attempt ${attempts}`);
        throw new Error(`Chaos induced failure (attempt ${attempts})`);
    }
    
    // Succeed on the 3rd time
    console.log(`[Chaos Handler] Succeeding on attempt ${attempts}`);
    return { success: true, message: 'Survived the chaos' };
};

export default processChaos;
