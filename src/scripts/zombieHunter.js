import 'dotenv/config';
import redis from '../config/redis.js';
import prisma from '../config/database.js';
import { logger } from '../config/logger.js';
import { sweepZombies } from '../api/services/zombie.service.js';

const args = process.argv.slice(2);
const isDryRun = !args.includes('--execute');

const timeoutIdx = args.indexOf('--timeout');
const TIMEOUT_MINUTES = timeoutIdx !== -1 ? parseInt(args[timeoutIdx + 1], 10) : 15;

if (isNaN(TIMEOUT_MINUTES) || TIMEOUT_MINUTES < 1) {
    logger.fatal('Error: --timeout must be a positive integer (minutes)');
    process.exit(1);
}


async function main() {
    logger.info({
        mode: isDryRun ? 'DRY RUN' : 'LIVE EXECUTION',
        timeoutMinutes: TIMEOUT_MINUTES
    }, "Starting Zombie Hunter Sweep");

    try {
       
        const results = await sweepZombies(TIMEOUT_MINUTES, isDryRun);

        logger.info({ summary: results }, "Zombie Hunter Sweep Completed");

        if (isDryRun) {
            logger.warn("Reminder: Run with --execute to commit these changes.");
        }

    } finally {
        //  cleanup 
        await prisma.$disconnect();
        redis.disconnect();
    }
}

main().catch(err => {
    logger.fatal({ err: err.message }, "Zombie Hunter failed with fatal error");
    process.exit(1);
});