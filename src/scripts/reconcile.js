import 'dotenv/config';
import redis  from '../config/redis.js';
import prisma from '../config/database.js';
import { runReconciliation } from '../api/services/reconcile.service.js';

const isDryRun = !process.argv.slice(2).includes('--execute');

async function main() {
    console.log(`\nReconciliation — ${isDryRun ? 'DRY RUN' : 'LIVE EXECUTION'}\n`);
    const results = await runReconciliation(isDryRun);
    console.log('Results:', results);

    await prisma.$disconnect();
    redis.disconnect();
}

main().catch(err => {
    console.error('Fatal error:', err.message);
    process.exit(1);
});