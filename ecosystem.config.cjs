module.exports = {
    apps: [
        // 1. The API Server
        {
            name: "api-server",
            script: "./src/server.js",
            instances: 2,
            exec_mode: 'cluster',

            env: {
                NODE_ENV: "production",
                PORT: 3002,
            }
        },

        //2. I/O WORKER POOL
        {
            name: "worker-io",
            script: "./src/workers/index.js",
            instances: 2,
            exec_mode: "fork",
            env: {
                NODE_ENV: 'production',
                WORKER_TYPE: 'io',
                QUEUE_NAME: "queue:io",
                WORKER_CONCURRENCY: 20
            }
        },

        // 3. Compute Work Pool
        {
            name: "worker-compute",
            script: "src/workers/index.js",
            instances: 4,
            exec_mode: "fork",
            env: {
                NODE_ENV: 'production',
                WORKER_TYPE: 'compute',
                QUEUE_NAME: "queue:compute"
            }
        },

        // 4. Delayed Queue Scheduler
        {
            name: "scheduler",
            script: "./src/workers/scheduler.js",
            instances: 1,
            exec_mode: "fork"
        },

        // Zombie Hunter
        {
            name: 'zombie-hunter',
            script: './src/scripts/zombieHunter.js',
            instances: 1,
            exec_mode: 'fork',
            autorestart: false,
            cron_restart: '*/15 * * * *',
            args: '--execute',
            watch: false,
        },
        {
            name: 'reconciler',
            script: './src/scripts/reconcile.js',
            instances: 1,
            exec_mode: 'fork',
            autorestart: false,
            cron_restart: '*/10 * * * *',
            args: '--execute',
            watch: false,
        },


    ]
}