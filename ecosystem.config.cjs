module.exports = {
    apps: [
        // 1. The API Server
        {
            name: "api-server",
            script: "./src/server.js",
            instances: 1,
            env: {
                NODE_ENV: "production",
            }
        },

        //2. I/O WORKER POOL
        {
            name: "worker-io",
            script: "./src/workers/index.js",
            instances: 2, 
            exec_mode: "fork",
            env: {
                QUEUE_NAME: "queue:io"
            }
        },

        // 3. Compute Work Pool
        {
            name:"worker-compute",
            script:"src/workers/index.js",
            instances:4,
            exec_mode:"fork",
            env:{
                QUEUE_NAME:"queue:compute"
            }
        }

    ]
}