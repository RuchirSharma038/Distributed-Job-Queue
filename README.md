# Distributed Job Queue

A background job processing system built from scratch — no Bull, no Celery, no Sidekiq. The goal was to understand what those libraries are doing under the hood: how jobs survive crashes, why priority queues need separate Redis lists, and what actually happens when a worker process gets killed mid-job.

This is the kind of infrastructure Stripe uses to send payment confirmation emails, Shopify uses to generate merchant reports, and Uber uses to asynchronously process ride receipts.

---

## Architecture

```
                         ┌─────────────────────────┐
                         │         CLIENTS         │
                         │  REST API  ·  Dashboard │
                         └────────────┬────────────┘
                                      │
                         ┌────────────▼────────────┐
                         │       API SERVER        │
                         │  Express  ·  socket.io  │
                         │    PM2 cluster × 2      │
                         └──────┬────────┬─────────┘
                                │        |
               ┌────────────────▼─┐  ┌───▼────────────────────────── ┐
               │    PostgreSQL    |  |           Redis               │
               │                  │  │                               │
               │  source of truth │  │  queue:io:high                │
               │  job status      │  │  queue:io:default             │
               │  retry_count     │  │  queue:io:low                 │
               │  result_data     │  │  queue:compute:high           │
               │  priority        │  │  queue:compute:default        │
               │  scheduled_at    │  │  queue:compute:low            │
               └────────────────┬─┘  │  queue:delayed  (sorted set)  │
                                │    │  queue:dead     (list)        │
                                │    │  job_updates    (pub/sub)     │
                                │    └───┬────────────────────────── ┘
                                │        │
               ┌────────────────┼────────┼────────────────────────────┐
               │                │        │      WORKER POOL (PM2)     │
               │    ┌───────────▼──────┐ │ ┌───────────────────────┐  │
               │    │  worker-io × 2   │ │ │  worker-compute × 4   │  │
               │    │                  │ │ │                       │  │
               │    │  emailHandler    │ │ │  imageHandler         │  │
               │    │  scraperHandler  │ │ │  pdfGenHandler        │  │
               │    └──────────────────┘ │ └───────────────────────┘  │
               │                         │                            │
               │    ┌────────────────┐   │  ┌─────────────────────┐   │
               │    │   scheduler    │   │  │   zombie-hunter     │   │
               │    │  (poll 5s)     │   │  │  (cron every 15m)   │   │
               │    └────────────────┘   │  └─────────────────────┘   │
               └───────────────────────────────────────────────────── ┘
```

### How a job moves through the system

```
  POST /api/jobs
       │
       ▼
  Validate payload + priority + runAt
       │
       ├── runAt in future? ──► ZADD queue:delayed (score = runAt ms)
       │                              │
       │                        scheduler polls every 5s
       │                              │
       └── immediate? ────────► LPUSH queue:{type}:{priority}
                                      │
                                 BRPOP (worker picks up)
                                      │
                              Postgres: status = running
                                      │
                              Execute handler
                                      │
                    ┌─────────────────┴──────────────────┐
                    │ success                             │ failure
                    ▼                                     ▼
              status = completed               retry_count < max_retries?
              result_data saved                     │           │
              publish to job_updates               YES          NO
                                                   │            │
                                            ZADD delayed    LPUSH dead
                                            (backoff)       status = dead
```

---

## Why Two Stores

**Redis** is the dispatcher. BRPOP returns in microseconds. Workers never wait on a database query to find their next job. Redis is also expendable — if it restarts and loses all queue data, you can rebuild it from Postgres.

**PostgreSQL** is the record. Every state transition is durable. If the entire stack goes down, you can reconstruct the exact state of every job from Postgres and re-queue anything stuck in `queued` or `running`. This is the recovery script pattern.

Neither store can do the other's job well. Postgres with polling for job dispatch adds unacceptable latency and hammers the database. Redis alone gives you no audit trail, no result storage, and no way to query jobs by status.

---

## Job Lifecycle

| Status | Set by | Meaning |
|---|---|---|
| `scheduled` | API on submission | Future-dated job, sitting in `queue:delayed` |
| `queued` | API on submission (or scheduler on promotion) | In a live Redis queue, waiting for a worker |
| `running` | Worker on pickup | A worker process is actively executing the handler |
| `retrying` | Worker on handler failure | Failed, waiting in `queue:delayed` for backoff window |
| `completed` | Worker on success | Handler returned, `result_data` is populated |
| `dead` | Worker when retries exhausted | In `queue:dead`, awaiting manual replay |
| `sweeping` | Zombie hunter (transient) | Being claimed by the zombie hunter, resolves in seconds |

---

## Tech Stack

| Layer | Technology | Why |
|---|---|---|
| API server | Node.js + Express | Fast I/O, same language as workers |
| Queue | Redis (ioredis) | BRPOP is blocking and atomic, ZADD gives us scheduled execution for free |
| Database | PostgreSQL + Prisma | JSONB payload storage, full audit trail, group-by queries |
| Process management | PM2 | Fork mode for workers, cluster mode for the API, built-in cron for zombie hunter |
| Real-time | socket.io + Redis pub/sub | Workers are separate processes and cannot touch socket.io directly |
| Logging | Pino | Structured JSON logs with child loggers per job, ready for Datadog/CloudWatch |

---

## Features

**Priority queues** — Three tiers per worker type (`high`, `default`, `low`). Redis `BRPOP` checks queues left-to-right, so `queue:io:high` is completely drained before a worker ever touches `queue:io:default`. No priority inversion, no extra logic.

**Scheduled execution** — Submit a job with `runAt` and it goes straight to `queue:delayed` (a Redis sorted set where the score is the Unix timestamp). The scheduler polls every 5 seconds and promotes ready jobs to their live queue, preserving priority.

**Retry with exponential backoff** — Failed jobs go back to `queue:delayed` with a score of `now + baseDelay * 2^(attempt - 1)`. Attempt 1 waits 10s, attempt 2 waits 20s, attempt 3 waits 40s. The same sorted set handles both retries and scheduled jobs.

**Dead Letter Queue** — Jobs that exhaust their retry budget get pushed to `queue:dead` (a Redis list) and their DB status is set to `dead`. Nothing is deleted. A single API call or CLI command replays all dead jobs with a fresh retry budget.

**Zombie hunter** — Workers killed by the OS (OOM, SIGKILL, hardware fault) leave jobs stuck in `running` forever. The zombie hunter queries Postgres for `status = running AND started_at < 15 minutes ago`, atomically claims them with a `sweeping` status update, and sends them back through the retry loop.

**Idempotent handlers** — Every handler checks whether its output already exists before doing work. Image processing checks for existing output files, PDF generation checks for an existing file on disk, email uses a Redis key (`sent_email:{jobId}`) as an idempotency token. Safe to retry without side effects.

**Real-time dashboard** — Workers publish state changes to a Redis pub/sub channel. The API server subscribes and forwards events to connected browsers via socket.io. The dashboard shows live job status, queue depths, and a graveyard table of dead jobs.

---

## Setup

**Prerequisites:** Node.js 18+, Docker

```bash
# Start Postgres + Redis
docker compose up -d

# Install dependencies
npm install

# Create the database tables
npx prisma migrate dev --name initial

# Generate the Prisma client
npx prisma generate

# Copy and fill in the environment file
cp .env.example .env
```

**.env**
```env
DATABASE_URL="postgresql://admin:secretpassword@localhost:5432/job_queue"
REDIS_HOST=127.0.0.1
REDIS_PORT=6379
NODE_ENV=development
LOG_LEVEL=info
PORT=3002
```

**Start everything with PM2:**
```bash
pm2 start ecosystem.config.cjs
pm2 status
```

You should see 8 processes online: `api-server × 2`, `worker-io × 2`, `worker-compute × 4`, `scheduler × 1`, `zombie-hunter` (stopped between cron runs is correct).

**Open the dashboard:**
```
http://localhost:3002/static/dashboard.html
```

---

## API

### Submit a job

```bash
POST /api/jobs
Content-Type: application/json

{
  "jobType": "SCRAPE_WEBSITE",
  "payload": {
    "url": "https://books.toscrape.com/catalogue/a-light-in-the-attic_1000/index.html",
    "targetSelector": ".price_color"
  },
  "priority": "high",        # optional: high | default | low
  "runAt": "2025-01-20T09:00:00.000Z"  # optional: schedule for future
}
```

Response:
```json
{
  "message": "Job accepted for processing",
  "jobId": "a3f8c1d2-...",
  "status": "queued",
  "priority": "high",
  "pollUrl": "/api/jobs/a3f8c1d2-..."
}
```

### Poll for job status

```bash
GET /api/jobs/:id
```

The response shape varies by status. A completed scrape job returns:
```json
{
  "jobId": "a3f8c1d2-...",
  "status": "completed",
  "priority": "high",
  "processingTimeMs": 1834,
  "result": {
    "extractedText": "£53.74",
    "numericValue": 53.74,
    "scrapedAt": "2025-01-15T10:30:00.000Z"
  }
}
```

A job in the retry loop returns `pollAgainInMs` so the client knows when to check back:
```json
{
  "status": "retrying",
  "nextRetryAt": "2025-01-15T10:30:15.000Z",
  "pollAgainInMs": 14200,
  "lastError": "Selector '.price_color' found no data on the page"
}
```

### System health

```bash
GET /api/system/health   # full metrics + alerts + scaling advice
GET /api/system/stats    # job counts + queue depths + dead jobs list
```

### Dead Letter Queue

```bash
POST /api/jobs/replay-dead      # replay all dead jobs
POST /api/jobs/:id/replay       # replay one specific job
POST /api/system/zombie-sweep   # run zombie hunter immediately
```

### Supported job types

| Type | Required payload fields |
|---|---|
| `SEND_EMAIL` | `to`, `subject`, `body` |
| `SCRAPE_WEBSITE` | `url`, `targetSelector` |
| `PROCESS_IMAGE` | `inputPath`, `filename`, `operations` |
| `GENERATE_PDF` | `filename`, `invoiceData` |

---

## Fault Recovery

**After a 3rd-party API outage:**
```bash
# All jobs that failed during the outage land in queue:dead
# Wait for the service to recover, then:
curl -X POST http://localhost:3002/api/jobs/replay-dead

# Or use the npm script
npm run queue:replay-dead
```

**After a Redis wipe:**
Postgres still has every job and its last known status. Jobs stuck in `queued` can be re-pushed to Redis by querying `WHERE status = 'queued'` and running LPUSH for each.

**Manual zombie sweep:**
```bash
npm run queue:zombie-hunt:dry   # preview what would be swept
npm run queue:zombie-hunt       # execute
```

---

## Monitoring

The health endpoint is designed to give you a concrete scaling decision, not just numbers:

```json
{
  "status": "warning",
  "queues": {
    "queue:compute:high":    { "depth": 0 },
    "queue:compute:default": { "depth": 74 },
    "queue:compute:low":     { "depth": 12 },
    "queue:compute:total":   { "depth": 86 }
  },
  "alerts": [
    {
      "severity": "warning",
      "queue": "queue:compute",
      "depth": 86,
      "message": "Compute queue total depth (86) exceeds threshold (50)"
    }
  ],
  "scaling": [
    {
      "action": "scale_up",
      "target": "worker-compute",
      "reason": "Total compute queue depth is 86. Add compute workers.",
      "command": "pm2 scale worker-compute +2"
    }
  ]
}
```

Run `pm2 scale worker-compute +2`, watch the depth drop in real time via `pm2 monit`. Scale back down when it stabilizes.

**RedisInsight** is available at `http://localhost:5540` — connect to `127.0.0.1:6379` and you can inspect the actual job IDs in each queue, see the sorted set scores for delayed jobs, and verify the dead queue contents before replaying.

---

## Folder Structure

```
distributed-job-queue/
│
├── prisma/
│   ├── schema.prisma              Job model with all fields
│   ├── prisma.config.ts
│   └── migrations/
│       └── 0001_baseline/
│           └── migration.sql
│
├── src/
│   ├── server.js                  Express + HTTP server + socket.io setup
│   │
│   ├── api/
│   │   ├── controllers/
│   │   │   ├── job.controller.js       createJob, getJobById, replay endpoints
│   │   │   └── system.controller.js    health, stats, zombie sweep
│   │   ├── routes/
│   │   │   ├── job.routes.js
│   │   │   └── system.routes.js
│   │   ├── services/
│   │   │   ├── job_service.js          createJob, replayDead, replaySingle
│   │   │   ├── system_service.js       DB metrics, Redis depths, alerts
│   │   │   └── zombie.service.js       sweepZombies() — shared by CLI + API
│   │   └── validators/
│   │       └── jobValidators.js        payload, runAt, priority validation
│   │
│   ├── config/
│   │   ├── constants.js           Queue names, priority helpers, BRPOP arg lists
│   │   ├── database.js            Prisma client with pg pool adapter
│   │   ├── logger.js              Pino with worker_id in every line
│   │   ├── redis.js               Main ioredis instance
│   │   ├── redisSubscriber.js     Dedicated subscriber connection (pub/sub)
│   │   └── socket.js              socket.io singleton (avoids circular imports)
│   │
│   ├── workers/
│   │   ├── index.js               BRPOP loop, state transitions, pub/sub publish
│   │   ├── scheduler.js           Polls queue:delayed, promotes to live queues
│   │   └── handlers/
│   │       ├── handlerMap.js      { SEND_EMAIL: emailHandler, ... }
│   │       ├── emailHandler.js    Nodemailer + idempotency key
│   │       ├── imageHandler.js    Sharp (thumbnail, webp, grayscale)
│   │       ├── pdfGenHandler.js   PDFKit invoice generator
│   │       └── scrapperHandler.js Axios + Cheerio price scraper
│   │
│   └── scripts/
│       ├── flush.js               Wipe DB + Redis (dev reset)
│       ├── replayDead.js          CLI: node replayDead.js --execute
│       └── zombieHunter.js        CLI: node zombieHunter.js --execute
│
├── public/
│   ├── dashboard.html             Real-time control center UI
│   ├── processed/                 Image handler output (gitignored)
│   └── pdfs/                     PDF handler output (gitignored)
│
├── ecosystem.config.cjs           PM2 process definitions
├── docker-compose.yml             Postgres + Redis + RedisInsight
├── package.json
└── .env
```

---

## npm Scripts

```bash
npm run queue:replay-dead        # replay all dead jobs (--execute)
npm run queue:inspect-dead       # preview dead jobs without touching them
npm run queue:zombie-hunt        # run zombie sweep (--execute)
npm run queue:zombie-hunt:dry    # preview zombie sweep
```

---

## Design Decisions Worth Knowing

**Why not use BullMQ?** BullMQ is excellent and would be the right call for a production product. Building the primitives manually meant understanding exactly why the scheduler is a separate process (without it, every worker would need a distributed lock to avoid promoting the same delayed job twice), why `status = sweeping` exists (atomic claim pattern to prevent concurrent zombie hunters from double-processing the same job), and why the write order in retry/dead paths is always Postgres first and Redis second (losing a Redis write is recoverable; losing a Postgres write leaves the system in an inconsistent state).

**Why BRPOP instead of polling?** BRPOP blocks at the Redis server level and returns the moment an item is available. A polling loop with `RPOP` + `setTimeout` wastes CPU and adds latency proportional to the polling interval. BRPOP is O(1) and adds zero latency.

**Why separate queues for IO and compute?** Image processing is CPU-bound and takes 2-5 seconds. Web scraping is network-bound and spends 90% of its time waiting on HTTP. Mixing them in one queue means a burst of image jobs blocks all email delivery. Separate queues let you scale each worker pool independently based on actual demand.

**Why Redis pub/sub for the dashboard instead of WebSockets from workers?** Workers run in separate PM2 processes with no access to the API server's socket.io instance. Redis pub/sub is the standard inter-process communication pattern for this — workers publish, the API server subscribes and forwards. One channel, any number of publishers and subscribers.
