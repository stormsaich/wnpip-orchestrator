# orchestrator — Intended Project Structure

```
orchestrator/                       ← git repo root
│
├── README.md
├── SPEC.md
├── .env.example
├── STRUCTURE.md                    ← this file
│
├── Dockerfile
│   # FROM python:3.11-slim
│   # COPY shared/ /build/shared/ && pip install /build/shared/
│   # COPY requirements.txt . && pip install -r requirements.txt
│   # COPY . .
│   # CMD ["python", "main.py"]
│
├── requirements.txt
│   # apscheduler==3.10.4
│   # redis==5.0.8
│   # pymongo[srv]==4.10.1
│   # python-dotenv==1.0.0
│   # -e ../shared   (or copied into image at build time)
│
├── main.py
│   # Entry point.
│   # 1. Load Config() from environment
│   # 2. Connect to Redis and MongoDB (fail fast on unavailable)
│   # 3. Register crawl:sites in Redis (from SITES config)
│   # 4. Instantiate all Job classes
│   # 5. Register jobs with APScheduler (intervals from Config)
│   # 6. Start Heartbeat thread (writes health:orchestrator every 30s)
│   # 7. scheduler.start() — blocks until SIGTERM
│   # 8. Graceful shutdown on SIGTERM
│
├── manage.py
│   # CLI for operator actions. Not run as a service — run manually.
│   # Commands:
│   #   crawl-now --site <site_key> --url <url> [--notes "reason"]
│   #   queue-stats [--site <site_key>]
│   #   list-sites
│   #   health
│   # Uses admin.py internally for crawl-now.
│
├── admin.py
│   # admin_request_crawl(redis, mongo, site_key, url, notes) → None
│   #   Sets IMMEDIATE priority score in queue
│   #   Ensures url_hash is in url_map
│   #   Upserts crawl_state.admin_requests array
│   # No web server — this is a function, not an API endpoint
│
└── jobs/
    ├── __init__.py
    │
    ├── base_job.py
    │   # BaseJob(redis, mongo, config)
    │   # All jobs inherit from this
    │   # Provides: logger, error handling, per-site iteration helper
    │
    ├── recrawl_scheduler.py
    │   # RecrawlSchedulerJob(BaseJob)
    │   # run() → None
    │   #   For each site_key in crawl:sites:
    │   #     Query crawl_state for listings with next_crawl_at <= now
    │   #     Compute priority score per listing status
    │   #     ZADD NX crawl:queue:<site_key> <score> <url_hash>
    │
    ├── suspect_checker.py
    │   # SuspectCheckerJob(BaseJob)
    │   # run() → None
    │   #   Find all suspect listings in crawl_state
    │   #   For each: check raw_listings for most recent crawl result
    │   #   Decision: revert to active OR confirm removed
    │   #   Update crawl_state status and next_crawl_at
    │
    ├── archive_checker.py
    │   # ArchiveCheckerJob(BaseJob)
    │   # run() → None
    │   #   Find removed listings older than ARCHIVE_AFTER_REMOVED_DAYS
    │   #   Update crawl_state: status=archived, archived_at=now
    │   #   Update queue score to LOW priority
    │   # NOTE: does NOT move documents — that is the archiver service's job
    │
    ├── discovery_trigger.py
    │   # DiscoveryTriggerJob(BaseJob)
    │   # run() → None
    │   #   For each site_key:
    │   #     Check orchestrator:lock:discovery:<site_key>
    │   #     If not locked: set lock, launch subprocess
    │   #     If locked: log skip
    │   # subprocess.Popen(DISCOVERY_COMMAND, env={SITE_KEY: site_key, ...})
    │
    ├── health_monitor.py
    │   # HealthMonitorJob(BaseJob)
    │   # run() → None
    │   #   Check all heartbeat keys
    │   #   Check queue depths
    │   #   Check proxy pool available ratio
    │   #   Check suspect count
    │   #   For each failure: LPUSH alerts:queue <json>
    │
    └── queue_reaper.py
        # QueueReaperJob(BaseJob)
        # run() → None
        #   For each site_key:
        #     ZRANGEBYSCORE crawl:inflight:<site_key> 0 <stale_threshold_ms>
        #     ZREM from inflight
        #     ZADD back to queue at current timestamp
```

## Docker notes

The `orchestrator` image does not contain Scrapy. Discovery is launched via subprocess
using the `spiders` container image. In Docker Compose, this means the orchestrator
container calls `docker exec` or a shared socket to launch the discovery container —
or more simply, the orchestrator writes a signal to Redis that the discovery container
polls for.

See `jobs/discovery_trigger.py` — the trigger mechanism is configurable via
`DISCOVERY_COMMAND` env var to allow different launch strategies per environment.
