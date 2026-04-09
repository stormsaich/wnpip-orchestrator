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
│   #   register-site --site-key <key> --website <domain> --display-name <name>
│   #                 [--listing-url-pattern <regex>]
│   #     Writes site:config:<site_key> hash + SADD crawl:sites atomically
│   #   list-sites
│   #     SMEMBERS crawl:sites + HGETALL site:config per site
│   #   seed-queue --site <site_key> --file <urls.txt>
│   #     LPUSH site:seed_queue:<site_key> for each URL in file
│   #   crawl-now --site <site_key> --url <url> [--notes "reason"]
│   #   queue-stats [--site <site_key>]
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
    │   # BaseJob(redis, mongo, config, scheduler)
    │   # All jobs inherit from this
    │   # Provides:
    │   #   - logger, error handling, per-site iteration helper
    │   #   - _read_overrides() → dict: reads config:orchestrator:overrides each cycle
    │   #   - _is_enabled(job_name) → bool: checks override enabled flag
    │   #   - _check_interval_change(job_name, param, current_minutes): reschedules if changed
    │   #   - _write_status(job_name, result, duration_ms, run_type): writes job status hash
    │   #   - _publish_event(event_type, job_name, **kwargs): PUBLISH to orchestrator:events
    │   # run() template:
    │   #   1. Read overrides
    │   #   2. Check for manual trigger (RPOP trigger list) → sets run_type = "manual"
    │   #   3. If no trigger AND not enabled → publish job_skipped, return
    │   #   4. Check interval change → reschedule APScheduler job if needed
    │   #   5. SET is_running=1 in status hash, publish job_started
    │   #   6. Execute job body (implemented by subclass)
    │   #   7. SET is_running=0, write full status hash, publish job_completed
    │
    ├── recrawl_scheduler.py
    │   # RecrawlSchedulerJob(BaseJob)
    │   # Interval param: RECRAWL_INTERVAL_MINUTES | Enabled flag: RECRAWL_SCHEDULER_ENABLED
    │   # run() → None
    │   #   For each site_key in crawl:sites:
    │   #     Query crawl_state for listings with next_crawl_at <= now
    │   #     Compute priority score per listing status
    │   #     ZADD NX crawl:queue:<site_key> <score> <url_hash>
    │
    ├── suspect_checker.py
    │   # SuspectCheckerJob(BaseJob)
    │   # Interval param: SUSPECT_CHECK_INTERVAL_MINUTES | Enabled flag: SUSPECT_CHECKER_ENABLED
    │   # run() → None
    │   #   Find all suspect listings in crawl_state
    │   #   For each: check raw_listings for most recent crawl result
    │   #   Decision: revert to active OR confirm removed
    │   #   Update crawl_state status and next_crawl_at
    │
    ├── archive_checker.py
    │   # ArchiveCheckerJob(BaseJob)
    │   # Interval param: (daily — no interval override) | Enabled flag: ARCHIVE_CHECKER_ENABLED
    │   # run() → None
    │   #   Find removed listings older than ARCHIVE_AFTER_REMOVED_DAYS
    │   #   Update crawl_state: status=archived, archived_at=now
    │   #   Update queue score to LOW priority
    │   # NOTE: does NOT move documents — that is the archiver service's job
    │
    ├── discovery_trigger.py
    │   # DiscoveryTriggerJob(BaseJob)
    │   # Interval param: DISCOVERY_TRIGGER_INTERVAL_HOURS | Enabled flag: DISCOVERY_TRIGGER_ENABLED
    │   # run() → None
    │   #   For each site_key in crawl:sites:
    │   #     Attempt SET NX EX on orchestrator:lock:discovery:<site_key> (TTL=3h)
    │   #     If lock acquired: LPUSH discovery:trigger:<site_key> {site_key, triggered_at}
    │   #     If lock already held: log skip (discovery still running)
    │   # The discovery container polls its trigger queue via BRPOP and manages itself
    │
    ├── health_monitor.py
    │   # HealthMonitorJob(BaseJob)
    │   # Interval param: HEALTH_CHECK_INTERVAL_SECONDS | Enabled flag: HEALTH_MONITOR_ENABLED
    │   # run() → None
    │   #   Check all heartbeat keys
    │   #   Check queue depths
    │   #   Check proxy pool available ratio
    │   #   Check suspect count
    │   #   For each failure: LPUSH alerts:queue <json>
    │   # WARNING: disabling this job silences all pipeline alerting
    │
    ├── queue_reaper.py
    │   # QueueReaperJob(BaseJob)
    │   # Interval param: QUEUE_REAPER_INTERVAL_MINUTES | Enabled flag: QUEUE_REAPER_ENABLED
    │   # run() → None
    │   #   For each site_key:
    │   #     ZRANGEBYSCORE crawl:inflight:<site_key> 0 <stale_threshold_ms>
    │   #     ZREM from inflight
    │   #     ZADD back to queue at current timestamp
    │
    └── trigger_watcher.py
        # TriggerWatcherJob — runs every 30 seconds, not configurable, always enabled
        # Exists solely to ensure manual triggers are acted on promptly regardless of
        # the target job's scheduled interval (e.g. ArchiveCheckerJob runs daily —
        # without this, a manual trigger could wait 24 hours)
        # run() → None
        #   For each job_name in ALL_JOB_NAMES:
        #     RPOP orchestrator:job:<job_name>:trigger
        #     If message present: call job.run(run_type="manual") directly
```

## Docker notes

The `orchestrator` image does not contain Scrapy and has no knowledge of other
containers. Discovery is triggered exclusively via Redis: the orchestrator writes
a signal to `discovery:trigger:<site_key>` and the discovery container polls for
it via `BRPOP`. Each container manages itself based on what it sees in Redis.

No `docker exec`, Docker socket, or direct service calls are used anywhere in this
architecture. All inter-service coordination goes through Redis queues and keys.
