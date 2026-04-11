# orchestrator

## Purpose

The orchestrator is the scheduling brain of the wnpdc pipeline. It is a long-running
Python process that manages the lifecycle of every listing URL across all registered sites.

It does not fetch URLs or store listing content. It manages what gets crawled and when.

## Responsibilities

1. Promote listings due for re-crawl into the Redis crawl queue
2. Confirm or revert `suspect` listings (possible removals)
3. Transition listings through lifecycle states (active → removed → archived)
4. Trigger URL discovery runs on schedule
5. Recover stalled in-flight crawl items (queue reaper)
6. Monitor service health and emit alerts

## How it runs

Long-running process using APScheduler. Six scheduled jobs run on independent intervals.
On startup it registers all sites from `crawl:sites` in Redis, then begins the job loop.

```bash
# Local development
python main.py

# Docker
docker compose up orchestrator
```

## Docker Compose

Minimal example to run the orchestrator alongside Redis and MongoDB:

```yaml
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"

  mongo:
    image: mongo:7
    ports:
      - "27017:27017"

  orchestrator:
    image: ghcr.io/techstormstudios/wnpip-orchestrator:main
    depends_on:
      - redis
      - mongo
    environment:
      REDIS_URL: redis://redis:6379
      MONGO_URI: mongodb://mongo:27017
      MONGO_DB: wnpdc
      LOG_LEVEL: INFO
      HEARTBEAT_INTERVAL_SECONDS: 5
      HEALTH_CHECK_INTERVAL_SECONDS: 60
      RECRAWL_INTERVAL_MINUTES: 5
      SUSPECT_CHECK_INTERVAL_MINUTES: 15
      QUEUE_REAPER_INTERVAL_MINUTES: 30
      DISCOVERY_TRIGGER_INTERVAL_HOURS: 4
      INFLIGHT_TTL_SECONDS: 300
      ARCHIVE_AFTER_REMOVED_DAYS: 30
    restart: unless-stopped
```

For the full variable reference see `.env.example`.

## Environment variables

See `.env.example` for the full list with descriptions.

Required at minimum:
```
REDIS_URL
MONGO_URI
MONGO_DB
```

## Dependencies

- `wnpip-shared-libraray` (local dev: `requirements-dev.txt`; Docker/CI: copied in by Dockerfile)
- `apscheduler==3.10.4`
- `redis==5.0.8`
- `pymongo==4.10.1`
- `python-dotenv==1.0.0`

## Admin operations

The `manage.py` CLI provides operator commands:

```bash
# Request immediate crawl of a specific URL
python manage.py crawl-now --site www_property24_com --url https://www.property24.com/...

# Show queue depths per site
python manage.py queue-stats

# List all registered sites
python manage.py list-sites
```

## Observability

- Heartbeat: writes `health:orchestrator` to Redis every `HEARTBEAT_INTERVAL_SECONDS` (default 5s, TTL 300s)
- Alerts: writes JSON objects to `alerts:history` sorted set in Redis on health check failures
- Logs: structured JSON to stdout, level controlled by `LOG_LEVEL`
