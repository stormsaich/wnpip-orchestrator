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

## Environment variables

See `.env.example` for the full list with descriptions.

Required at minimum:
```
REDIS_URL
MONGO_URI
MONGO_DB
```

## Dependencies

- `wnpdcshared` (local install from `../shared`)
- `apscheduler==3.10.4`
- `redis==5.0.8`
- `pymongo[srv]==4.10.1`
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

- Heartbeat: writes `health:orchestrator` to Redis every 30 seconds (TTL 300s)
- Alerts: pushes JSON objects to `alerts:queue` in Redis on health check failures
- Logs: structured JSON to stdout, level controlled by `LOG_LEVEL`
