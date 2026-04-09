# orchestrator — Technical Specification

## Runtime Config

The orchestrator implements the Runtime Config Protocol defined in `shared/SPEC.md §2`.
Redis is the live source of truth for all tunable parameters — the UI can adjust
them without restarting the container.

### On startup

```python
redis.hset("config:orchestrator:current", mapping={
    "RECRAWL_INTERVAL_MINUTES":           config.recrawl_interval_minutes,
    "SUSPECT_CHECK_INTERVAL_MINUTES":     config.suspect_check_interval_minutes,
    "HEALTH_CHECK_INTERVAL_SECONDS":      config.health_check_interval_seconds,
    "QUEUE_REAPER_INTERVAL_MINUTES":      config.queue_reaper_interval_minutes,
    "DISCOVERY_TRIGGER_INTERVAL_HOURS":   config.discovery_trigger_interval_hours,
    "INFLIGHT_TTL_SECONDS":              config.inflight_ttl_seconds,
    "ARCHIVE_AFTER_REMOVED_DAYS":         config.archive_after_removed_days,
})
```

### Each job cycle

Read `config:orchestrator:overrides` at the start of each scheduled job run and
use override values where present. APScheduler job intervals are updated if the
relevant override changes between runs.

### Tunable parameters

| Parameter | Effect when overridden |
| --- | --- |
| `RECRAWL_INTERVAL_MINUTES` | How often RecrawlSchedulerJob runs |
| `SUSPECT_CHECK_INTERVAL_MINUTES` | How often SuspectCheckerJob runs |
| `HEALTH_CHECK_INTERVAL_SECONDS` | How often HealthMonitorJob runs |
| `QUEUE_REAPER_INTERVAL_MINUTES` | How often QueueReaperJob runs |
| `DISCOVERY_TRIGGER_INTERVAL_HOURS` | How often discovery is triggered |
| `INFLIGHT_TTL_SECONDS` | Age threshold for reaping stuck inflight items |
| `ARCHIVE_AFTER_REMOVED_DAYS` | Days removed before ArchiveCheckerJob transitions to archived |

---

## Architecture

Long-running Python process (APScheduler). All state is in Redis and MongoDB.
Stateless — can be restarted at any time without data loss or duplicate operations.

Communicates exclusively through Redis and MongoDB. Never calls the crawler, discovery
service, or any other service directly.

---

## Scheduled Jobs

### 1. `RecrawlSchedulerJob` — every 5 minutes (configurable: `RECRAWL_INTERVAL_MINUTES`)

**Purpose:** Find listings due for re-crawl and add them to the Redis queue.

**Inputs:**
- MongoDB `crawl_state`: query `{listing_status: {$in: ["active", "removed"]}, next_crawl_at: {$lte: now}}`
- Iterates all site_keys registered in `crawl:sites`

**Outputs:**
- Redis `crawl:queue:<site_key>`: `ZADD NX <score> <url_hash>` for each due listing
  - NX flag: do not overwrite if already queued at higher priority

**Priority score logic:**
```
status=discovered   → score = CrawlPriority.HIGH (-1)
status=suspect      → score = CrawlPriority.HIGH (-1)
status=active       → score = next_crawl_at_ms (Unix ms timestamp)
status=removed      → score = next_crawl_at_ms + FAR_FUTURE_OFFSET_MS (LOW priority)
status=archived     → score = next_crawl_at_ms + FAR_FUTURE_OFFSET_MS (LOW priority)
```

**Failure behaviour:** Logs warning and skips to next site on MongoDB timeout.

---

### 2. `SuspectCheckerJob` — every 15 minutes (configurable: `SUSPECT_CHECK_INTERVAL_MINUTES`)

**Purpose:** Resolve listings in `suspect` state — either confirm removal or revert to active.

**Inputs:**
- MongoDB `crawl_state`: query `{listing_status: "suspect"}`
- MongoDB `raw_listings`: check `listing_status` of the most recent crawl for each suspect listing

**Decision logic:**
```
If raw_listings.listing_status == "active":
    → revert crawl_state.listing_status to "active"
    → update next_crawl_at per normal re-crawl rules

If raw_listings.listing_status == "removed" AND crawl is recent (< 2 hours old):
    → confirm: set crawl_state.listing_status = "removed"
    → set removed_at = now
    → set next_crawl_at = now + 7 days (weekly re-check for re-listing)

If no recent crawl exists (crawler hasn't hit it yet):
    → ensure it is queued at HIGH priority
    → leave status as suspect
```

**Outputs:**
- MongoDB `crawl_state`: status updates, next_crawl_at updates
- Redis `crawl:queue:<site_key>`: re-queue at HIGH if no recent crawl

---

### 3. `ArchiveCheckerJob` — daily

**Purpose:** Transition long-term removed listings to `archived` status.
Document movement to the archive database is handled by the separate `archiver` service.

**Inputs:**
- MongoDB `crawl_state`: query `{listing_status: "removed", last_crawled: {$lt: threshold}}`
- Threshold: `now - ARCHIVE_AFTER_REMOVED_DAYS`

**Outputs:**
- MongoDB `crawl_state`: set `listing_status = "archived"`, set `archived_at = now`
- Redis `crawl:queue:<site_key>`: update score to LOW priority (`next_crawl_at_ms + FAR_FUTURE_OFFSET_MS`)

**Note:** The orchestrator only changes status to `archived`. The `archiver` service
performs the actual document migration to the archive MongoDB.

---

### 4. `DiscoveryTriggerJob` — every 4 hours (configurable: `DISCOVERY_TRIGGER_INTERVAL_HOURS`)

**Purpose:** Launch URL discovery runs for each registered site.

**Inputs:**
- Redis `crawl:sites`: set of registered site_keys
- Redis `orchestrator:lock:discovery:<site_key>`: check for existing lock (prevents overlap)

**Behaviour:**
- Acquires lock with TTL = 3 hours before launching
- Launches discovery as a subprocess: `scrapy crawl p24_discovery`
- If lock already held: skip and log warning
- If subprocess fails: release lock, push alert to `alerts:queue`

**Outputs:**
- Redis `orchestrator:lock:discovery:<site_key>`: lock set with TTL
- Subprocess: Scrapy discovery spider (runs independently to completion)

---

### 5. `HealthMonitorJob` — every 60 seconds (configurable: `HEALTH_CHECK_INTERVAL_SECONDS`)

**Purpose:** Check all service heartbeats and operational metrics; emit alerts on failures.

**Checks performed:**

| Check | Condition | Alert severity |
|---|---|---|
| Crawler heartbeat | `health:crawler:<site_key>` key age > 5 minutes | CRITICAL |
| Orchestrator self-heartbeat | Written by orchestrator itself every 30s | INFO only |
| Queue depth | `ZCARD crawl:queue:<site_key>` > 100,000 | WARNING |
| Inflight age | Any item in `crawl:inflight:<site_key>` older than `INFLIGHT_TTL_SECONDS` | WARNING |
| Proxy pool health | `ZCARD proxy:pool:available` / total proxies < 20% | WARNING |
| Discovery recency | `orchestrator:lock:discovery:<site_key>` not set and last discovery > 8 hours | WARNING |
| Suspect count | `crawl_state.count({status: "suspect"})` > 500 per site | WARNING |

**Outputs:**
- Redis `alerts:queue`: LPUSH JSON objects `{severity, service, site_key, message, timestamp}`
- Stdout logs at appropriate level

---

### 6. `QueueReaperJob` — every 30 minutes (configurable: `QUEUE_REAPER_INTERVAL_MINUTES`)

**Purpose:** Recover URL hashes stuck in the inflight set (crawler died mid-job).

**Inputs:**
- Redis `crawl:inflight:<site_key>`: sorted set with `acquired_at_ms` scores

**Logic:**
```
threshold_ms = now_ms - (INFLIGHT_TTL_SECONDS * 1000)
stale_hashes = ZRANGEBYSCORE crawl:inflight:<site_key> 0 threshold_ms

For each stale hash:
    ZREM crawl:inflight:<site_key> <url_hash>
    ZADD crawl:queue:<site_key> NX <now_ms> <url_hash>   ← re-queue for immediate crawl
```

**Failure behaviour:** Logs each reaped item at WARNING level.

---

## Admin Interface

### `admin_request_crawl(site_key, url, notes)`

Requests an immediate crawl of a specific URL, bypassing the normal queue priority.

**Steps:**
1. Normalise URL and compute `url_hash`
2. `HSET crawl:url_map:<site_key> <url_hash> <url>`  — ensure URL is resolvable
3. `ZADD crawl:queue:<site_key> -2 <url_hash>`  — IMMEDIATE priority (no NX flag — overrides existing)
4. Upsert `crawl_state` with `listing_status = "discovered"` if not already present
5. Append to `crawl_state.admin_requests` array: `{requested_at, notes, priority: "IMMEDIATE"}`

**Invocation surfaces:**
- `manage.py crawl-now --site <site_key> --url <url> --notes "reason"`
- Direct Redis: `ZADD crawl:queue:www_property24_com -2 <url_hash>` (manual operator action)
- Future REST API endpoint (not in scope for v1)

---

## Redis Keys

| Key | Operation | Notes |
|---|---|---|
| `crawl:sites` | SMEMBERS (read) | Enumerate all sites to manage |
| `crawl:queue:<site_key>` | ZADD NX, ZADD (admin), ZCARD | Schedule crawls; admin override |
| `crawl:inflight:<site_key>` | ZRANGEBYSCORE, ZREM | Queue reaper |
| `crawl:url_map:<site_key>` | HSET | Admin crawl requests |
| `orchestrator:lock:discovery:<site_key>` | SET EX, GET | Discovery mutex |
| `health:orchestrator` | SET EX | Self-heartbeat |
| `health:crawler:<site_key>` | GET | Monitor crawler liveness |
| `proxy:pool:available` | ZCARD | Health check |
| `alerts:queue` | LPUSH | Emit health alerts |

## MongoDB Collections

| Collection | Operations | Notes |
|---|---|---|
| `crawl_state` | find, update_one, update_many | Re-crawl scheduling, status transitions |
| `raw_listings` | find_one | SuspectCheckerJob reads crawl result to confirm status |

**The orchestrator never writes to `raw_listings`.**

---

## Site Registration

Sites are registered in `crawl:sites` (Redis Set). The orchestrator discovers all
active sites by reading this set on startup and on each job run.

To add a new site:
```
SADD crawl:sites www_privateproperty_co_za
```

Once registered, all orchestrator jobs automatically manage that site's queue on
their next run. No orchestrator code changes required.

---

## Graceful Shutdown

On `SIGTERM`:
1. APScheduler stops accepting new job runs
2. Any running job is allowed to complete (max 60 seconds)
3. Heartbeat thread is stopped
4. Redis and MongoDB connections are closed
5. Process exits with code 0

---

## Failure Modes

| Failure | Behaviour |
|---|---|
| Redis unavailable at startup | Raise `RedisUnavailableError`, exit with code 1 |
| MongoDB unavailable at startup | Raise `MongoUnavailableError`, exit with code 1 |
| Redis unavailable during job | Log error, skip job iteration, retry on next interval |
| MongoDB unavailable during job | Log error, skip job iteration, retry on next interval |
| Discovery subprocess fails | Log error, release lock, push alert to `alerts:queue` |
| Job takes longer than interval | APScheduler skips the overlapping run (coalesce=True) |
