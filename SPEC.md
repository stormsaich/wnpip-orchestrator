# orchestrator — Technical Specification

## Runtime Config

The orchestrator implements the Runtime Config Protocol defined in `shared/SPEC.md §2`.
Redis is the live source of truth for all tunable parameters — the UI can adjust
them without restarting the container.

### On startup

```python
redis.hset("config:orchestrator:current", mapping={
    # Intervals
    "RECRAWL_INTERVAL_MINUTES":           config.recrawl_interval_minutes,
    "SUSPECT_CHECK_INTERVAL_MINUTES":     config.suspect_check_interval_minutes,
    "HEALTH_CHECK_INTERVAL_SECONDS":      config.health_check_interval_seconds,
    "QUEUE_REAPER_INTERVAL_MINUTES":      config.queue_reaper_interval_minutes,
    "DISCOVERY_TRIGGER_INTERVAL_HOURS":   config.discovery_trigger_interval_hours,
    "INFLIGHT_TTL_SECONDS":               config.inflight_ttl_seconds,
    "ARCHIVE_AFTER_REMOVED_DAYS":         config.archive_after_removed_days,
    # Enabled flags — all jobs start enabled by default
    "RECRAWL_SCHEDULER_ENABLED":          "1",
    "SUSPECT_CHECKER_ENABLED":            "1",
    "ARCHIVE_CHECKER_ENABLED":            "1",
    "DISCOVERY_TRIGGER_ENABLED":          "1",
    "HEALTH_MONITOR_ENABLED":             "1",
    "QUEUE_REAPER_ENABLED":               "1",
})
```

### Each job cycle

At the start of every job run the orchestrator:

1. Reads `config:orchestrator:overrides` — applies any UI-set values in preference to env-var defaults
2. Compares effective interval against what APScheduler is currently scheduled at — reschedules via `scheduler.reschedule_job()` if changed
3. Checks the job's enabled flag — if `0` and no manual trigger is pending, logs "skipped (disabled)" and returns immediately
4. Checks `orchestrator:job:<name>:trigger` — if a manual trigger message is present, runs regardless of enabled state and tags the run as `"manual"`

### Tunable parameters

| Parameter | Type | Effect when overridden |
| --- | --- | --- |
| `RECRAWL_INTERVAL_MINUTES` | int | How often RecrawlSchedulerJob runs |
| `SUSPECT_CHECK_INTERVAL_MINUTES` | int | How often SuspectCheckerJob runs |
| `HEALTH_CHECK_INTERVAL_SECONDS` | int | How often HealthMonitorJob runs |
| `QUEUE_REAPER_INTERVAL_MINUTES` | int | How often QueueReaperJob runs |
| `DISCOVERY_TRIGGER_INTERVAL_HOURS` | int | How often discovery is triggered |
| `INFLIGHT_TTL_SECONDS` | int | Age threshold for reaping stuck inflight items |
| `ARCHIVE_AFTER_REMOVED_DAYS` | int | Days removed before ArchiveCheckerJob transitions to archived |
| `RECRAWL_SCHEDULER_ENABLED` | bool (0/1) | Enable or disable automated RecrawlSchedulerJob runs |
| `SUSPECT_CHECKER_ENABLED` | bool (0/1) | Enable or disable automated SuspectCheckerJob runs |
| `ARCHIVE_CHECKER_ENABLED` | bool (0/1) | Enable or disable automated ArchiveCheckerJob runs |
| `DISCOVERY_TRIGGER_ENABLED` | bool (0/1) | Enable or disable automated DiscoveryTriggerJob runs |
| `HEALTH_MONITOR_ENABLED` | bool (0/1) | Enable or disable automated HealthMonitorJob runs |
| `QUEUE_REAPER_ENABLED` | bool (0/1) | Enable or disable automated QueueReaperJob runs |

**Important:** Disabling a job stops automated scheduling only. A disabled job can still be run via manual trigger. Disabling `HEALTH_MONITOR` silences all alerting — this should be a conscious operator action.

---

## Job Control Protocol

### Manual triggers

Each job monitors a Redis List. The UI writes to this list to request an immediate run:

```
orchestrator:job:recrawl_scheduler:trigger
orchestrator:job:suspect_checker:trigger
orchestrator:job:archive_checker:trigger
orchestrator:job:discovery_trigger:trigger
orchestrator:job:health_monitor:trigger
orchestrator:job:queue_reaper:trigger
```

Trigger message format (JSON):
```json
{"requested_by": "ui", "requested_at": "2026-04-09T10:00:00Z"}
```

The UI writes: `LPUSH orchestrator:job:<name>:trigger <json>`

A dedicated `TriggerWatcherJob` runs every 30 seconds and scans all trigger lists. If a trigger message is found it fires the corresponding job immediately via APScheduler's `scheduler.get_job(<name>).func()` — outside the normal interval. This ensures manual triggers are acted on within 30 seconds regardless of the job's scheduled interval (important for daily jobs like `ArchiveCheckerJob`).

Manual trigger behaviour:
- Runs even if the job's enabled flag is `0`
- Consumes the trigger message with `RPOP` before running (idempotent — one trigger = one run)
- Tags the run as `"manual"` in the status hash and event

### Job status reporting

After every run the orchestrator writes to a per-job status hash:

```
orchestrator:job:<name>:status  (Hash)
```

| Field | Type | Description |
| --- | --- | --- |
| `last_run_at` | ISO string | Timestamp of most recent run start |
| `last_run_type` | string | `"scheduled"` or `"manual"` |
| `last_duration_ms` | int | Wall-clock time for the run |
| `last_result` | string | `"ok"` or `"error"` |
| `last_error` | string | Error message if result is `"error"`, else empty string |
| `next_run_at` | ISO string | Computed from current effective interval |
| `is_running` | `"0"` / `"1"` | Set to `"1"` at run start, `"0"` at run end |
| `run_count` | int | Cumulative total runs (scheduled + manual) |
| `enabled` | `"0"` / `"1"` | Current enabled state (mirrors override or default) |

`is_running` is set before the job body executes and cleared in a `finally` block. The UI uses this to show a running indicator without polling.

### Event pub/sub

The orchestrator publishes to `orchestrator:events` (Redis Pub/Sub channel) on key lifecycle moments. The UI subscribes to receive real-time updates without polling individual status keys.

Event format:
```json
{"event": "<event_type>", "job": "<job_name>", "at": "<iso_timestamp>", ...}
```

| Event type | Additional fields | When published |
| --- | --- | --- |
| `job_started` | `run_type` | Job body begins execution |
| `job_completed` | `result`, `duration_ms` | Job body finishes (ok or error) |
| `job_skipped` | `reason` (`"disabled"`) | Automated run skipped due to enabled=0 |
| `job_triggered` | `requested_by` | Manual trigger message consumed |
| `interval_changed` | `from_minutes`, `to_minutes` | APScheduler rescheduled |
| `config_applied` | `param`, `value` | Override value applied to a job cycle |

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

**Purpose:** Signal each registered site's discovery container to run a URL discovery pass.

**Inputs:**
- Redis `crawl:sites`: set of registered site_keys
- Redis `orchestrator:lock:discovery:<site_key>`: check for existing lock (prevents overlap)

**Behaviour:**
- For each site_key in `crawl:sites`:
  - Attempt to acquire `orchestrator:lock:discovery:<site_key>` with TTL = 3 hours (`SET NX EX`)
  - If lock already held: skip and log — discovery is still running for that site
  - If lock acquired: `LPUSH discovery:trigger:<site_key>` with a JSON message containing `{site_key, triggered_at}`
- The discovery container polls `discovery:trigger:<site_key>` via `BRPOP`, runs independently, and deletes the lock when done
- If the discovery container crashes, the 3-hour lock TTL ensures the next trigger cycle proceeds

**Outputs:**
- Redis `orchestrator:lock:discovery:<site_key>`: lock set with 3-hour TTL
- Redis `discovery:trigger:<site_key>`: LPUSH trigger message for discovery container

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
| `crawl:sites` | SMEMBERS (read), SADD (manage.py / UI) | Enumerate all registered site_keys |
| `site:config:<site_key>` | HGETALL (read), HSET (manage.py / UI) | Site metadata — display name, website, enabled flag |
| `site:seed_queue:<site_key>` | LPUSH (UI / manage.py), RPOP (orchestrator) | Bulk URL injection queue |
| `crawl:queue:<site_key>` | ZADD NX, ZADD (admin), ZCARD | Schedule crawls; admin override |
| `crawl:inflight:<site_key>` | ZRANGEBYSCORE, ZREM | Queue reaper |
| `crawl:url_map:<site_key>` | HSET | Admin crawl requests |
| `discovery:trigger:<site_key>` | LPUSH | Signal discovery container to run |
| `orchestrator:lock:discovery:<site_key>` | SET NX EX, DEL | Discovery mutex (set by orchestrator, deleted by discovery container on completion) |
| `orchestrator:job:<name>:trigger` | LPUSH (UI), RPOP (orchestrator) | Manual run trigger per job |
| `orchestrator:job:<name>:status` | HSET | Job status reported after every run; read by UI |
| `orchestrator:events` | PUBLISH | Real-time lifecycle events; UI subscribes |
| `config:orchestrator:current` | HSET (on startup) | Env-var baseline; read by UI for display |
| `config:orchestrator:overrides` | HGETALL (each cycle) | UI-set overrides; intervals + enabled flags |
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

Sites are registered exclusively through Redis — no hardcoded lists, no config files.
The orchestrator enumerates `crawl:sites` (Redis Set) on every job cycle and manages
each site's queue automatically.

### Site registry — `site:config:<site_key>` (Hash)

Every registered site has a config hash as the single source of truth for its metadata:

| Field | Example | Description |
| --- | --- | --- |
| `display_name` | `"Property24"` | Human-readable name for the UI |
| `website` | `"www.property24.com"` | Full domain (used by dedup queries) |
| `site_key` | `"www_property24_com"` | Redis namespace key |
| `listing_url_pattern` | `"/(to-rent\|for-sale)/[^/]+/\\d+"` | Regex for identifying listing URLs |
| `registered_at` | ISO timestamp | When the site was first registered |
| `enabled` | `"1"` | `"0"` pauses all orchestrator jobs for this site |

### Registering a site

Registration writes to both the config hash and the enumeration set atomically:

```python
pipe = redis.pipeline()
pipe.hset(f"site:config:{site_key}", mapping={
    "display_name":        display_name,
    "website":             website,
    "site_key":            site_key,
    "listing_url_pattern": listing_url_pattern,
    "registered_at":       datetime.utcnow().isoformat(),
    "enabled":             "1",
})
pipe.sadd("crawl:sites", site_key)
pipe.execute()
```

This is exposed via `manage.py register-site` and the UI site management panel.
Never write `SADD crawl:sites` directly without also writing `site:config:<site_key>`.

### Startup bootstrap

On startup, the orchestrator reads `crawl:sites`. If the set is empty (Redis was
flushed or a fresh deploy), it logs a CRITICAL alert and continues running — it will
manage sites as they are registered. It does not seed `crawl:sites` from env vars or
any other static source.

### Site-level enable/disable

Setting `site:config:<site_key> enabled = "0"` causes all orchestrator jobs to skip
that site on their next cycle. The site remains registered and its queue is preserved.
This allows pausing a site's crawl activity without deregistering it.

### Queue seeding — `site:seed_queue:<site_key>` (List)

For bulk URL injection (bootstrapping a new site, recovering from a Redis flush, or
operator-driven seeding) without running the discovery spider:

```
LPUSH site:seed_queue:www_property24_com "https://www.property24.com/to-rent/..."
LPUSH site:seed_queue:www_property24_com "https://www.property24.com/for-sale/..."
```

The `RecrawlSchedulerJob` drains this list at the start of each cycle:

1. `RPOP site:seed_queue:<site_key>` — up to a configured batch size
2. Normalise URL, compute `url_hash`
3. `HSET crawl:url_map:<site_key> <url_hash> <url>` — register in URL map
4. Upsert `crawl_state` with `listing_status = "discovered"` if not already present
5. `ZADD NX crawl:queue:<site_key> -1 <url_hash>` — HIGH priority (same as new discovery)

This replaces any need to write directly to `crawl:queue` for bulk operations.
The UI and `manage.py seed-queue` expose this without operators touching Redis directly.

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
| Discovery container does not pick up trigger | Lock TTL expires after 3 hours; next trigger cycle proceeds |
| Job takes longer than interval | APScheduler skips the overlapping run (coalesce=True) |
| Manual trigger for a long-interval job | TriggerWatcherJob picks it up within 30 seconds regardless of job interval |
| Job disabled but manual trigger received | TriggerWatcherJob runs the job; enabled flag does not block manual runs |
