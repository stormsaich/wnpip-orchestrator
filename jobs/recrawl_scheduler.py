"""RecrawlSchedulerJob — drains seed queues and enqueues listings due for re-crawl."""

import time
from datetime import datetime, timezone

from wnpip_shared import CrawlPriority, RedisKeyBuilder, normalise_url, url_hash

from .base_job import BaseJob

_SEED_BATCH = 500   # max URLs to drain from the seed queue per cycle
_MONGO_LIMIT = 1000  # max listings to query from crawl_state per cycle


class RecrawlSchedulerJob(BaseJob):
    JOB_NAME = "recrawl_scheduler"
    ENABLED_FLAG = "RECRAWL_SCHEDULER_ENABLED"
    INTERVAL_PARAM = "RECRAWL_INTERVAL_MINUTES"
    INTERVAL_UNIT = "minutes"

    def execute(self, overrides: dict, run_type: str) -> None:
        sites = self._redis.smembers(RedisKeyBuilder.sites())
        if not sites:
            self._log.info("No sites registered; nothing to schedule.")
            return

        total_seeded = 0
        total_queued = 0

        for site_key in sites:
            seeded, queued = self._process_site(site_key)
            total_seeded += seeded
            total_queued += queued

        self._log.info(
            f"recrawl_scheduler complete: seeded={total_seeded} queued={total_queued} "
            f"across {len(sites)} site(s)"
        )

    def _process_site(self, site_key: str) -> tuple[int, int]:
        kb = RedisKeyBuilder(site_key)
        seeded = self._drain_seed_queue(site_key, kb)
        queued = self._enqueue_due_listings(site_key, kb)
        return seeded, queued

    def _drain_seed_queue(self, site_key: str, kb: RedisKeyBuilder) -> int:
        """Pop raw URLs from site:seed_queue:<site_key>, normalise, and enqueue."""
        seed_key = f"site:seed_queue:{site_key}"
        count = 0

        for _ in range(_SEED_BATCH):
            raw_url = self._redis.rpop(seed_key)
            if raw_url is None:
                break

            try:
                normalised = normalise_url(raw_url)
                url_h = url_hash(normalised)
            except Exception as exc:
                self._log.warning(f"[{site_key}] Failed to normalise seed URL {raw_url!r}: {exc}")
                continue

            # Register in the URL map
            self._redis.hset(kb.url_map(), url_h, normalised)

            # Enqueue at HIGH priority (NX — don't demote already-queued items)
            self._redis.zadd(kb.queue(), {url_h: CrawlPriority.HIGH}, nx=True)

            # Upsert crawl_state — only insert if new
            self._crawl_state.update_one(
                {"url_hash": url_h, "site_key": site_key},
                {
                    "$setOnInsert": {
                        "url_hash": url_h,
                        "site_key": site_key,
                        "url": normalised,
                        "listing_status": "discovered",
                        "created_at": datetime.now(timezone.utc),
                        "next_crawl_at": datetime.now(timezone.utc),
                    }
                },
                upsert=True,
            )
            count += 1

        if count:
            self._log.info(f"[{site_key}] Drained {count} seed URLs")
        return count

    def _enqueue_due_listings(self, site_key: str, kb: RedisKeyBuilder) -> int:
        """Query crawl_state for listings due a re-crawl and enqueue them."""
        now = datetime.utcnow()

        cursor = self._crawl_state.find(
            {
                "site_key": site_key,
                "listing_status": {"$in": ["discovered", "active", "suspect", "removed"]},
                "next_crawl_at": {"$lte": now},
            },
            {"url_hash": 1, "listing_status": 1, "next_crawl_at": 1},
            limit=_MONGO_LIMIT,
        )

        count = 0
        for doc in cursor:
            url_h = doc.get("url_hash")
            if not url_h:
                continue

            score = self._compute_score(doc)
            # NX — don't demote items already at a higher priority
            self._redis.zadd(kb.queue(), {url_h: score}, nx=True)
            count += 1

        if count:
            self._log.info(f"[{site_key}] Enqueued {count} listings for re-crawl")
        return count

    @staticmethod
    def _compute_score(doc: dict) -> int | float:
        """Map listing_status to a crawl queue score."""
        status = doc.get("listing_status", "active")
        next_crawl_at = doc.get("next_crawl_at")

        if status in ("discovered", "suspect"):
            return CrawlPriority.HIGH

        # Convert next_crawl_at to Unix milliseconds
        if isinstance(next_crawl_at, datetime):
            ts_ms = int(next_crawl_at.timestamp() * 1000)
        else:
            ts_ms = int(time.time() * 1000)

        if status == "active":
            return ts_ms

        # removed / archived → far future
        return ts_ms + CrawlPriority.FAR_FUTURE_OFFSET_MS
