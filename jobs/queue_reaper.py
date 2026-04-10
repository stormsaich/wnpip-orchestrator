"""QueueReaperJob — reclaims stale in-flight items back into the crawl queue.

An item becomes stale when its timestamp in the inflight sorted set is older
than INFLIGHT_TTL_SECONDS. This can happen if a crawler process crashed mid-crawl.
"""

import time

from wnpip_shared import RedisKeyBuilder

from .base_job import BaseJob


class QueueReaperJob(BaseJob):
    JOB_NAME = "queue_reaper"
    ENABLED_FLAG = "QUEUE_REAPER_ENABLED"
    INTERVAL_PARAM = "QUEUE_REAPER_INTERVAL_MINUTES"
    INTERVAL_UNIT = "minutes"

    def execute(self, overrides: dict, run_type: str) -> None:
        ttl = int(
            overrides.get("INFLIGHT_TTL_SECONDS") or self._config.inflight_ttl_seconds
        )
        threshold_ms = int((time.time() - ttl) * 1000)

        sites = self._redis.smembers(RedisKeyBuilder.sites())
        total_reaped = 0

        for site_key in sites:
            reaped = self._reap_site(site_key, threshold_ms)
            total_reaped += reaped

        self._log.info(
            f"queue_reaper complete: reaped={total_reaped} across {len(sites)} site(s) "
            f"(ttl={ttl}s)"
        )

    def _reap_site(self, site_key: str, threshold_ms: int) -> int:
        kb = RedisKeyBuilder(site_key)
        inflight_key = kb.inflight()
        queue_key = kb.queue()

        # Find all items whose inflight score (enqueued-at ms) is older than the threshold
        stale = self._redis.zrangebyscore(inflight_key, 0, threshold_ms)
        if not stale:
            return 0

        now_ms = int(time.time() * 1000)
        pipe = self._redis.pipeline()
        for url_h in stale:
            pipe.zrem(inflight_key, url_h)
            # Re-add to queue with current timestamp as score (NX: don't touch already-queued)
            pipe.zadd(queue_key, {url_h: now_ms}, nx=True)
        pipe.execute()

        self._log.warning(
            f"[{site_key}] Reaped {len(stale)} stale inflight item(s) "
            f"(threshold_ms={threshold_ms})"
        )
        return len(stale)
