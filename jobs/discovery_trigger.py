"""DiscoveryTriggerJob — signals crawler services to run a full discovery pass.

Uses a per-site Redis lock (3-hour TTL) to prevent concurrent discovery jobs
from being triggered for the same site.
"""

import json
from datetime import datetime, timezone

from wnpip_shared import RedisKeyBuilder

from .base_job import BaseJob

_DISCOVERY_LOCK_TTL_SECONDS = 10_800  # 3 hours


class DiscoveryTriggerJob(BaseJob):
    JOB_NAME = "discovery_trigger"
    ENABLED_FLAG = "DISCOVERY_TRIGGER_ENABLED"
    INTERVAL_PARAM = "DISCOVERY_TRIGGER_INTERVAL_HOURS"
    INTERVAL_UNIT = "hours"

    def execute(self, overrides: dict, run_type: str) -> None:
        sites = self._redis.smembers(RedisKeyBuilder.sites())
        if not sites:
            self._log.info("No sites registered; skipping discovery trigger.")
            return

        triggered = 0
        skipped = 0

        for site_key in sites:
            kb = RedisKeyBuilder(site_key)
            lock_key = kb.discovery_lock()  # orchestrator:lock:discovery:<site_key>

            acquired = self._redis.set(lock_key, "1", nx=True, ex=_DISCOVERY_LOCK_TTL_SECONDS)
            if not acquired:
                self._log.info(f"Discovery lock already held for {site_key}; skipping.")
                skipped += 1
                continue

            payload = json.dumps(
                {
                    "site_key": site_key,
                    "triggered_at": datetime.now(timezone.utc).isoformat(),
                    "triggered_by": "orchestrator",
                }
            )
            self._redis.lpush(f"discovery:trigger:{site_key}", payload)
            self._log.info(f"Discovery triggered for {site_key}")
            triggered += 1

        self._log.info(
            f"discovery_trigger complete: triggered={triggered} skipped={skipped}"
        )
