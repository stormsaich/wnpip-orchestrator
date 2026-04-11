"""HealthMonitorJob — checks crawler heartbeats, queue depths, proxy pool health.

Emits structured alert objects to the alerts:queue Redis list and writes
the orchestrator's own self-heartbeat.
"""

import json
import uuid
from datetime import datetime, timezone

from wnpip_shared import RedisKeyBuilder

from .base_job import BaseJob

ALERTS_HISTORY_KEY = "alerts:history"

_HEARTBEAT_CRITICAL_AGE_SECONDS = 300
_HEARTBEAT_WARNING_AGE_SECONDS = 120
_QUEUE_DEPTH_WARNING = 100_000
_SUSPECT_COUNT_WARNING = 500
_PROXY_POOL_LOW_RATIO = 0.2


class HealthMonitorJob(BaseJob):
    JOB_NAME = "health_monitor"
    ENABLED_FLAG = "HEALTH_MONITOR_ENABLED"
    INTERVAL_PARAM = "HEALTH_CHECK_INTERVAL_SECONDS"
    INTERVAL_UNIT = "seconds"

    def execute(self, overrides: dict, run_type: str) -> None:
        alerts: list[tuple[str, str, str | None, str]] = []
        now = datetime.now(timezone.utc)

        sites = self._redis.smembers(RedisKeyBuilder.sites())
        for site_key in sites:
            self._check_site(site_key, now, alerts)

        self._check_proxy_pool(alerts)

        for severity, check, site_key, message in alerts:
            self._emit_alert(severity, check, site_key, message, now)

        # Self-heartbeat — 300s TTL so health_monitor itself can be monitored
        self._redis.set(
            RedisKeyBuilder.health_orchestrator(),
            now.isoformat(),
            ex=_HEARTBEAT_CRITICAL_AGE_SECONDS,
        )

        if alerts:
            self._log.warning(
                f"health_monitor: {len(alerts)} alert(s) emitted across {len(sites)} site(s)"
            )
        else:
            self._log.info(f"health_monitor: all checks passed for {len(sites)} site(s)")

    # ── Per-site checks ────────────────────────────────────────────────────────

    def _check_site(
        self,
        site_key: str,
        now: datetime,
        alerts: list,
    ) -> None:
        kb = RedisKeyBuilder(site_key)

        # Crawler and discovery heartbeats
        for service_label, hb_key in (
            ("crawler", kb.health_crawler()),
            ("discovery", kb.health_discovery()),
        ):
            self._check_heartbeat(service_label, hb_key, site_key, now, alerts)

        # Queue depth
        depth = self._redis.zcard(kb.queue())
        if depth > _QUEUE_DEPTH_WARNING:
            alerts.append(
                (
                    "WARNING",
                    "queue_depth_high",
                    site_key,
                    f"Crawl queue depth {depth} for {site_key} exceeds {_QUEUE_DEPTH_WARNING}",
                )
            )

        # Suspect count
        try:
            suspect_count = self._crawl_state.count_documents(
                {"site_key": site_key, "listing_status": "suspect"}
            )
            if suspect_count > _SUSPECT_COUNT_WARNING:
                alerts.append(
                    (
                        "WARNING",
                        "suspect_count_high",
                        site_key,
                        f"{suspect_count} suspect listings for {site_key} (threshold {_SUSPECT_COUNT_WARNING})",
                    )
                )
        except Exception as exc:
            self._log.warning(f"[{site_key}] Failed to count suspect listings: {exc}")

    def _check_heartbeat(
        self,
        service_label: str,
        hb_key: str,
        site_key: str,
        now: datetime,
        alerts: list,
    ) -> None:
        hb_raw = self._redis.get(hb_key)
        if hb_raw is None:
            alerts.append((
                "CRITICAL",
                f"{service_label}_heartbeat_missing",
                site_key,
                f"{service_label.capitalize()} heartbeat missing — no data in Redis for {site_key}",
            ))
            return
        try:
            hb_dt = datetime.fromisoformat(hb_raw)
            if hb_dt.tzinfo is None:
                hb_dt = hb_dt.replace(tzinfo=timezone.utc)
            age = (now - hb_dt).total_seconds()
            if age > _HEARTBEAT_CRITICAL_AGE_SECONDS:
                alerts.append((
                    "CRITICAL",
                    f"{service_label}_heartbeat_stale",
                    site_key,
                    f"{service_label.capitalize()} heartbeat stale for {site_key} — last seen {age:.0f}s ago (threshold {_HEARTBEAT_CRITICAL_AGE_SECONDS}s)",
                ))
            elif age > _HEARTBEAT_WARNING_AGE_SECONDS:
                alerts.append((
                    "WARNING",
                    f"{service_label}_heartbeat_stale",
                    site_key,
                    f"{service_label.capitalize()} heartbeat stale for {site_key} — last seen {age:.0f}s ago (threshold {_HEARTBEAT_WARNING_AGE_SECONDS}s)",
                ))
        except Exception as exc:
            alerts.append((
                "WARNING",
                f"{service_label}_heartbeat_parse_error",
                site_key,
                f"Cannot parse {service_label} heartbeat for {site_key}: {exc}",
            ))

    # ── Global checks ──────────────────────────────────────────────────────────

    def _check_proxy_pool(self, alerts: list) -> None:
        try:
            available = self._redis.zcard(RedisKeyBuilder.proxy_pool_available())
            total = self._redis.scard(RedisKeyBuilder.proxy_pool_all())
            if total > 0 and (available / total) < _PROXY_POOL_LOW_RATIO:
                alerts.append(
                    (
                        "WARNING",
                        "proxy_pool_low",
                        None,
                        f"Proxy pool low — {available}/{total} proxies available "
                        f"({100 * available / total:.0f}% < {_PROXY_POOL_LOW_RATIO * 100:.0f}%)",
                    )
                )
        except Exception as exc:
            self._log.warning(f"Failed to check proxy pool health: {exc}")

    # ── Alert emission ─────────────────────────────────────────────────────────

    def _emit_alert(
        self,
        severity: str,
        check: str,
        site_key: str | None,
        message: str,
        now: datetime,
    ) -> None:
        timestamp_ms = int(now.timestamp() * 1000)
        alert_id = str(uuid.uuid4())
        payload = json.dumps(
            {
                "id": alert_id,
                "severity": severity,
                "service": "orchestrator",
                "check": check,
                "site_key": site_key,
                "message": message,
                # Fields the UI template reads for timestamp display
                "ts": now.strftime("%Y-%m-%d %H:%M:%S"),
                "timestamp_ms": timestamp_ms,
            }
        )
        try:
            # Write to alerts:history sorted set (score = timestamp_ms)
            # The UI reads this with ZREVRANGE to get newest-first
            self._redis.zadd(ALERTS_HISTORY_KEY, {payload: timestamp_ms})
        except Exception as exc:
            self._log.error(f"Failed to push alert to history: {exc}")
