"""Base class for all orchestrator jobs.

Provides the run() template, config-override handling, APScheduler reschedule
logic, status writing, and pub/sub event publishing.
"""

import json
import time
from datetime import datetime, timezone

from apscheduler.triggers.interval import IntervalTrigger

from wnpip_shared import get_logger


class BaseJob:
    """Template-method base for all scheduled orchestrator jobs.

    Subclasses must set the class-level attributes and implement execute().
    """

    JOB_NAME: str = ""        # e.g. "recrawl_scheduler"
    ENABLED_FLAG: str = ""    # e.g. "RECRAWL_SCHEDULER_ENABLED" — empty = always on
    INTERVAL_PARAM: str = ""  # e.g. "RECRAWL_INTERVAL_MINUTES"  — empty = fixed schedule
    INTERVAL_UNIT: str = "minutes"  # "seconds" | "minutes" | "hours"

    def __init__(self, redis, crawl_state, raw_listings, config, scheduler) -> None:
        self._redis = redis
        self._crawl_state = crawl_state
        self._raw_listings = raw_listings
        self._config = config
        self._scheduler = scheduler
        self._log = get_logger(f"orchestrator.{self.JOB_NAME or 'base'}")

    # ── Override / config helpers ──────────────────────────────────────────────

    def _read_overrides(self) -> dict:
        """Return the current UI-written overrides hash from Redis."""
        return self._redis.hgetall("config:orchestrator:overrides") or {}

    def _is_enabled(self, overrides: dict) -> bool:
        """Return True if this job is enabled (defaults to True when no flag)."""
        if not self.ENABLED_FLAG:
            return True
        raw = overrides.get(self.ENABLED_FLAG, "1")
        return raw in ("1", "true", "True")

    def _check_trigger(self) -> tuple[bool, str]:
        """Pop a manual trigger message from this job's trigger list.

        Returns (triggered, raw_message).
        """
        msg = self._redis.rpop(f"orchestrator:job:{self.JOB_NAME}:trigger")
        return (msg is not None), (msg or "")

    def _get_current_interval(self) -> int | float | None:
        """Read the current interval value from the live APScheduler job trigger."""
        try:
            job = self._scheduler.get_job(self.JOB_NAME)
            if job is None:
                return None
            trigger = job.trigger
            # IntervalTrigger stores interval as a timedelta
            if hasattr(trigger, "interval"):
                td = trigger.interval
                total_seconds = td.total_seconds()
                if self.INTERVAL_UNIT == "seconds":
                    return total_seconds
                elif self.INTERVAL_UNIT == "minutes":
                    return total_seconds / 60
                elif self.INTERVAL_UNIT == "hours":
                    return total_seconds / 3600
        except Exception:
            pass
        return None

    def _reschedule_if_needed(self, overrides: dict) -> None:
        """Compare effective interval with the live APScheduler trigger and reschedule if changed."""
        if not self.INTERVAL_PARAM:
            # Fixed schedule (e.g. archive_checker at 24h) — no dynamic reschedule
            return

        # Determine effective interval from overrides or config default
        raw = overrides.get(self.INTERVAL_PARAM)
        if raw is not None:
            try:
                new_val = int(raw)
            except (ValueError, TypeError):
                return
        else:
            new_val = getattr(self._config, self.INTERVAL_PARAM.lower(), None)
            if new_val is None:
                return

        current_val = self._get_current_interval()
        if current_val is None:
            return

        # Allow a small float tolerance before triggering a reschedule
        if abs(float(new_val) - float(current_val)) < 0.01:
            return

        self._log.info(
            f"Rescheduling {self.JOB_NAME}: {current_val} → {new_val} {self.INTERVAL_UNIT}"
        )
        kwargs = {self.INTERVAL_UNIT: new_val}
        try:
            self._scheduler.reschedule_job(self.JOB_NAME, trigger=IntervalTrigger(**kwargs))
            self._publish_event(
                "interval_changed",
                job_name=self.JOB_NAME,
                old_value=current_val,
                new_value=new_val,
                unit=self.INTERVAL_UNIT,
            )
        except Exception as exc:
            self._log.warning(f"Failed to reschedule {self.JOB_NAME}: {exc}")

    # ── Status / event helpers ─────────────────────────────────────────────────

    def _write_status(self, result: str, duration_ms: int, run_type: str, error: str = "") -> None:
        """Persist job run metadata to Redis and increment the run counter."""
        now_iso = datetime.now(timezone.utc).isoformat()

        # Compute next scheduled run
        next_run_at = ""
        try:
            job = self._scheduler.get_job(self.JOB_NAME)
            if job and job.next_run_time:
                next_run_at = job.next_run_time.isoformat()
        except Exception:
            pass

        status_key = f"orchestrator:job:{self.JOB_NAME}:status"
        self._redis.hset(
            status_key,
            mapping={
                "last_run_at": now_iso,
                "last_result": result,
                "last_duration_ms": str(duration_ms),
                "last_run_type": run_type,
                "last_error": error,
                "next_run_at": next_run_at,
                "is_running": "0",
            },
        )
        self._redis.hincrby(status_key, "run_count", 1)

    def _publish_event(self, event_type: str, **kwargs) -> None:
        """Publish a structured event to the orchestrator events channel."""
        payload = {
            "event": event_type,
            "job": self.JOB_NAME,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **kwargs,
        }
        try:
            self._redis.publish("orchestrator:events", json.dumps(payload))
        except Exception as exc:
            self._log.warning(f"Failed to publish event {event_type}: {exc}")

    # ── Template method ────────────────────────────────────────────────────────

    def run(self, run_type: str = "scheduled") -> None:
        """Entry point called by APScheduler.

        Implements the standard lifecycle:
          1. Read config overrides.
          2. Check for a manual trigger (only when called as scheduled).
          3. Skip if disabled (unless manually triggered).
          4. Reschedule if interval has changed.
          5. Execute the job body.
          6. Write status and publish completion event.
        """
        overrides = self._read_overrides()

        if run_type == "scheduled":
            has_trigger, trigger_data = self._check_trigger()
            if has_trigger:
                run_type = "manual"
                try:
                    parsed = json.loads(trigger_data)
                    requested_by = parsed.get("triggered_by", "unknown")
                except Exception:
                    requested_by = "unknown"
                self._publish_event("job_triggered", requested_by=requested_by)
            elif not self._is_enabled(overrides):
                self._publish_event("job_skipped", reason="disabled")
                return

        self._reschedule_if_needed(overrides)

        self._redis.hset(f"orchestrator:job:{self.JOB_NAME}:status", "is_running", "1")
        self._publish_event("job_started", run_type=run_type)

        start = time.time()
        result = "ok"
        error = ""
        try:
            self.execute(overrides, run_type)
        except Exception as exc:
            result = "error"
            error = str(exc)
            self._log.exception(f"Job {self.JOB_NAME} failed: {exc}")
        finally:
            duration_ms = int((time.time() - start) * 1000)
            self._write_status(result, duration_ms, run_type, error)
            self._publish_event("job_completed", result=result, duration_ms=duration_ms)

    def execute(self, overrides: dict, run_type: str) -> None:
        """Override in subclasses to implement the actual job logic."""
        raise NotImplementedError(f"{self.__class__.__name__} must implement execute()")
