"""TriggerWatcherJob — polls for manual trigger messages and dispatches them.

Runs every 30 seconds. For each registered job it checks the job's trigger
list via RPOP. When a message is found it executes that job immediately outside
of its normal schedule, recording the run as run_type="manual".

Design note: trigger_watcher itself uses the standard BaseJob.run() template
(so it benefits from status writing and event publishing). Its execute() method
is what dispatches to other jobs.
"""

import json
import time

from .base_job import BaseJob


class TriggerWatcherJob(BaseJob):
    JOB_NAME = "trigger_watcher"
    ENABLED_FLAG = ""    # Always enabled
    INTERVAL_PARAM = ""  # Fixed 30-second schedule set in main.py
    INTERVAL_UNIT = "seconds"

    def __init__(self, redis, crawl_state, raw_listings, config, scheduler, job_map: dict) -> None:
        super().__init__(redis, crawl_state, raw_listings, config, scheduler)
        self._job_map = job_map  # dict[str, BaseJob] — all other jobs, keyed by JOB_NAME

    def execute(self, overrides: dict, run_type: str) -> None:
        dispatched = 0

        for job_name, job in self._job_map.items():
            trigger_key = f"orchestrator:job:{job_name}:trigger"
            msg = self._redis.rpop(trigger_key)
            if msg is None:
                continue

            self._log.info(f"Manual trigger consumed for {job_name!r}: {msg}")

            try:
                parsed = json.loads(msg)
                requested_by = parsed.get("triggered_by", "unknown")
            except Exception:
                requested_by = "unknown"

            self._dispatch(job, job_name, requested_by)
            dispatched += 1

        if dispatched:
            self._log.info(f"trigger_watcher: dispatched {dispatched} manual job(s)")

    def _dispatch(self, job: "BaseJob", job_name: str, requested_by: str) -> None:
        """Execute a job synchronously as a manual run, recording status and events."""
        job._publish_event("job_triggered", requested_by=requested_by)
        self._redis.hset(f"orchestrator:job:{job_name}:status", "is_running", "1")
        job._publish_event("job_started", run_type="manual")

        start = time.time()
        result = "ok"
        error = ""
        try:
            overrides = job._read_overrides()
            job.execute(overrides, "manual")
        except Exception as exc:
            result = "error"
            error = str(exc)
            self._log.exception(f"Manual execution of {job_name!r} failed: {exc}")
        finally:
            duration_ms = int((time.time() - start) * 1000)
            job._write_status(result, duration_ms, "manual", error)
            job._publish_event("job_completed", result=result, duration_ms=duration_ms)
