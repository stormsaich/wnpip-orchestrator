"""ArchiveCheckerJob — moves long-removed listings to archived status.

Runs once every 24 hours (fixed interval; not overridable via INTERVAL_PARAM).
The threshold age is read from overrides or the Config default.
"""

from datetime import datetime, timedelta, timezone

from .base_job import BaseJob


class ArchiveCheckerJob(BaseJob):
    JOB_NAME = "archive_checker"
    ENABLED_FLAG = "ARCHIVE_CHECKER_ENABLED"
    INTERVAL_PARAM = None   # Fixed 24-hour schedule; no dynamic reschedule
    INTERVAL_UNIT = None    # Not used

    def execute(self, overrides: dict, run_type: str) -> None:
        archive_days = int(
            overrides.get("ARCHIVE_AFTER_REMOVED_DAYS") or self._config.archive_after_removed_days
        )
        threshold = datetime.utcnow() - timedelta(days=archive_days)
        now = datetime.utcnow()

        self._log.info(
            f"archive_checker: archiving listings removed before {threshold.isoformat()} "
            f"(>{archive_days} days ago)"
        )

        result = self._crawl_state.update_many(
            {
                "listing_status": "removed",
                "removed_at": {"$lt": threshold},
            },
            {
                "$set": {
                    "listing_status": "archived",
                    "archived_at": now,
                    "updated_at": now,
                }
            },
        )

        archived_count = result.modified_count
        self._log.info(f"archive_checker: archived {archived_count} listing(s)")

        # Queue scores for these newly-archived listings will be corrected to the
        # FAR_FUTURE offset on the next recrawl_scheduler pass — no Redis update needed here.
