"""SuspectCheckerJob — resolves listings in the suspect state.

For each suspect listing:
  - If there is a more-recent raw crawl and it confirmed the listing is active
    → revert to active.
  - If the recent crawl showed removal
    → confirm removal.
  - Otherwise
    → ensure the listing is queued at HIGH priority for an immediate re-crawl.
"""

from datetime import datetime, timedelta, timezone

from wnpip_shared import CrawlPriority, RedisKeyBuilder

from .base_job import BaseJob

REMOVAL_CONFIRMATION_COUNT = 1  # single confirmation is sufficient


class SuspectCheckerJob(BaseJob):
    JOB_NAME = "suspect_checker"
    ENABLED_FLAG = "SUSPECT_CHECKER_ENABLED"
    INTERVAL_PARAM = "SUSPECT_CHECK_INTERVAL_MINUTES"
    INTERVAL_UNIT = "minutes"

    def execute(self, overrides: dict, run_type: str) -> None:
        suspect_docs = list(
            self._crawl_state.find(
                {"listing_status": "suspect"},
                {"url_hash": 1, "site_key": 1, "suspect_since": 1},
            )
        )

        if not suspect_docs:
            self._log.info("No suspect listings found.")
            return

        reverted = 0
        confirmed_removed = 0
        requeued = 0

        for listing in suspect_docs:
            url_h = listing.get("url_hash")
            site_key = listing.get("site_key", "")
            suspect_since = listing.get("suspect_since")

            if not url_h:
                continue

            outcome = self._resolve(url_h, site_key, suspect_since)
            if outcome == "reverted":
                reverted += 1
            elif outcome == "removed":
                confirmed_removed += 1
            else:
                requeued += 1

        self._log.info(
            f"suspect_checker complete: reverted={reverted} "
            f"removed={confirmed_removed} requeued={requeued} "
            f"total={len(suspect_docs)}"
        )

    def _resolve(self, url_h: str, site_key: str, suspect_since) -> str:
        """Attempt to resolve a single suspect listing. Returns the outcome label."""
        now = datetime.now(timezone.utc)

        # Find the most recent raw crawl document
        raw_doc = self._raw_listings.find_one(
            {"url_hash": url_h},
            sort=[("crawled_at", -1)],
        )

        # Determine whether the raw crawl happened *after* the listing became suspect
        recrawled_since_suspect = False
        if raw_doc and suspect_since:
            crawled_at = raw_doc.get("crawled_at")
            if crawled_at is not None:
                # Normalise both to naive UTC for comparison if needed
                if hasattr(crawled_at, "tzinfo") and crawled_at.tzinfo is not None:
                    crawled_at_naive = crawled_at.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    crawled_at_naive = crawled_at

                if hasattr(suspect_since, "tzinfo") and suspect_since.tzinfo is not None:
                    suspect_since_naive = suspect_since.astimezone(timezone.utc).replace(tzinfo=None)
                else:
                    suspect_since_naive = suspect_since

                recrawled_since_suspect = crawled_at_naive > suspect_since_naive

        if raw_doc and recrawled_since_suspect:
            raw_status = raw_doc.get("listing_status", "")

            if raw_status == "active":
                # Revert to active — re-crawl in 12 hours
                self._crawl_state.update_one(
                    {"url_hash": url_h},
                    {
                        "$set": {
                            "listing_status": "active",
                            "next_crawl_at": datetime.utcnow() + timedelta(hours=12),
                            "updated_at": datetime.utcnow(),
                        },
                        "$unset": {"suspect_since": ""},
                    },
                )
                self._log.info(f"[{site_key}] Reverted {url_h} to active")
                return "reverted"

            if raw_status in ("removed", "removal_signal"):
                # Confirm removal — re-check in 7 days
                self._crawl_state.update_one(
                    {"url_hash": url_h},
                    {
                        "$set": {
                            "listing_status": "removed",
                            "removed_at": datetime.utcnow(),
                            "next_crawl_at": datetime.utcnow() + timedelta(days=7),
                            "updated_at": datetime.utcnow(),
                        },
                        "$unset": {"suspect_since": ""},
                    },
                )
                self._log.info(f"[{site_key}] Confirmed removal for {url_h}")
                return "removed"

        # No conclusive evidence yet — make sure it's queued for an immediate re-crawl
        if site_key:
            kb = RedisKeyBuilder(site_key)
            self._redis.zadd(kb.queue(), {url_h: CrawlPriority.HIGH}, nx=True)

        return "requeued"
