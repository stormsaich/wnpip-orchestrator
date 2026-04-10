"""Sync admin utilities for the orchestrator manage.py CLI.

The shared library's admin_request_crawl is async (for the UI service).
This module provides a synchronous equivalent for use in manage.py.
"""

from datetime import datetime, timezone

from wnpip_shared import CrawlPriority, RedisKeyBuilder, normalise_url, url_hash


def admin_request_crawl(redis_client, db, site_key: str, url: str, notes: str) -> None:
    """Queue a URL for immediate (IMMEDIATE priority) crawl.

    Sync version for use by manage.py and other CLI tools.

    Args:
        redis_client: Sync Redis client (redis.Redis with decode_responses=True).
        db:           PyMongo database handle.
        site_key:     Canonical site key (e.g. "www_property24_com").
        url:          Fully-qualified URL to crawl.
        notes:        Operator-supplied reason (stored in audit log).
    """
    normalised = normalise_url(url)
    url_h = url_hash(normalised)
    kb = RedisKeyBuilder(site_key)

    # Ensure URL is recorded in the hash map
    redis_client.hset(kb.url_map(), url_h, normalised)

    # Enqueue at IMMEDIATE priority — overwrites any existing score (no NX flag)
    redis_client.zadd(kb.queue(), {url_h: CrawlPriority.IMMEDIATE})

    # Upsert crawl_state — insert if not present, set discovered status on insert
    db["crawl_state"].update_one(
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

    # Audit log entry (best-effort)
    try:
        db["admin_crawl_log"].insert_one(
            {
                "site_key": site_key,
                "url": normalised,
                "url_hash": url_h,
                "notes": notes,
                "requested_at": datetime.now(timezone.utc).isoformat(),
                "requested_by": "cli-admin",
            }
        )
    except Exception:
        pass  # Mongo hiccup — crawl is still queued in Redis
