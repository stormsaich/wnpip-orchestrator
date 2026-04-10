"""Orchestrator management CLI.

Usage examples:
    python manage.py register-site --site-key www_example_com --website www.example.com --display-name "Example"
    python manage.py list-sites
    python manage.py seed-queue --site www_example_com --file urls.txt
    python manage.py crawl-now --site www_example_com --url https://www.example.com/listing/123
    python manage.py queue-stats
    python manage.py queue-stats --site www_example_com
    python manage.py health
"""

import argparse
import json
import sys
from datetime import datetime, timezone

from dotenv import load_dotenv

load_dotenv(override=False)

from wnpip_shared import (
    Config,
    RedisKeyBuilder,
    get_active_client,
    get_logger,
    get_redis_client,
    site_key_from_website,
)
from admin import admin_request_crawl

log = get_logger("orchestrator.manage")


# ── Connection helpers ─────────────────────────────────────────────────────────

def _connect(config: Config):
    redis = get_redis_client(config.redis_url)
    mongo_client = get_active_client(config.mongo_uri)
    db = mongo_client[config.mongo_db]
    return redis, mongo_client, db


# ── Command handlers ───────────────────────────────────────────────────────────

def cmd_register_site(args, config: Config) -> None:
    redis, mongo_client, db = _connect(config)
    try:
        site_key = args.site_key
        website = args.website
        display_name = args.display_name
        listing_url_pattern = getattr(args, "listing_url_pattern", None) or ""

        # Add to the global sites set
        redis.sadd(RedisKeyBuilder.sites(), site_key)

        # Store site config in Redis hash
        site_config_key = f"site:config:{site_key}"
        redis.hset(
            site_config_key,
            mapping={
                "site_key": site_key,
                "website": website,
                "display_name": display_name,
                "listing_url_pattern": listing_url_pattern,
                "enabled": "1",
                "registered_at": datetime.now(timezone.utc).isoformat(),
            },
        )

        # Upsert site record in MongoDB
        db["sites"].update_one(
            {"site_key": site_key},
            {
                "$set": {
                    "site_key": site_key,
                    "website": website,
                    "display_name": display_name,
                    "listing_url_pattern": listing_url_pattern,
                    "enabled": True,
                    "updated_at": datetime.now(timezone.utc),
                },
                "$setOnInsert": {
                    "registered_at": datetime.now(timezone.utc),
                },
            },
            upsert=True,
        )

        print(f"Registered site: {site_key!r} ({website}) — {display_name!r}")
    finally:
        mongo_client.close()


def cmd_list_sites(args, config: Config) -> None:
    redis, mongo_client, db = _connect(config)
    try:
        sites = sorted(redis.smembers(RedisKeyBuilder.sites()))
        if not sites:
            print("No sites registered.")
            return

        print(f"{'SITE KEY':<35} {'WEBSITE':<35} {'DISPLAY NAME':<30} {'ENABLED'}")
        print("-" * 110)
        for site_key in sites:
            site_config = redis.hgetall(f"site:config:{site_key}") or {}
            website = site_config.get("website", "—")
            display_name = site_config.get("display_name", "—")
            enabled = "yes" if site_config.get("enabled", "1") == "1" else "no"
            print(f"{site_key:<35} {website:<35} {display_name:<30} {enabled}")
    finally:
        mongo_client.close()


def cmd_seed_queue(args, config: Config) -> None:
    redis, mongo_client, db = _connect(config)
    try:
        site_key = args.site
        file_path = args.file
        seed_key = f"site:seed_queue:{site_key}"

        count = 0
        skipped = 0
        with open(file_path, "r", encoding="utf-8") as fh:
            for line in fh:
                url = line.strip()
                if not url or url.startswith("#"):
                    skipped += 1
                    continue
                redis.lpush(seed_key, url)
                count += 1

        print(f"Pushed {count} URL(s) to seed queue for {site_key!r} (skipped {skipped} blank/comment lines)")
    finally:
        mongo_client.close()


def cmd_crawl_now(args, config: Config) -> None:
    redis, mongo_client, db = _connect(config)
    try:
        site_key = args.site
        url = args.url
        notes = getattr(args, "notes", "") or ""

        admin_request_crawl(redis, db, site_key, url, notes)
        print(f"Queued {url!r} for immediate crawl on site {site_key!r}")
        if notes:
            print(f"Notes: {notes}")
    finally:
        mongo_client.close()


def cmd_queue_stats(args, config: Config) -> None:
    redis, mongo_client, db = _connect(config)
    try:
        site_key_filter = getattr(args, "site", None)

        if site_key_filter:
            sites = [site_key_filter]
        else:
            sites = sorted(redis.smembers(RedisKeyBuilder.sites()))

        if not sites:
            print("No sites found.")
            return

        print(f"{'SITE KEY':<35} {'QUEUE':<10} {'INFLIGHT':<10} {'URL MAP':<10}")
        print("-" * 70)
        for site_key in sites:
            kb = RedisKeyBuilder(site_key)
            queue_depth = redis.zcard(kb.queue())
            inflight_depth = redis.zcard(kb.inflight())
            url_map_size = redis.hlen(kb.url_map())
            print(f"{site_key:<35} {queue_depth:<10} {inflight_depth:<10} {url_map_size:<10}")
    finally:
        mongo_client.close()


def cmd_health(args, config: Config) -> None:
    redis, mongo_client, db = _connect(config)
    try:
        now = datetime.now(timezone.utc)

        def _age_str(key: str) -> str:
            raw = redis.get(key)
            if raw is None:
                return "MISSING"
            try:
                ts = datetime.fromisoformat(raw)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                age = (now - ts).total_seconds()
                return f"{age:.0f}s ago"
            except Exception:
                return f"PARSE ERROR: {raw!r}"

        print("=== Orchestrator Health ===")
        print(f"health:orchestrator    : {_age_str(RedisKeyBuilder.health_orchestrator())}")
        print(f"health:proxy_service   : {_age_str(RedisKeyBuilder.health_proxy_service())}")

        sites = sorted(redis.smembers(RedisKeyBuilder.sites()))
        print(f"\n=== Crawler Heartbeats ({len(sites)} site(s)) ===")
        for site_key in sites:
            kb = RedisKeyBuilder(site_key)
            hb_age = _age_str(kb.health_crawler())
            print(f"  {site_key:<35} : {hb_age}")

        # Proxy pool summary
        print("\n=== Proxy Pool ===")
        available = redis.zcard(RedisKeyBuilder.proxy_pool_available())
        total = redis.scard(RedisKeyBuilder.proxy_pool_all())
        ratio = f"{100 * available / total:.0f}%" if total > 0 else "N/A"
        print(f"  Available: {available}/{total} ({ratio})")

        # Recent alerts (show last 5)
        print("\n=== Recent Alerts (last 5) ===")
        alerts_key = RedisKeyBuilder.alerts_queue()
        alerts_raw = redis.lrange(alerts_key, 0, 4)
        if not alerts_raw:
            print("  No alerts.")
        else:
            for raw in alerts_raw:
                try:
                    alert = json.loads(raw)
                    ts = alert.get("timestamp", "?")
                    sev = alert.get("severity", "?").upper()
                    check = alert.get("check", "?")
                    sk = alert.get("site_key") or "global"
                    msg = alert.get("message", "")
                    print(f"  [{sev}] {ts} | {check} | {sk} | {msg}")
                except Exception:
                    print(f"  {raw}")
    finally:
        mongo_client.close()


# ── CLI wiring ─────────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="manage.py",
        description="WNPIP Orchestrator management CLI",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # register-site
    p_reg = sub.add_parser("register-site", help="Register a new site in the pipeline")
    p_reg.add_argument("--site-key", required=True, help="Canonical site key, e.g. www_example_com")
    p_reg.add_argument("--website", required=True, help="Domain, e.g. www.example.com")
    p_reg.add_argument("--display-name", required=True, help="Human-readable name")
    p_reg.add_argument("--listing-url-pattern", default="", help="Regex pattern to identify listing URLs")

    # list-sites
    sub.add_parser("list-sites", help="List all registered sites")

    # seed-queue
    p_seed = sub.add_parser("seed-queue", help="Push URLs from a file into a site's seed queue")
    p_seed.add_argument("--site", required=True, help="Site key")
    p_seed.add_argument("--file", required=True, help="Path to a text file with one URL per line")

    # crawl-now
    p_crawl = sub.add_parser("crawl-now", help="Immediately queue a URL for crawling")
    p_crawl.add_argument("--site", required=True, help="Site key")
    p_crawl.add_argument("--url", required=True, help="URL to crawl")
    p_crawl.add_argument("--notes", default="", help="Operator notes (stored in audit log)")

    # queue-stats
    p_stats = sub.add_parser("queue-stats", help="Show queue depths for all (or one) site")
    p_stats.add_argument("--site", default=None, help="Limit to a specific site key")

    # health
    sub.add_parser("health", help="Show service heartbeat ages and recent alerts")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    try:
        config = Config()
    except Exception as exc:
        print(f"ERROR: Failed to load config: {exc}", file=sys.stderr)
        sys.exit(1)

    dispatch = {
        "register-site": cmd_register_site,
        "list-sites":    cmd_list_sites,
        "seed-queue":    cmd_seed_queue,
        "crawl-now":     cmd_crawl_now,
        "queue-stats":   cmd_queue_stats,
        "health":        cmd_health,
    }

    handler = dispatch.get(args.command)
    if handler is None:
        print(f"Unknown command: {args.command}", file=sys.stderr)
        sys.exit(1)

    try:
        handler(args, config)
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        log.exception(f"manage.py command {args.command!r} failed")
        sys.exit(1)


if __name__ == "__main__":
    main()
