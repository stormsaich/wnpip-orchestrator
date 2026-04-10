"""Orchestrator entry point.

Initialises connections, instantiates all jobs, registers them with APScheduler,
starts a background heartbeat thread, and hands control to BlockingScheduler.
"""

import signal
import sys

from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.interval import IntervalTrigger

from wnpip_shared import (
    Config,
    Heartbeat,
    RedisKeyBuilder,
    get_active_client,
    get_logger,
    get_redis_client,
)
from jobs import (
    ArchiveCheckerJob,
    DiscoveryTriggerJob,
    HealthMonitorJob,
    QueueReaperJob,
    RecrawlSchedulerJob,
    SuspectCheckerJob,
    TriggerWatcherJob,
)

log = get_logger("orchestrator.main")


def main() -> None:
    config = Config()

    log.info("Connecting to Redis...")
    redis = get_redis_client(config.redis_url)

    log.info("Connecting to MongoDB...")
    mongo_client = get_active_client(config.mongo_uri)
    db = mongo_client[config.mongo_db]
    crawl_state = db["crawl_state"]
    raw_listings = db["raw_listings"]

    # Publish the current baseline configuration so the UI can display defaults
    kb = RedisKeyBuilder()
    redis.hset(
        kb.config_current("orchestrator"),
        mapping={
            "RECRAWL_INTERVAL_MINUTES":          str(config.recrawl_interval_minutes),
            "SUSPECT_CHECK_INTERVAL_MINUTES":    str(config.suspect_check_interval_minutes),
            "HEALTH_CHECK_INTERVAL_SECONDS":     str(config.health_check_interval_seconds),
            "QUEUE_REAPER_INTERVAL_MINUTES":     str(config.queue_reaper_interval_minutes),
            "DISCOVERY_TRIGGER_INTERVAL_HOURS":  str(config.discovery_trigger_interval_hours),
            "INFLIGHT_TTL_SECONDS":              str(config.inflight_ttl_seconds),
            "ARCHIVE_AFTER_REMOVED_DAYS":        str(config.archive_after_removed_days),
            "RECRAWL_SCHEDULER_ENABLED":         "1",
            "SUSPECT_CHECKER_ENABLED":           "1",
            "ARCHIVE_CHECKER_ENABLED":           "1",
            "DISCOVERY_TRIGGER_ENABLED":         "1",
            "HEALTH_MONITOR_ENABLED":            "1",
            "QUEUE_REAPER_ENABLED":              "1",
        },
    )

    scheduler = BlockingScheduler()

    # Common constructor arguments
    job_args = (redis, crawl_state, raw_listings, config, scheduler)

    # Instantiate all jobs
    recrawl   = RecrawlSchedulerJob(*job_args)
    suspect   = SuspectCheckerJob(*job_args)
    archive   = ArchiveCheckerJob(*job_args)
    discovery = DiscoveryTriggerJob(*job_args)
    health    = HealthMonitorJob(*job_args)
    reaper    = QueueReaperJob(*job_args)

    # job_map is used by trigger_watcher to dispatch manual runs
    job_map = {
        "recrawl_scheduler": recrawl,
        "suspect_checker":   suspect,
        "archive_checker":   archive,
        "discovery_trigger": discovery,
        "health_monitor":    health,
        "queue_reaper":      reaper,
    }
    watcher = TriggerWatcherJob(*job_args, job_map=job_map)

    # Register jobs with APScheduler
    scheduler.add_job(
        recrawl.run,
        IntervalTrigger(minutes=config.recrawl_interval_minutes),
        id="recrawl_scheduler",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        suspect.run,
        IntervalTrigger(minutes=config.suspect_check_interval_minutes),
        id="suspect_checker",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        archive.run,
        IntervalTrigger(hours=24),  # Fixed daily; not dynamically overridable
        id="archive_checker",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        discovery.run,
        IntervalTrigger(hours=config.discovery_trigger_interval_hours),
        id="discovery_trigger",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        health.run,
        IntervalTrigger(seconds=config.health_check_interval_seconds),
        id="health_monitor",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        reaper.run,
        IntervalTrigger(minutes=config.queue_reaper_interval_minutes),
        id="queue_reaper",
        max_instances=1,
        coalesce=True,
    )
    scheduler.add_job(
        watcher.run,
        IntervalTrigger(seconds=30),
        id="trigger_watcher",
        max_instances=1,
        coalesce=True,
    )

    # Background heartbeat — writes health:orchestrator every 30s
    heartbeat = Heartbeat(redis, RedisKeyBuilder.health_orchestrator(), interval=30)
    heartbeat.start()

    def shutdown(signum, frame) -> None:
        log.info(f"Shutdown signal {signum} received — stopping orchestrator")
        heartbeat.stop()
        scheduler.shutdown(wait=True)
        mongo_client.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    log.info(
        "Orchestrator started. "
        f"recrawl={config.recrawl_interval_minutes}m "
        f"suspect={config.suspect_check_interval_minutes}m "
        f"health={config.health_check_interval_seconds}s "
        f"reaper={config.queue_reaper_interval_minutes}m "
        f"discovery={config.discovery_trigger_interval_hours}h "
        "archive=24h "
        "watcher=30s"
    )
    scheduler.start()  # Blocks until shutdown()


if __name__ == "__main__":
    main()
