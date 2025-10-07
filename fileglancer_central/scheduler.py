from typing import Callable, Optional
from apscheduler.schedulers.background import BackgroundScheduler as APScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.triggers.interval import IntervalTrigger
from loguru import logger


# Global scheduler instance
_scheduler: Optional[APScheduler] = None


def get_scheduler(db_url: Optional[str] = None) -> APScheduler:
    """Get the global APScheduler instance with shared database jobstore."""
    global _scheduler
    if _scheduler is None:
        if db_url is None:
            # Fallback to simple in-memory scheduler if no db_url provided
            logger.warning("No database URL provided, using in-memory jobstore (not suitable for multi-worker)")
            _scheduler = APScheduler()
        else:
            # Configure shared database jobstore
            jobstore = SQLAlchemyJobStore(url=db_url, tablename='apscheduler_jobs')

            # Configure executor
            executor = ThreadPoolExecutor(max_workers=5)

            # Job defaults for robust operation
            job_defaults = {
                'coalesce': True,           # Combine missed executions if multiple are queued
                'max_instances': 1,         # Prevent overlapping executions of the same job
                'misfire_grace_time': 300   # 5 minutes grace period for late execution
            }

            _scheduler = APScheduler(
                jobstores={'default': jobstore},
                executors={'default': executor},
                job_defaults=job_defaults
            )

            logger.info(f"Configured APScheduler with shared database jobstore at {db_url}")

    return _scheduler


def start_scheduler(db_url: Optional[str] = None) -> None:
    """Start the global scheduler."""
    scheduler = get_scheduler(db_url)
    if not scheduler.running:
        scheduler.start()
        logger.info("APScheduler started successfully")
    else:
        logger.warning("APScheduler is already running")


def stop_scheduler() -> None:
    """Stop the global scheduler."""
    if _scheduler and _scheduler.running:
        _scheduler.shutdown()
        logger.info("APScheduler stopped")


def schedule_job(func: Callable, interval_minutes: int, job_id: str, db_url: Optional[str] = None) -> None:
    """Schedule a job to run periodically using APScheduler."""
    scheduler = get_scheduler(db_url)

    # Remove existing job if it exists
    try:
        scheduler.remove_job(job_id)
        logger.debug(f"Removed existing job '{job_id}'")
    except:
        pass  # Job doesn't exist, which is fine

    # Add the new job
    scheduler.add_job(
        func=func,
        trigger=IntervalTrigger(minutes=interval_minutes),
        id=job_id,
        name=f"Scheduled job: {job_id}",
        replace_existing=True,
        max_instances=1  # Prevent overlapping executions
    )

    logger.info(f"Scheduled job '{job_id}' to run every {interval_minutes} minutes using APScheduler")