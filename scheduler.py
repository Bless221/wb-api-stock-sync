from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import Settings
from exceptions import CriticalAPIError

logger = logging.getLogger(__name__)

JOB_ID = "marketplace_stock_sync"


class SyncScheduler:
    """Thin wrapper around :class:`AsyncIOScheduler` for the sync job.

    Detects CriticalAPIError and initiates immediate graceful shutdown.
    """

    def __init__(
        self,
        settings: Settings,
        job: Callable[[], Awaitable[None]],
    ) -> None:
        self._settings = settings
        self._job = job
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._stop_event = asyncio.Event()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> AsyncIOScheduler:
        """Register the interval job and start the scheduler."""
        scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        scheduler.add_job(
            self._guarded_job,
            trigger=IntervalTrigger(minutes=self._settings.sync_interval_minutes),
            id=JOB_ID,
            name="Marketplace stock synchronisation",
            max_instances=1,      # never overlap two cycles
            coalesce=True,        # collapse missed runs into one
            misfire_grace_time=120,
            replace_existing=True,
        )
        scheduler.add_listener(self._on_job_problem, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        scheduler.start()
        self._scheduler = scheduler

        logger.info(
            "Scheduler started: every %d minute(s), job id=%s",
            self._settings.sync_interval_minutes,
            JOB_ID,
        )
        return scheduler

    async def run_forever(self) -> None:
        """Block the coroutine until :meth:`shutdown` is called."""
        await self._stop_event.wait()

    def shutdown(self, wait: bool = False) -> None:
        """Stop the scheduler and release :meth:`run_forever`."""
        if self._scheduler is not None and self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("Scheduler stopped")
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _guarded_job(self) -> None:
        """Run the job coroutine with special handling for critical errors."""
        try:
            await self._job()
        except CriticalAPIError as exc:
            # Log at CRITICAL level and stop the scheduler immediately
            logger.critical(
                "CRITICAL API ERROR DETECTED: %s | Shutting down scheduler to prevent spam",
                exc,
            )
            self.shutdown(wait=False)
        except asyncio.CancelledError:
            raise
        except Exception:  # noqa: BLE001 - scheduler must survive normal job errors
            logger.exception("Scheduled synchronisation crashed")

    @staticmethod
    def _on_job_problem(event: JobEvent) -> None:
        """Log APScheduler-level job failures and misfires."""
        if getattr(event, "exception", None) is not None:
            logger.error("APScheduler job '%s' raised: %s", event.job_id, event.exception)
        else:
            logger.warning("APScheduler job '%s' was missed", event.job_id)