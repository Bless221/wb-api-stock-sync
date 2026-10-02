from __future__ import annotations

import asyncio
import logging
from typing import Awaitable, Callable, Optional

from apscheduler.events import EVENT_JOB_ERROR, EVENT_JOB_MISSED, JobEvent
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.interval import IntervalTrigger

from config import Settings

logger = logging.getLogger(__name__)

JOB_ID = "marketplace_stock_sync"


class SyncScheduler:

    def __init__(
            self,
            settings: Settings,
            job: Callable[[], Awaitable[None]],
    ) -> None:
        self._settings = settings
        self._job = job
        self._scheduler: Optional[AsyncIOScheduler] = None
        self._stop_event = asyncio.Event()
        self._is_shutting_down = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> AsyncIOScheduler:
        scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
        scheduler.add_job(
            self._guarded_job,
            trigger=IntervalTrigger(minutes=self._settings.sync_interval_minutes),
            id=JOB_ID,
            name="Marketplace stock synchronisation",
            max_instances=1,  # never overlap two cycles
            coalesce=True,  # collapse missed runs into one
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
        await self._stop_event.wait()

    def shutdown(self, wait: bool = False) -> None:
        if self._is_shutting_down:
            logger.warning("Shutdown already in progress")
            return

        self._is_shutting_down = True

        if self._scheduler is not None and self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("Scheduler stopped")
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _guarded_job(self) -> None:
        try:
            await self._job()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - scheduler must survive normal job errors
            # Already logged in run_sync_cycle, just note here
            logger.debug("Sync cycle failed: %s (see above for details)", type(exc).__name__)

    @staticmethod
    def _on_job_problem(event: JobEvent) -> None:
        if getattr(event, "exception", None) is not None:
            logger.error("APScheduler job '%s' raised: %s", event.job_id, event.exception)
        else:
            logger.warning("APScheduler job '%s' was missed", event.job_id)