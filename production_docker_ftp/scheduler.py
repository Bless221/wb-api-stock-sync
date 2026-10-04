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

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._scheduler: AsyncIOScheduler = AsyncIOScheduler(timezone="UTC")
        self._stop_event = asyncio.Event()
        self._is_shutting_down = False
        self._job_func: Optional[Callable[[], Awaitable[None]]] = None

    def add_sync_job(self, func: Callable[[], Awaitable[None]]) -> None:
        self._job_func = func

    def start(self) -> AsyncIOScheduler:
        if not self._job_func:
            raise ValueError("Sync job function must be configured first")

        self._scheduler.add_job(
            self._guarded_job,
            trigger=IntervalTrigger(minutes=self._settings.sync_interval_minutes),
            id=JOB_ID,
            name="Marketplace stock synchronisation",
            max_instances=1,
            coalesce=True,
            misfire_grace_time=300,
            replace_existing=True,
        )
        self._scheduler.add_listener(self._on_job_problem, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        self._scheduler.start()
        logger.info("Scheduler successfully started: running every %d minute(s)", self._settings.sync_interval_minutes)
        return self._scheduler

    async def run_forever(self) -> None:
        await self._stop_event.wait()

    def shutdown(self, wait: bool = False) -> None:
        if self._is_shutting_down:
            return
        self._is_shutting_down = True
        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("Scheduler background engine stopped")
        self._stop_event.set()

    async def _guarded_job(self) -> None:
        if not self._job_func:
            return
        try:
            res = self._job_func()
            if asyncio.iscoroutine(res):
                await res
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Sync cycle execution failed inside scheduler wrapper: %s", exc)

    @staticmethod
    def _on_job_problem(event: JobEvent) -> None:
        if getattr(event, "exception", None) is not None:
            logger.error("APScheduler job '%s' raised an unhandled exception: %s", event.job_id, event.exception)
        else:
            logger.warning("APScheduler job '%s' was missed or skipped", event.job_id)
