from __future__ import annotations

import asyncio
import logging
from typing import Any, Awaitable, Callable, Optional

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

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def start(self) -> AsyncIOScheduler:
        if not self._job_func:
            raise ValueError("Cannot start scheduler: sync job functions must be added first via add_sync_job()")

        self._scheduler.add_job(
            self._guarded_job,
            trigger=IntervalTrigger(minutes=self._settings.sync_interval_minutes),
            id=JOB_ID,
            name="Marketplace stock synchronisation",
            max_instances=1,          # Строго один экземпляр задачи одновременно
            coalesce=True,            # Сливать пропущенные из-за лагов запуски в один
            misfire_grace_time=300,   # Окно допуска запуска при жестких тормозах CPU/диска
            replace_existing=True,
        )
        self._scheduler.add_listener(self._on_job_problem, EVENT_JOB_ERROR | EVENT_JOB_MISSED)
        self._scheduler.start()

        logger.info(
            "Scheduler successfully started: running every %d minute(s), job id=%s",
            self._settings.sync_interval_minutes,
            JOB_ID,
        )
        return self._scheduler

    async def run_forever(self) -> None:
        await self._stop_event.wait()

    def shutdown(self, wait: bool = False) -> None:
        if self._is_shutting_down:
            logger.warning("Shutdown already in progress")
            return

        self._is_shutting_down = True

        if self._scheduler and self._scheduler.running:
            self._scheduler.shutdown(wait=wait)
            logger.info("Scheduler background engine stopped")
        self._stop_event.set()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _guarded_job(self) -> None:
        if not self._job_func:
            return

        try:
            res = self._job_func()
            if asyncio.iscoroutine(res):
                await res
        except asyncio.CancelledError:
            logger.debug("Sync cycle job was explicitly cancelled via system loop shutdown trigger")
            raise
        except Exception as exc:
            logger.error("Sync cycle execution failed inside scheduler wrapper: %s", exc)

    @staticmethod
    def _on_job_problem(event: JobEvent) -> None:
        if getattr(event, "exception", None) is not None:
            logger.error("APScheduler job '%s' raised an unhandled exception: %s", event.job_id, event.exception)
        else:
            logger.warning("APScheduler job '%s' was missed or skipped due to internal loop overlap", event.job_id)
