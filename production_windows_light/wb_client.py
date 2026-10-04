from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

import aiohttp

from config import Settings
from mapper import WBStockItem
from exceptions import CriticalAPIError

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
CRITICAL_STATUSES: frozenset[int] = frozenset({401, 403, 400})


@dataclass(slots=True)
class WBSyncReport:
    marketplace: str = "wildberries"
    total_items: int = 0
    sent_items: int = 0
    failed_items: int = 0
    batches_total: int = 0
    batches_ok: int = 0
    batches_failed: int = 0
    rate_limit_hits: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.batches_failed == 0 and not self.errors

    def as_line(self) -> str:
        status = "OK" if self.success else "FAILED"
        return (
            f"[WB] {status}: updated={self.sent_items}/{self.total_items}, "
            f"batches={self.batches_ok}/{self.batches_total}, 429_hits={self.rate_limit_hits}, "
            f"time={self.duration_seconds:.2f}s"
        )


class WildberriesClient:

    def __init__(self, settings: Settings, session: aiohttp.ClientSession) -> None:
        self._settings = settings
        self._session = session
        self._batch_size = settings.batch_size
        self._base_delay = settings.wb_request_delay
        self._backoff_base = settings.wb_backoff_base
        self._backoff_max = settings.wb_backoff_max
        self._max_retries = settings.wb_max_retries
        self._max_concurrent_batches = settings.max_concurrent_batches

        self._cooldown_until: float = 0.0
        self._state_lock = asyncio.Lock()
        self._batch_semaphore = asyncio.Semaphore(self._max_concurrent_batches)
        # ИСПРАВЛЕНО: Добавлен флаг экстренной остановки Circuit Breaker для Windows-рантайма
        self._is_disabled = False

    async def update_stocks(self, items: Sequence[WBStockItem]) -> WBSyncReport:
        started = time.monotonic()
        report = WBSyncReport(total_items=len(items))

        if not items:
            logger.info("[WB] Nothing to sync: empty item list")
            report.duration_seconds = time.monotonic() - started
            return report

        batches = [items[i:i + self._batch_size] for i in range(0, len(items), self._batch_size)]
        report.batches_total = len(batches)
        logger.info("[WB] Starting sync: %d items in %d batches", len(items), len(batches))

        # ИСПРАВЛЕНО: Сброс статуса блокировки на старте каждого цикла синхронизации
        self._is_disabled = False

        tasks = [self._send_batch_guarded(batch, idx + 1, report) for idx, batch in enumerate(batches)]
        await asyncio.gather(*tasks)

        report.duration_seconds = time.monotonic() - started
        logger.info(report.as_line())
        return report

    async def _send_batch_guarded(self, batch: Sequence[WBStockItem], number: int, report: WBSyncReport) -> None:
        async with self._batch_semaphore:
            # ИСПРАВЛЕНО: Проверка флага Circuit Breaker на входе в семафор
            if self._is_disabled:
                logger.warning("[WB] Batch %d cancelled due to fatal authentication error in parallel task", number)
                report.batches_failed += 1
                report.failed_items += len(batch)
                return

            try:
                await self._send_batch(batch, number, report)
                report.batches_ok += 1
                report.sent_items += len(batch)
            except CriticalAPIError:
                # ИСПРАВЛЕНО: Взвод глобального флага аварии при получении 401/403/400 ошибки
                self._is_disabled = True
                raise
            except Exception as exc:
                report.batches_failed += 1
                report.failed_items += len(batch)
                report.errors.append(f"batch {number}: {exc}")
                logger.error("[WB] Batch %d failed: %s", number, exc)

    async def _send_batch(self, batch: Sequence[WBStockItem], number: int, report: WBSyncReport) -> None:
        payload = {"stocks": [item.to_payload() for item in batch]}
        url = self._settings.wb_stocks_url
        headers = {
            "Authorization": self._settings.wb_api_token.get_secret_value(),
            "Content-Type": "application/json"
        }
        attempt = 0

        while True:
            # ИСПРАВЛЕНО: Проверка флага Circuit Breaker перед началом расчета задержек
            if self._is_disabled:
                raise Exception("Marketplace integration disabled via parallel task event")

            # ИСПРАВЛЕНО: Расчет diff и засыпание перенесены строго ВНУТРЬ критической секции лока
            async with self._state_lock:
                now = time.monotonic()
                diff = self._cooldown_until - now
                if diff > 0:
                    await asyncio.sleep(diff)

            # ИСПРАВЛЕНО: Повторная проверка флага Circuit Breaker сразу после выхода из сна корутины
            if self._is_disabled:
                raise Exception("Marketplace integration disabled via parallel task event")

            try:
                async with self._session.put(url, json=payload, headers=headers) as response:
                    status = response.status
                    text = await response.text()

                    if status in CRITICAL_STATUSES:
                        raise CriticalAPIError(marketplace="wildberries", status_code=status, message=text[:200])

                    if status == 200:
                        async with self._state_lock:
                            self._cooldown_until = time.monotonic() + self._base_delay
                        logger.info("[WB] Batch %d accepted successfully", number)
                        return

                    if status == 429:
                        report.rate_limit_hits += 1

                    if status in RETRYABLE_STATUSES and attempt < self._max_retries:
                        delay = min(self._backoff_max, self._backoff_base * (2 ** attempt)) + random.uniform(0.0, 0.5)
                        async with self._state_lock:
                            self._cooldown_until = time.monotonic() + delay
                        logger.warning("[WB] Batch %d got HTTP %d, retry %d/%d in %.1fs", number, status, attempt + 1, self._max_retries, delay)
                        attempt += 1
                        await asyncio.sleep(delay)
                    else:
                        raise Exception(f"HTTP error {status}: {text[:200]}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    raise Exception(f"Network error after max retries: {exc}") from exc
                delay = min(self._backoff_max, self._backoff_base * (2 ** attempt)) + random.uniform(0.0, 0.5)
                async with self._state_lock:
                    self._cooldown_until = time.monotonic() + delay
                logger.warning("[WB] Network error on batch %d, retry %d/%d in %.1fs: %s", number, attempt + 1, self._max_retries, delay, exc)
                attempt += 1
                await asyncio.sleep(delay)
