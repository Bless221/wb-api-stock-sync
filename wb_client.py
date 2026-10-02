from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from types import TracebackType
from typing import Any, Optional, Sequence

import aiohttp

from config import Settings
from mapper import WBStockItem

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
CRITICAL_STATUSES: frozenset[int] = frozenset({401, 403, 400})


class CriticalAPIError(Exception):
    def __init__(self, marketplace: str, status_code: int, message: str) -> None:
        self.marketplace = marketplace
        self.status_code = status_code
        self.message = message
        super().__init__(
            f"[{marketplace.upper()}] Critical API error (HTTP {status_code}): {message}"
        )


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
        status = "OK" if self.success else "PARTIAL/FAIL"
        return (
            f"[WB] {status}: sent={self.sent_items}/{self.total_items}, "
            f"batches={self.batches_ok}/{self.batches_total}, "
            f"429_hits={self.rate_limit_hits}, time={self.duration_seconds:.2f}s"
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

    async def update_stocks(self, items: Sequence[WBStockItem]) -> WBSyncReport:
        started = time.monotonic()
        report = WBSyncReport(total_items=len(items))

        if not items:
            logger.info("[WB] Nothing to sync: empty item list")
            report.duration_seconds = time.monotonic() - started
            return report

        batches = list(self._chunk(items, self._batch_size))
        report.batches_total = len(batches)
        logger.info(
            "[WB] Starting sync: %d items in %d batches (max_concurrent=%d)",
            len(items),
            len(batches),
            self._max_concurrent_batches,
        )

        tasks = [
            self._send_batch_guarded(batch, number + 1, report)
            for number, batch in enumerate(batches)
        ]

        await asyncio.gather(*tasks)

        report.duration_seconds = time.monotonic() - started
        logger.info(report.as_line())
        return report

    @staticmethod
    def _chunk(items: Sequence[WBStockItem], size: int) -> list[Sequence[WBStockItem]]:
        return [items[index: index + size] for index in range(0, len(items), size)]

    async def _send_batch_guarded(
            self,
            batch: Sequence[WBStockItem],
            number: int,
            report: WBSyncReport,
    ) -> None:
        async with self._batch_semaphore:
            try:
                await self._send_batch(batch, number, report)
            except CriticalAPIError:
                raise
            except Exception as exc:
                report.batches_failed += 1
                report.failed_items += len(batch)
                report.errors.append(f"batch {number}: {exc}")
                logger.error("[WB] Batch %d failed: %s", number, exc)

    async def _send_batch(
            self,
            batch: Sequence[WBStockItem],
            number: int,
            report: WBSyncReport,
    ) -> None:
        payload: dict[str, Any] = {"stocks": [item.to_payload() for item in batch]}
        url = f"https://wildberries.ru{self._settings.wb_warehouse_id}"
        
        headers = {
            "Authorization": self._settings.wb_api_token,
            "Content-Type": "application/json"
        }
        attempt = 0

        while True:
            await self._await_cooldown()

            try:
                async with self._session.put(url, json=payload, headers=headers) as response:
                    status = response.status
                    body = await response.text()

                    if status in CRITICAL_STATUSES:
                        raise CriticalAPIError(
                            marketplace="wildberries",
                            status_code=status,
                            message=self._parse_error_body(body),
                        )

                    if status in (200, 204):
                        await self._relax_cooldown()
                        report.batches_ok += 1
                        report.sent_items += len(batch)
                        logger.info("[WB] Batch %d accepted (%d items)", number, len(batch))
                        return

                    if status == 429:
                        report.rate_limit_hits += 1

                    if status in RETRYABLE_STATUSES and attempt < self._max_retries:
                        retry_after = self._get_retry_after(response)
                        delay = await self._register_failure(attempt, retry_after)
                        logger.warning(
                            "[WB] Batch %d got HTTP %d, retry %d/%d in %.1fs",
                            number, status, attempt + 1, self._max_retries, delay
                        )
                        attempt += 1
                        await asyncio.sleep(delay)
                    else:
                        raise Exception(f"HTTP error {status}: {self._parse_error_body(body)}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    raise Exception(f"Network error after max retries: {exc}") from exc
                delay = await self._register_failure(attempt, None)
                logger.warning("[WB] Network error on batch %d, retrying in %.1fs: %s", number, delay, exc)
                attempt += 1
                await asyncio.sleep(delay)

    # ------------------------------------------------------------------
    # Атомарное управление защитным щитом лимитов (Rate Limit Shield)
    # ------------------------------------------------------------------
    async def _await_cooldown(self) -> None:
        while True:
            now = time.monotonic()
            async with self._state_lock:
                diff = self._cooldown_until - now
                if diff <= 0:
                    return
            await asyncio.sleep(diff)

    async def _register_failure(self, attempt: int, retry_after: Optional[float]) -> float:
        async with self._state_lock:
            if retry_after and retry_after > 0:
                delay = retry_after
            else:
                delay = min(self._backoff_max, self._backoff_base * (2 ** attempt))
                delay += random.uniform(0, 0.5 * delay)  # Jitter
            
            self._cooldown_until = time.monotonic() + delay
            return delay

    async def _relax_cooldown(self) -> None:
        async with self._state_lock:
            self._cooldown_until = time.monotonic() + self._base_delay

    @staticmethod
    def _get_retry_after(response: aiohttp.ClientResponse) -> Optional[float]:
        header = response.headers.get("Retry-After")
        if header:
            try:
                return float(header)
            except ValueError:
                return None
        return None

    @staticmethod
    def _parse_error_body(body: str) -> str:
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                return data.get("error", {}).get("message", body[:200])
        except Exception:
            pass
        return body[:200]
