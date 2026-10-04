from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from typing import Any, Optional, Sequence

import aiohttp

from config import Settings
from mapper import OzonStockItem
from exceptions import CriticalAPIError

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
CRITICAL_STATUSES: frozenset[int] = frozenset({401, 403, 400})


@dataclass
class OzonSyncReport:
    marketplace: str = "ozon"
    total_items: int = 0
    sent_items: int = 0
    rejected_items: int = 0
    failed_items: int = 0
    batches_total: int = 0
    batches_ok: int = 0
    batches_failed: int = 0
    rate_limit_hits: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.batches_failed == 0 and self.rejected_items == 0 and not self.errors

    def as_line(self) -> str:
        status = "OK" if self.success else "PARTIAL/FAIL"
        return (
            f"[OZON] {status}: updated={self.sent_items}/{self.total_items}, "
            f"rejected={self.rejected_items}, batches={self.batches_ok}/{self.batches_total}, "
            f"429_hits={self.rate_limit_hits}, time={self.duration_seconds:.2f}s"
        )


class OzonClient:

    def __init__(self, settings: Settings, session: aiohttp.ClientSession) -> None:
        self._settings = settings
        self._session = session

        self._batch_size = settings.batch_size
        self._base_delay = settings.ozon_request_delay
        self._backoff_base = settings.ozon_backoff_base
        self._backoff_max = settings.ozon_backoff_max
        self._max_retries = settings.ozon_max_retries
        self._max_concurrent_batches = settings.max_concurrent_batches

        self._cooldown_until: float = 0.0
        self._state_lock = asyncio.Lock()
        self._batch_semaphore = asyncio.Semaphore(self._max_concurrent_batches)
        self._is_disabled = False

    async def update_stocks(self, items: Sequence[OzonStockItem]) -> OzonSyncReport:
        started = time.monotonic()
        report = OzonSyncReport(total_items=len(items))

        if not items:
            logger.info("[OZON] Nothing to sync: empty item list")
            report.duration_seconds = time.monotonic() - started
            return report

        batches = list(self._chunk(items, self._batch_size))
        report.batches_total = len(batches)
        logger.info(
            "[OZON] Starting sync: %d items in %d batches (max_concurrent=%d)",
            len(items),
            len(batches),
            self._max_concurrent_batches,
        )

        self._is_disabled = False

        tasks = [
            self._send_batch_guarded(batch, number + 1, report)
            for number, batch in enumerate(batches)
        ]

        await asyncio.gather(*tasks)

        report.duration_seconds = time.monotonic() - started
        logger.info(report.as_line())
        return report

    @staticmethod
    def _chunk(items: Sequence[OzonStockItem], size: int) -> list[Sequence[OzonStockItem]]:
        return [items[index: index + size] for index in range(0, len(items), size)]

    async def _send_batch_guarded(
            self,
            batch: Sequence[OzonStockItem],
            number: int,
            report: OzonSyncReport,
    ) -> None:
        async with self._batch_semaphore:
            if self._is_disabled:
                logger.warning("[OZON] Batch %d cancelled due to critical error in parallel task", number)
                report.batches_failed += 1
                report.failed_items += len(batch)
                return

            try:
                body = await self._send_batch(batch, number, report)
                self._collect_item_results(body, number, report)
                report.batches_ok += 1
            except CriticalAPIError as exc:
                # ИСПРАВЛЕНО: Флаг взводится, но исключение гасится локально для сохранения
                # результатов других батчей и корректного формирования итогового OzonSyncReport
                self._is_disabled = True
                report.batches_failed += 1
                report.failed_items += len(batch)
                report.errors.append(f"batch {number} critical: {exc}")
                logger.error("[OZON] Batch %d failed with critical auth error, pipeline suspended", number)
            except Exception as exc:
                report.batches_failed += 1
                report.failed_items += len(batch)
                report.errors.append(f"batch {number}: {exc}")
                logger.error("[OZON] Batch %d failed: %s", number, exc)

    async def _send_batch(
            self,
            batch: Sequence[OzonStockItem],
            number: int,
            report: OzonSyncReport,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"stocks": [item.to_payload() for item in batch]}
        url = self._settings.ozon_stocks_url

        headers = {
            "Client-Id": self._settings.ozon_client_id.get_secret_value(),
            "Api-Key": self._settings.ozon_api_key.get_secret_value(),
            "Content-Type": "application/json"
        }
        attempt = 0

        while True:
            # ИСПРАВЛЕНО: Закрыта слепая зона Circuit Breaker на входе в цикл
            if self._is_disabled:
                raise Exception("Ozon API client disabled due to fatal error")

            async with self._state_lock:
                now = time.monotonic()
                diff = self._cooldown_until - now
                if diff > 0:
                    await asyncio.sleep(diff)

            # ИСПРАВЛЕНО: Повторная проверка флага аварии после выхода из состояния сна
            if self._is_disabled:
                raise Exception("Ozon API client disabled due to fatal error")

            try:
                async with self._session.post(url, json=payload, headers=headers) as response:
                    status = response.status
                    text = await response.text()

                    if status in CRITICAL_STATUSES:
                        raise CriticalAPIError(
                            marketplace="ozon",
                            status_code=status,
                            message=self._parse_error_body(text),
                        )

                    if status == 200:
                        async with self._state_lock:
                            self._cooldown_until = time.monotonic() + self._base_delay
                        return self._parse_json(text)

                    if status == 429:
                        report.rate_limit_hits += 1

                    if status in RETRYABLE_STATUSES and attempt < self._max_retries:
                        retry_after = self._get_retry_after(response)
                        delay = await self._register_failure(attempt, retry_after)
                        logger.warning(
                            "[OZON] Batch %d got HTTP %d, retry %d/%d in %.1fs",
                            number, status, attempt + 1, self._max_retries, delay
                        )
                        attempt += 1
                        await asyncio.sleep(delay)
                    else:
                        raise Exception(f"HTTP error {status}: {self._parse_error_body(text)}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    raise Exception(f"Network error after max retries: {exc}") from exc
                delay = await self._register_failure(attempt, None)
                logger.warning("[OZON] Network error on batch %d, retry %d/%d in %.1fs: %s", number, attempt + 1, self._max_retries, delay, exc)
                attempt += 1
                await asyncio.sleep(delay)

    async def _register_failure(self, attempt: int, retry_after: Optional[float]) -> float:
        async with self._state_lock:
            if retry_after and retry_after > 0:
                delay = retry_after
            else:
                delay = min(self._backoff_max, self._backoff_base * (2 ** attempt))
                delay += random.uniform(0.0, 0.5 * delay)
            self._cooldown_until = time.monotonic() + delay
            return delay

    def _collect_item_results(self, response_data: dict[str, Any], batch_number: int, report: OzonSyncReport) -> None:
        results = response_data.get("result", [])
        if not results:
            logger.warning("[OZON] Batch %d returned empty result array", batch_number)
            return

        batch_sent = 0
        batch_rejected = 0

        for item in results:
            if not isinstance(item, dict):
                continue

            if item.get("updated", False):
                batch_sent += 1
            else:
                batch_rejected += 1
                errors = item.get("errors")
                err_msg = "Rejected"
                if isinstance(errors, dict):
                    err_msg = errors.get("message", "Unknown error")
                elif isinstance(errors, list) and errors:
                    err_msg = errors[0].get("message", "Unknown error") if isinstance(errors[0], dict) else str(errors[0])

                logger.warning("[OZON] SKU %s rejected: %s", item.get("offer_id"), err_msg)

        report.sent_items += batch_sent
        report.rejected_items += batch_rejected

        if batch_rejected > 0:
            logger.warning("[OZON] Batch %d partial success: updated=%d, rejected=%d", batch_number, batch_sent,
                           batch_rejected)
        else:
            logger.info("[OZON] Batch %d accepted successfully (%d items)", batch_number, batch_sent)

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
    def _parse_json(text: str) -> dict[str, Any]:
        try:
            return json.loads(text)
        except Exception as exc:
            raise Exception(f"Failed to parse JSON response: {exc}") from exc

    @staticmethod
    def _parse_error_body(body: str) -> str:
        try:
            data = json.loads(body)
            if isinstance(data, dict):
                if "message" in data:
                    return str(data["message"])
                elif "error" in data and isinstance(data["error"], dict):
                    return str(data["error"].get("message", body[:200]))
        except Exception:
            pass
        return body[:200]

