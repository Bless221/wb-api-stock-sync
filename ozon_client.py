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
from mapper import OzonStockItem

logger = logging.getLogger(__name__)

RETRYABLE_STATUSES: frozenset[int] = frozenset({408, 425, 429, 500, 502, 503, 504})
CRITICAL_STATUSES: frozenset[int] = frozenset({401, 403})


class CriticalAPIError(Exception):

    def __init__(self, marketplace: str, status_code: int, message: str) -> None:
        self.marketplace = marketplace
        self.status_code = status_code
        self.message = message
        super().__init__(
            f"[{marketplace.upper()}] Critical API error (HTTP {status_code}): {message}"
        )


@dataclass(slots=True)
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

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._session: Optional[aiohttp.ClientSession] = None
        self._owns_session = True

        self._batch_size = settings.batch_size
        self._base_delay = settings.ozon_request_delay
        self._backoff_base = settings.ozon_backoff_base
        self._backoff_max = settings.ozon_backoff_max
        self._max_retries = settings.ozon_max_retries
        self._max_concurrent_batches = settings.max_concurrent_batches

        # Isolated rate-limit shield state (Ozon only)
        self._cooldown_until: float = 0.0
        self._state_lock = asyncio.Lock()

        # Semaphore to limit concurrent batch tasks
        self._batch_semaphore = asyncio.Semaphore(self._max_concurrent_batches)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "OzonClient":
        await self._ensure_session()
        return self

    async def __aexit__(
            self,
            exc_type: Optional[type[BaseException]],
            exc: Optional[BaseException],
            tb: Optional[TracebackType],
    ) -> None:
        await self.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._settings.request_timeout)
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def update_stocks(self, items: Sequence[OzonStockItem]) -> OzonSyncReport:
        started = time.monotonic()
        report = OzonSyncReport(total_items=len(items))

        if not items:
            logger.info("[OZON] Nothing to sync: empty item list")
            report.duration_seconds = time.monotonic() - started
            return report

        await self._ensure_session()
        batches = list(self._chunk(items, self._batch_size))
        report.batches_total = len(batches)
        logger.info(
            "[OZON] Starting sync: %d items in %d batches (max_concurrent=%d)",
            len(items),
            len(batches),
            self._max_concurrent_batches,
        )

        # Create all batch tasks and run them in parallel with asyncio.gather
        tasks = [
            self._send_batch_guarded(batch, number + 1, report)
            for number, batch in enumerate(batches)
        ]

        # Gather runs all tasks concurrently (subject to semaphore limit)
        await asyncio.gather(*tasks, return_exceptions=False)

        report.duration_seconds = time.monotonic() - started
        logger.info(report.as_line())
        return report

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
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
            try:
                body = await self._send_batch(batch, number, report)
                self._collect_item_results(body, number, report)
                report.batches_ok += 1
            except CriticalAPIError:
                # Re-raise critical errors to stop the scheduler
                raise
            except Exception as exc:  # noqa: BLE001 - one batch must not kill the run
                report.batches_failed += 1
                report.failed_items += len(batch)
                message = f"batch {number}: {exc}"
                report.errors.append(message)
                logger.error("[OZON] Batch %d failed: %s", number, exc)

    async def _send_batch(
            self,
            batch: Sequence[OzonStockItem],
            number: int,
            report: OzonSyncReport,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {"stocks": [item.to_payload() for item in batch]}
        url = self._settings.ozon_stocks_url
        headers = self._settings.ozon_headers()  # Tokens properly extracted here
        attempt = 0

        while True:
            await self._await_cooldown()
            session = await self._ensure_session()

            try:
                async with session.post(url, json=payload, headers=headers) as response:
                    status = response.status
                    text = await response.text()

                    # Critical errors: stop immediately without retry
                    if status in CRITICAL_STATUSES:
                        error_summary = self._parse_error_body(text)
                        raise CriticalAPIError(
                            marketplace="ozon",
                            status_code=status,
                            message=error_summary,
                        )

                    if status == 200:
                        await self._relax_cooldown()
                        return self._parse_json(text)

                    if status == 429:
                        report.rate_limit_hits += 1

                    if status in RETRYABLE_STATUSES and attempt < self._max_retries:
                        delay = await self._register_failure(attempt, self._retry_after(response))
                        logger.warning(
                            "[OZON] Batch %d got HTTP %d, retry %d/%d in %.1fs",
                            number,
                            status,
                            attempt + 1,
                            self._max_retries,
                            delay,
                        )
                        attempt += 1
                        await asyncio.sleep(delay)
                        continue

                    error_summary = self._parse_error_body(text)
                    raise RuntimeError(f"HTTP {status}: {error_summary}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    raise RuntimeError(f"network error: {exc}") from exc
                delay = await self._register_failure(attempt, None)
                logger.warning(
                    "[OZON] Batch %d network error (%s), retry %d/%d in %.1fs",
                    number,
                    exc,
                    attempt + 1,
                    self._max_retries,
                    delay,
                )
                attempt += 1
                await asyncio.sleep(delay)

    @staticmethod
    def _collect_item_results(
            body: dict[str, Any],
            number: int,
            report: OzonSyncReport,
    ) -> None:
        results = body.get("result") or []
        if not isinstance(results, list):
            logger.warning("[OZON] Batch %d: unexpected response shape", number)
            return

        for entry in results:
            if not isinstance(entry, dict):
                continue

            if entry.get("updated") is True:
                report.sent_items += 1
                continue

            report.rejected_items += 1
            offer_id = entry.get("offer_id", "<unknown>")
            errors = entry.get("errors") or []

            error_parts = []
            for error in errors:
                if isinstance(error, dict):
                    message = error.get("message") or error.get("code")
                    if message:
                        error_parts.append(str(message))
                elif isinstance(error, str):
                    error_parts.append(error)

            details = "; ".join(error_parts) if error_parts else "rejected without details"
            message = f"offer_id={offer_id}: {details}"
            report.errors.append(message)
            logger.warning("[OZON] Batch %d rejected item: %s", number, message)

    async def _register_failure(self, attempt: int, retry_after: Optional[float]) -> float:
        delay = self._backoff_base * (2 ** attempt)
        delay = min(delay, self._backoff_max)
        if retry_after is not None:
            delay = max(delay, min(retry_after, self._backoff_max))
        delay += random.uniform(0.0, min(1.0, delay * 0.1))  # jitter

        async with self._state_lock:
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
        return delay

    async def _relax_cooldown(self) -> None:
        async with self._state_lock:
            self._cooldown_until = 0.0

    async def _await_cooldown(self) -> None:
        async with self._state_lock:
            remaining = self._cooldown_until - time.monotonic()
        if remaining > 0:
            logger.debug("[OZON] Cooling down for %.1fs", remaining)
            await asyncio.sleep(remaining)

    @staticmethod
    def _retry_after(response: aiohttp.ClientResponse) -> Optional[float]:
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    @staticmethod
    def _parse_json(text: str) -> dict[str, Any]:
        try:
            data = json.loads(text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"invalid JSON response: {exc}") from exc
        return data if isinstance(data, dict) else {"result": data}

    @staticmethod
    def _parse_error_body(text: str) -> str:
        text = text.strip()
        if not text:
            return "<empty response>"

        # Try to parse as JSON and extract meaningful error message
        try:
            data = json.loads(text)
            if isinstance(data, dict):
                # Check for common error fields
                for key in ("message", "error", "errorText", "description"):
                    if key in data:
                        msg = data[key]
                        if isinstance(msg, str):
                            return msg[:500]
                # If no standard field, return stringified dict
                return str(data)[:500]
            return str(data)[:500]
        except (json.JSONDecodeError, ValueError):
            # Plain text response
            return text[:500]