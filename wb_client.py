"""Asynchronous Wildberries Marketplace API v3 client with concurrent batches.

Responsibilities:

* chunk the payload into batches of ``BATCH_SIZE`` (100) items;
* fire multiple batches **concurrently** (not sequentially) via asyncio.gather
  with a semaphore to respect rate limits;
* talk to ``PUT /api/v3/stocks/{warehouseId}`` over ``aiohttp``;
* own an **isolated** exponential backoff state.

Concurrency model: if one batch gets 429, only that task backs off.
Other batches continue. Max concurrent batches is tunable via CONCURRENCY.
"""

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
MAX_CONCURRENT_BATCHES = 3  # Balance between throughput and not overwhelming server


@dataclass(slots=True)
class WBSyncReport:
    """Aggregated outcome of one Wildberries synchronisation run."""

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
        """``True`` when every batch has been accepted by the marketplace."""
        return self.batches_failed == 0 and not self.errors

    def as_line(self) -> str:
        """Compact representation for log output."""
        status = "OK" if self.success else "PARTIAL/FAIL"
        return (
            f"[WB] {status}: sent={self.sent_items}/{self.total_items}, "
            f"batches={self.batches_ok}/{self.batches_total}, "
            f"429_hits={self.rate_limit_hits}, time={self.duration_seconds:.2f}s"
        )


class WildberriesClient:
    """Async client for the Wildberries stocks endpoint (API v3) with concurrent batch dispatch."""

    def __init__(
        self,
        settings: Settings,
        session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._settings = settings
        self._session = session
        self._owns_session = session is None

        self._batch_size = settings.batch_size
        self._base_delay = settings.wb_request_delay
        self._backoff_base = settings.wb_backoff_base
        self._backoff_max = settings.wb_backoff_max
        self._max_retries = settings.wb_max_retries

        # Isolated rate-limit shield state (Wildberries only).
        self._cooldown_until: float = 0.0
        self._state_lock = asyncio.Lock()

        # Semaphore to limit concurrent batch tasks
        self._batch_semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------
    async def __aenter__(self) -> "WildberriesClient":
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
        """Create the ``aiohttp`` session lazily if it was not injected."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._settings.request_timeout)
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close the session if this client owns it."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def update_stocks(self, items: Sequence[WBStockItem]) -> WBSyncReport:
        """Push stock levels to Wildberries in concurrent batches of 100 items.

        Launches up to MAX_CONCURRENT_BATCHES tasks simultaneously,
        respecting the global cooldown window and per-task backoff.
        """
        started = time.monotonic()
        report = WBSyncReport(total_items=len(items))

        if not items:
            logger.info("[WB] Nothing to sync: empty item list")
            report.duration_seconds = time.monotonic() - started
            return report

        await self._ensure_session()
        batches = list(self._chunk(items, self._batch_size))
        report.batches_total = len(batches)
        logger.info("[WB] Starting sync: %d items in %d batches (max_concurrent=%d)",
                    len(items), len(batches), MAX_CONCURRENT_BATCHES)

        tasks = [
            self._send_batch_guarded(batch, number + 1, report)
            for number, batch in enumerate(batches)
        ]

        await asyncio.gather(*tasks, return_exceptions=False)

        report.duration_seconds = time.monotonic() - started
        logger.info(report.as_line())
        return report

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    @staticmethod
    def _chunk(items: Sequence[WBStockItem], size: int) -> list[Sequence[WBStockItem]]:
        """Split a sequence into consecutive chunks of ``size`` elements."""
        return [items[index : index + size] for index in range(0, len(items), size)]

    async def _send_batch_guarded(
        self,
        batch: Sequence[WBStockItem],
        number: int,
        report: WBSyncReport,
    ) -> None:
        """Acquire semaphore, then send a batch with isolated exponential backoff."""
        async with self._batch_semaphore:
            try:
                await self._send_batch(batch, number, report)
            except Exception as exc:  # noqa: BLE001 - one batch must not kill the run
                report.batches_failed += 1
                report.failed_items += len(batch)
                message = f"batch {number}: {exc}"
                report.errors.append(message)
                logger.error("[WB] Batch %d failed: %s", number, exc)

    async def _send_batch(
        self,
        batch: Sequence[WBStockItem],
        number: int,
        report: WBSyncReport,
    ) -> None:
        """Send a single batch with isolated exponential backoff retry loop."""
        payload: dict[str, Any] = {"stocks": [item.to_payload() for item in batch]}
        url = self._settings.wb_stocks_url
        headers = self._settings.wb_headers()
        attempt = 0

        while True:
            await self._await_cooldown()
            session = await self._ensure_session()

            try:
                async with session.put(url, json=payload, headers=headers) as response:
                    status = response.status
                    body = await response.text()

                    if status in (200, 204):
                        await self._relax_cooldown()
                        report.batches_ok += 1
                        report.sent_items += len(batch)
                        logger.info("[WB] Batch %d accepted (%d items)", number, len(batch))
                        return

                    if status == 429:
                        report.rate_limit_hits += 1

                    if status in RETRYABLE_STATUSES and attempt < self._max_retries:
                        delay = await self._register_failure(attempt, self._retry_after(response))
                        logger.warning(
                            "[WB] Batch %d got HTTP %d, retry %d/%d in %.1fs",
                            number,
                            status,
                            attempt + 1,
                            self._max_retries,
                            delay,
                        )
                        attempt += 1
                        await asyncio.sleep(delay)
                        continue

                    error_summary = self._parse_error_body(body)
                    raise RuntimeError(f"HTTP {status}: {error_summary}")

            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                if attempt >= self._max_retries:
                    raise RuntimeError(f"network error: {exc}") from exc
                delay = await self._register_failure(attempt, None)
                logger.warning(
                    "[WB] Batch %d network error (%s), retry %d/%d in %.1fs",
                    number,
                    exc,
                    attempt + 1,
                    self._max_retries,
                    delay,
                )
                attempt += 1
                await asyncio.sleep(delay)

    async def _register_failure(self, attempt: int, retry_after: Optional[float]) -> float:
        """Compute the next backoff delay and arm the WB-only cooldown window."""
        delay = self._backoff_base * (2 ** attempt)
        delay = min(delay, self._backoff_max)
        if retry_after is not None:
            delay = max(delay, min(retry_after, self._backoff_max))
        delay += random.uniform(0.0, min(1.0, delay * 0.1))  # jitter

        async with self._state_lock:
            self._cooldown_until = max(self._cooldown_until, time.monotonic() + delay)
        return delay

    async def _relax_cooldown(self) -> None:
        """Drop the cooldown window after a successful call."""
        async with self._state_lock:
            self._cooldown_until = 0.0

    async def _await_cooldown(self) -> None:
        """Sleep until the client-local cooldown window expires."""
        async with self._state_lock:
            remaining = self._cooldown_until - time.monotonic()
        if remaining > 0:
            logger.debug("[WB] Cooling down for %.1fs", remaining)
            await asyncio.sleep(remaining)

    @staticmethod
    def _retry_after(response: aiohttp.ClientResponse) -> Optional[float]:
        """Parse the ``Retry-After`` header when the server provides one."""
        raw = response.headers.get("Retry-After")
        if not raw:
            return None
        try:
            return float(raw)
        except ValueError:
            return None

    @staticmethod
    def _parse_error_body(text: str) -> str:
        """Parse and clean error response body (JSON or plain text)."""
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
                # If no standard field, return stringified dict (first 500 chars)
                return str(data)[:500]
            return str(data)[:500]
        except (json.JSONDecodeError, ValueError):
            # Plain text response
            return text[:500]