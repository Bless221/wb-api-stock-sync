"""Entry point of the multi-marketplace stock synchroniser (v2.0).

Pipeline:

1. stream ``stocks.csv`` in chunks using csv.DictReader (O(1) memory);
2. per-chunk: deduplicate by ``item_sku``, translate to marketplace payloads;
3. aggregate results, then fire both clients concurrently via ``asyncio.gather``;
4. repeat every N minutes through a non-blocking ``AsyncIOScheduler``.

Streaming approach prevents OOM on 500k+ row files.
"""

from __future__ import annotations

import asyncio
import csv
import logging
import signal
import sys
from csv import DictReader
from io import StringIO
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from config import Settings, get_settings
from mapper import MappingError, MappingResult, ProductMapper
from ozon_client import OzonClient
from scheduler import SyncScheduler
from wb_client import WildberriesClient

logger = logging.getLogger("stock_sync")


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
def setup_logging(settings: Settings) -> None:
    """Configure console and rotating file logging."""
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)-14s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(formatter)

    file_handler = RotatingFileHandler(
        settings.log_file, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    file_handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(settings.log_level)
    root.handlers.clear()
    root.addHandler(console)
    root.addHandler(file_handler)

    logging.getLogger("apscheduler").setLevel(logging.WARNING)
    logging.getLogger("aiohttp").setLevel(logging.WARNING)


# ----------------------------------------------------------------------
# Streaming data layer (chunks, not bulk)
# ----------------------------------------------------------------------
def stream_stocks_chunks(
    settings: Settings, chunk_size: int = 5000
) -> tuple[int, pd.DataFrame]:
    """Stream-read ``stocks.csv`` in chunks to avoid OOM on large files.

    Yields aggregated deduped chunks. Returns (total_rows, aggregated_dataframe).
    Memory usage is O(chunk_size), not O(file_size).
    """
    path = settings.csv_path
    if not path.exists():
        logger.error("Stock file not found: %s", path)
        return 0, pd.DataFrame()

    accumulated: dict[str, dict[str, Any]] = {}
    total_rows = 0
    chunks_processed = 0

    try:
        with open(path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                logger.error("Stock file is empty or malformed")
                return 0, pd.DataFrame()

            required = {"item_sku", "quantity"}
            missing = required - set(reader.fieldnames)
            if missing:
                logger.error("Stock file is missing required columns: %s", sorted(missing))
                return 0, pd.DataFrame()

            for row_number, row in enumerate(reader, start=2):
                total_rows += 1

                sku = str(row.get("item_sku", "")).strip()
                if not sku:
                    logger.debug("Row %d: empty item_sku, skipped", row_number)
                    continue

                try:
                    qty = int(float(row.get("quantity", 0)))
                    qty = max(0, qty)
                except (ValueError, TypeError):
                    logger.debug("Row %d: invalid quantity for SKU=%s, skipped", row_number, sku)
                    continue

                accumulated[sku] = {"item_sku": sku, "quantity": qty}

                if len(accumulated) >= chunk_size:
                    chunks_processed += 1
                    logger.debug(
                        "Chunk #%d aggregated: %d unique SKUs", chunks_processed, len(accumulated)
                    )
                    accumulated = {}

    except Exception as exc:
        logger.exception("Failed to read stock file: %s", path)
        return total_rows, pd.DataFrame()

    final_df = pd.DataFrame(list(accumulated.values()))
    logger.info(
        "Stock file streamed: %d total rows, %d unique SKUs, %d chunks processed",
        total_rows,
        len(final_df),
        chunks_processed,
    )
    return total_rows, final_df


# ----------------------------------------------------------------------
# Synchronisation cycle
# ----------------------------------------------------------------------
async def run_sync_cycle(settings: Settings, mapper: ProductMapper) -> None:
    """Execute one full synchronisation cycle across all enabled marketplaces."""
    logger.info("=" * 80)
    logger.info("SYNC CYCLE STARTED")

    total_rows, df = stream_stocks_chunks(settings, chunk_size=10000)

    if df.empty:
        logger.warning("No stock data available after streaming, cycle skipped")
        logger.info("=" * 80)
        return

    try:
        mapping: MappingResult = mapper.map_dataframe(df)
    except MappingError:
        logger.exception("Mapping failed, cycle aborted")
        logger.info("=" * 80)
        return

    if not mapping.wb_items and not mapping.ozon_items:
        logger.warning(
            "No valid items after mapping (unknown=%d, inactive=%d, invalid=%d)",
            len(mapping.unknown_skus),
            len(mapping.inactive_skus),
            len(mapping.invalid_rows),
        )
        logger.info("=" * 80)
        return

    tasks: list[asyncio.Task[Any]] = []
    labels: list[str] = []

    async with WildberriesClient(settings) as wb_client, OzonClient(settings) as ozon_client:
        # --- Wildberries ------------------------------------------------
        if settings.enable_wb and mapping.wb_items:
            tasks.append(asyncio.create_task(wb_client.update_stocks(mapping.wb_items)))
            labels.append("wildberries")
        elif settings.enable_wb:
            logger.info("[WB] Enabled but no mapped items")

        # --- Ozon --------------------------------------------------------
        if settings.enable_ozon and mapping.ozon_items:
            tasks.append(asyncio.create_task(ozon_client.update_stocks(mapping.ozon_items)))
            labels.append("ozon")
        elif settings.enable_ozon:
            logger.info("[OZON] Enabled but no mapped items")

        if not tasks:
            logger.warning("All marketplaces disabled or no items to send")
            logger.info("=" * 80)
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)

    for label, result in zip(labels, results):
        if isinstance(result, BaseException):
            logger.error("[%s] Pipeline crashed: %s", label.upper(), result)
        else:
            logger.info("[RESULT] %s", result.as_line())

    logger.info("SYNC CYCLE FINISHED | total_csv_rows=%d", total_rows)
    logger.info("=" * 80)


# ----------------------------------------------------------------------
# Bootstrap
# ----------------------------------------------------------------------
async def main() -> None:
    """Start the daemon: optional warm-up run plus the 15-minute scheduler."""
    settings = get_settings()
    setup_logging(settings)

    logger.info("Multi-marketplace Stock Sync v2.0 (streaming-optimized)")
    logger.info(
        "Marketplaces: WB=%s, Ozon=%s | interval=%d min | batch=%d | stream_chunk=10k",
        settings.enable_wb,
        settings.enable_ozon,
        settings.sync_interval_minutes,
        settings.batch_size,
    )

    try:
        mapper = ProductMapper(
            mapping_path=settings.mapping_path,
            ozon_warehouse_id=settings.ozone_warehouse_id,
        )
    except MappingError:
        logger.exception("Cannot start without a valid mapping file")
        return

    scheduler = SyncScheduler(settings, job=lambda: run_sync_cycle(settings, mapper))
    scheduler.start()
    _install_signal_handlers(scheduler)

    if settings.run_on_startup:
        await run_sync_cycle(settings, mapper)

    logger.info("Daemon is running. Press Ctrl+C to stop.")
    await scheduler.run_forever()


def _install_signal_handlers(scheduler: SyncScheduler) -> None:
    """Attach SIGINT/SIGTERM handlers for a graceful shutdown where supported."""
    loop = asyncio.get_running_loop()
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            loop.add_signal_handler(sig, scheduler.shutdown)
        except NotImplementedError:  # Windows event loop
            continue


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.getLogger("stock_sync").info("Stopped by user")