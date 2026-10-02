from __future__ import annotations

import asyncio
import logging
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Optional

import aiohttp
import pandas as pd

from config import Settings, get_settings
from database import DatabaseError, init_database
from exceptions import (
    CriticalAPIError,
    CriticalDatabaseError,
    MappingError,
    NotifiableError,
    StockFileError,
    StockFileUnavailableError,
)
from mapper import ProductMapper
from notifications import TelegramNotifier
from ozon_client import OzonClient
from scheduler import SyncScheduler
from stock_file import StockFileManager
from wb_client import WildberriesClient

logger = logging.getLogger("stock_sync")


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------
def setup_logging(settings: Settings) -> None:
    # Ensure log directory exists
    settings.log_file.parent.mkdir(parents=True, exist_ok=True)

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
# Synchronisation cycle
# ----------------------------------------------------------------------
async def run_sync_cycle(
        settings: Settings,
        mapper: ProductMapper,
        scheduler: SyncScheduler,
        notifier: TelegramNotifier,
        http_session: aiohttp.ClientSession,
) -> None:
    logger.info("=" * 80)
    logger.info("SYNC CYCLE STARTED")

    cycle_start = time.monotonic()
    file_manager = StockFileManager(settings)

    # ================================================================
    # Stage 1: Validate and backup stock file
    # ================================================================
    try:
        file_manager.validate_file()
        backup_path = file_manager.create_backup()
        logger.info("Stock file backed up: %s", backup_path)
    except StockFileError as exc:
        logger.error("Stock file validation failed: %s", exc)
        await notifier.notify_critical_error(
            title="🚨 CRITICAL: Stock File Validation Failed",
            message=str(exc),
        )
        logger.info("=" * 80)
        raise

    # ================================================================
    # Stage 2: Read stock file with retry logic
    # ================================================================
    try:
        total_rows, df = await file_manager.read_with_retry(
            max_retries=3,
            retry_delays=[1.0, 3.0, 5.0],
        )
    except StockFileUnavailableError as exc:
        logger.error("Stock file unavailable after all retries: %s", exc)
        await notifier.notify_critical_error(
            title="🚨 CRITICAL: Stock File Unavailable",
            message=str(exc),
        )
        logger.info("=" * 80)
        raise

    if df.empty:
        logger.warning("No stock data available after streaming, cycle skipped")
        await notifier.notify_sync_warning(
            title="No stock data",
            details="Stock file is empty or contains no valid rows after processing.",
        )
        logger.info("=" * 80)
        return

    # ================================================================
    # Stage 3: Load and cache product mapping from database
    # ================================================================
    try:
        await mapper.load()
    except (MappingError, DatabaseError) as exc:
        logger.error("Failed to load product mapping: %s", exc)
        await notifier.notify_critical_error(
            title="🚨 CRITICAL: Product Mapping Load Failed",
            message=str(exc),
        )
        logger.info("=" * 80)
        raise

    # ================================================================
    # Stage 4: Map stock data to marketplace items
    # ================================================================
    try:
        mapping = mapper.map_dataframe(df)
    except MappingError:
        logger.exception("Mapping failed, cycle aborted")
        logger.info("=" * 80)
        raise

    if not mapping.wb_items and not mapping.ozon_items:
        logger.warning(
            "No valid items after mapping (unknown=%d, inactive=%d, invalid=%d)",
            len(mapping.unknown_skus),
            len(mapping.inactive_skus),
            len(mapping.invalid_rows),
        )
        logger.info("=" * 80)
        return

    # ================================================================
    # Stage 5: Send batches to marketplaces in parallel
    # ================================================================
    tasks: list[asyncio.Task[Any]] = []
    labels: list[str] = []
    sync_reports = {}

    try:
        wb_client = WildberriesClient(settings, http_session)
        ozon_client = OzonClient(settings, http_session)

        # --- Wildberries ------------------------------------------------
        if settings.enable_wb and mapping.wb_items:
            task = asyncio.create_task(wb_client.update_stocks(mapping.wb_items))
            tasks.append(task)
            labels.append("wildberries")
            logger.info("[WB] Sending %d items in batches", len(mapping.wb_items))

        # --- Ozon --------------------------------------------------------
        if settings.enable_ozon and mapping.ozon_items:
            task = asyncio.create_task(ozon_client.update_stocks(mapping.ozon_items))
            tasks.append(task)
            labels.append("ozon")
            logger.info("[OZON] Sending %d items in batches", len(mapping.ozon_items))

        if not tasks:
            logger.warning("All marketplaces disabled or no items to send")
            logger.info("=" * 80)
            return

        # Run all sync tasks in parallel
        results = await asyncio.gather(*tasks, return_exceptions=True)

        # ================================================================
        # Stage 6: Process results and handle errors
        # ================================================================
        critical_errors = []

        for label, result in zip(labels, results):
            if isinstance(result, CriticalAPIError):
                logger.critical(
                    "[%s] CRITICAL API ERROR: HTTP %d - %s",
                    label.upper(),
                    result.status_code,
                    result.message,
                )
                critical_errors.append(result)

            elif isinstance(result, BaseException):
                logger.error("[%s] Pipeline crashed: %s", label.upper(), result)
                sync_reports[label] = {
                    "status": "FAILED",
                    "error": str(result),
                }

            else:
                logger.info("[RESULT] %s", result.as_line())
                sync_reports[label] = {
                    "status": "OK" if result.success else "PARTIAL",
                    "report": result.as_line(),
                    "sent": result.sent_items,
                    "total": result.total_items,
                    "failed": result.failed_items,
                }

        # If critical API errors occurred, notify and stop scheduler
        if critical_errors:
            for exc in critical_errors:
                await notifier.notify_critical_error(
                    title=exc.message,
                    message=f"HTTP {exc.status_code} error - scheduler stopping",
                    marketplace=exc.marketplace,
                )
            scheduler.shutdown()
            raise critical_errors[0]

    except CriticalAPIError:
        raise
    except Exception as exc:
        logger.exception("Unexpected error during sync cycle")
        await notifier.notify_critical_error(
            title="🚨 CRITICAL: Unexpected Error",
            message=str(exc),
        )
        raise

    # ================================================================
    # Stage 7: Finalize and report
    # ================================================================
    cycle_duration = time.monotonic() - cycle_start
    logger.info("SYNC CYCLE FINISHED | total_csv_rows=%d | duration=%.2fs",
                total_rows, cycle_duration)

    # Send success notification if configured
    if settings.telegram_enabled and sync_reports:
        summary_lines = [
            f"✅ Sync completed in {cycle_duration:.1f}s",
            f"📊 Processed {total_rows} rows from stock file",
        ]
        for marketplace, report in sync_reports.items():
            if report["status"] == "OK":
                summary_lines.append(
                    f"  • {marketplace.upper()}: {report['sent']}/{report['total']} items"
                )
        summary = "\n".join(summary_lines)
        await notifier.notify_sync_success(summary)

    logger.info("=" * 80)


# ----------------------------------------------------------------------
# Bootstrap
# ----------------------------------------------------------------------
async def main() -> None:
    settings = get_settings()
    setup_logging(settings)

    logger.info("=" * 80)
    logger.info("Multi-marketplace Stock Sync v2.3 (SQLite + Telegram + Backups)")
    logger.info(
        "Marketplaces: WB=%s, Ozon=%s | interval=%d min | "
        "batch=%d | stream_chunk=%d | max_concurrent=%d | db=%s",
        settings.enable_wb,
        settings.enable_ozon,
        settings.sync_interval_minutes,
        settings.batch_size,
        settings.csv_chunk_size,
        settings.max_concurrent_batches,
        settings.database_path,
    )
    if settings.telegram_enabled:
        logger.info("Telegram notifications ENABLED for critical errors")
    else:
        logger.info("Telegram notifications DISABLED")
    logger.info("=" * 80)

    # ================================================================
    # Initialize HTTP session (shared across all clients)
    # ================================================================
    timeout = aiohttp.ClientTimeout(total=settings.request_timeout)
    connector = aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
    http_session = aiohttp.ClientSession(timeout=timeout, connector=connector)

    # ================================================================
    # Initialize Telegram notifier
    # ================================================================
    notifier = TelegramNotifier(settings, http_session)

    try:
        # ================================================================
        # Initialize database
        # ================================================================
        try:
            await init_database(settings.database_path, settings.mapping_path)
        except DatabaseError as exc:
            logger.critical("Cannot start without a valid database: %s", exc)
            await notifier.notify_critical_error(
                title="🚨 CRITICAL: Database Initialization Failed",
                message=str(exc),
            )
            await http_session.close()
            return

        # ================================================================
        # Load product mapper from database
        # ================================================================
        mapper = ProductMapper(
            database_path=settings.database_path,
            ozon_warehouse_id=settings.ozon_warehouse_id,
        )

        # ================================================================
        # Setup scheduler
        # ================================================================
        scheduler = SyncScheduler(
            settings,
            job=lambda: run_sync_cycle(settings, mapper, scheduler, notifier, http_session),
        )
        scheduler.start()
        _install_signal_handlers(scheduler)

        # ================================================================
        # Run startup sync if enabled
        # ================================================================
        if settings.run_on_startup:
            logger.info("Running initial sync on startup...")
            try:
                await run_sync_cycle(settings, mapper, scheduler, notifier, http_session)
            except (CriticalAPIError, NotifiableError) as exc:
                logger.critical("STARTUP SYNC FAILED: %s", exc)
                scheduler.shutdown()
                await http_session.close()
                return
            except Exception as exc:
                logger.exception("STARTUP SYNC FAILED WITH UNEXPECTED ERROR")
                scheduler.shutdown()
                await http_session.close()
                return

        # ================================================================
        # Run scheduler forever
        # ================================================================
        logger.info("Daemon is running. Press Ctrl+C to stop.")
        await scheduler.run_forever()

    finally:
        logger.info("Shutting down...")
        await http_session.close()
        logger.info("Shutdown complete")


def _install_signal_handlers(scheduler: SyncScheduler) -> None:
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
