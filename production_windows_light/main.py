from __future__ import annotations

import asyncio
import logging
import signal
import sys
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

import aiohttp

from config import Settings, get_settings
from database import DatabaseError, init_database
from exceptions import (
    CriticalAPIError,
    MappingError,
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


def setup_logging(settings: Settings) -> None:
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


async def run_sync_cycle(
        settings: Settings,
        mapper: ProductMapper,
        notifier: TelegramNotifier,
        wb_client: WildberriesClient,
        ozon_client: OzonClient,
) -> None:
    logger.info("=" * 80)
    logger.info("SYNC CYCLE STARTED")

    file_manager = StockFileManager(settings)

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
        return

    try:
        total_rows, df = await file_manager.read_with_retry(max_retries=3)
    except StockFileUnavailableError as exc:
        logger.error("Stock file unavailable after all retries: %s", exc)
        await notifier.notify_critical_error(
            title="🚨 CRITICAL: Stock File Unavailable",
            message=str(exc),
        )
        logger.info("=" * 80)
        return

    if df.empty:
        logger.warning("No stock data available after streaming, cycle skipped")
        await notifier.notify_sync_warning(
            title="No stock data",
            details="Stock file is empty or contains no valid rows after processing.",
        )
        logger.info("=" * 80)
        return

    try:
        await mapper.load()
    except (MappingError, DatabaseError) as exc:
        logger.error("Failed to load product mapping: %s", exc)
        await notifier.notify_critical_error(
            title="🚨 CRITICAL: Product Mapping Load Failed",
            message=str(exc),
        )
        logger.info("=" * 80)
        return

    try:
        mapping = mapper.map_dataframe(df)
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
    sync_reports: dict[str, Any] = {}

    if settings.enable_wb and mapping.wb_items:
        task = asyncio.create_task(wb_client.update_stocks(mapping.wb_items))
        tasks.append(task)
        labels.append("wildberries")
        logger.info("[WB] Sending %d items in batches", len(mapping.wb_items))

    if settings.enable_ozon and mapping.ozon_items:
        task = asyncio.create_task(ozon_client.update_stocks(mapping.ozon_items))
        tasks.append(task)
        labels.append("ozon")
        logger.info("[OZON] Sending %d items in batches", len(mapping.ozon_items))

    if not tasks:
        logger.warning("All marketplaces disabled or no items to send")
        logger.info("=" * 80)
        return

    results = await asyncio.gather(*tasks, return_exceptions=True)

    critical_errors = []
    success_summaries = []

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
            sync_reports[label] = {"status": "FAILED", "error": str(result)}
        else:
            logger.info("[RESULT] %s", result.as_line())
            sync_reports[label] = {
                "status": "OK" if result.success else "PARTIAL",
                "report": result.as_line(),
                "sent": result.sent_items,
                "total": result.total_items,
                "failed": result.failed_items,
            }
            success_summaries.append(result.as_line())

    if critical_errors:
        for exc in critical_errors:
            await notifier.notify_critical_error(
                title="🚨 CRITICAL Marketplace API Error",
                message=f"[{exc.marketplace.upper()}] HTTP {exc.status_code} - {exc.message}.\nСервис продолжит работу и повторит попытку через {settings.sync_interval_minutes} мин.",
                marketplace=exc.marketplace
            )
        logger.warning("Sync loop finished with critical errors. Application remains active.")

    if success_summaries and not critical_errors:
        await notifier.notify_sync_success(summary="\n".join(success_summaries))

    logger.info("SYNC CYCLE COMPLETED")
    logger.info("=" * 80)


async def main() -> None:
    settings = get_settings()
    setup_logging(settings)

    logger.info("Starting Multi-Marketplace Stock Sync v2.0...")

    try:
        mapping_path = Path("mapping.json")
        await init_database(Path(settings.database_path), mapping_path)
    except DatabaseError as exc:
        logger.critical("Failed to initialize system database: %s", exc)
        sys.exit(1)

    timeout = aiohttp.ClientTimeout(total=settings.request_timeout)
    async with aiohttp.ClientSession(timeout=timeout) as http_session:
        notifier = TelegramNotifier(settings, http_session)
        mapper = ProductMapper(Path(settings.database_path), settings.ozon_warehouse_id)
        wb_client = WildberriesClient(settings, http_session)
        ozon_client = OzonClient(settings, http_session)

        scheduler = SyncScheduler(settings)

        sync_job = lambda: run_sync_cycle(settings, mapper, notifier, wb_client, ozon_client)
        scheduler.add_sync_job(sync_job)

        scheduler.start()
        logger.info("Scheduler running. Press Ctrl+C to exit.")
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        def handle_exit_signal() -> None:
            logger.info("Received exit signal. Shutting down gracefully...")
            stop_event.set()

        if sys.platform != "win32":
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, handle_exit_signal)
        else:
            async def windows_wakeup() -> None:
                while not stop_event.is_set():
                    await asyncio.sleep(0.5)

            asyncio.create_task(windows_wakeup())

        if getattr(settings, "run_on_startup", True):
            logger.info("Executing initial startup synchronization run...")
            try:
                await sync_job()
            except Exception as exc:
                logger.error("Initial startup sync execution failed: %s", exc, exc_info=True)
                logger.warning("Application will remain alive in background. Waiting for next scheduled tick.")

        try:
            await stop_event.wait()
        except (KeyboardInterrupt, SystemExit):
            logger.info("Intercepted exit interrupt sequence.")
        finally:
            logger.info("Cleaning up running services and connection pools...")
            scheduler.shutdown(wait=True)
            await notifier.close()
            logger.info("Application successfully stopped.")


if __name__ == "__main__":
    import traceback
    import os

    try:
        print(f"[DEBUG] Рабочая директория: {os.getcwd()}")
        print(f"[DEBUG] Проверка .env: {os.path.exists('.env')}")
        print(f"[DEBUG] Проверка mapping.json: {os.path.exists('mapping.json')}")

        asyncio.run(main())
    except Exception as e:
        print("\n❌ [КРИТИЧЕСКОЕ ПАДЕНИЕ ПРИ СТАРТЕ]:")
        traceback.print_exc()
    except (KeyboardInterrupt, SystemExit):
        print("\nРабота завершена пользователем.")
