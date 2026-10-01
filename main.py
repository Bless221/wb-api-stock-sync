"""Entry point of the multi-marketplace stock synchroniser (v2.0).

Pipeline:

1. read ``stocks.csv`` with Pandas and deduplicate by ``item_sku``;
2. translate internal SKUs into WB barcodes and Ozon offer ids;
3. fire both marketplace clients **concurrently** via ``asyncio.gather``;
4. repeat every N minutes through a non-blocking ``AsyncIOScheduler``.

To sell an "economy" single-marketplace tier, flip ``ENABLE_WB`` /
``ENABLE_OZON`` in ``.env`` or simply comment out the corresponding block
inside :func:`run_sync_cycle` — the clients share no state.
"""

from __future__ import annotations

import asyncio
import logging
import signal
import sys
from logging.handlers import RotatingFileHandler
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
# Data layer (Pandas, inherited from v1.x)
# ----------------------------------------------------------------------
def load_stocks(settings: Settings) -> Optional[pd.DataFrame]:
    """Read ``stocks.csv``, clean it and drop duplicated internal SKUs."""
    path = settings.csv_path
    if not path.exists():
        logger.error("Stock file not found: %s", path)
        return None

    try:
        df = pd.read_csv(path, encoding="utf-8")
    except Exception:  # noqa: BLE001 - malformed CSV must not kill the daemon
        logger.exception("Failed to read stock file: %s", path)
        return None

    required = {"item_sku", "quantity"}
    missing = required - set(df.columns)
    if missing:
        logger.error("Stock file is missing required columns: %s", sorted(missing))
        return None

    initial_rows = len(df)
    df = df.dropna(subset=["item_sku", "quantity"])
    df["item_sku"] = df["item_sku"].astype(str).str.strip()
    df = df[df["item_sku"] != ""]
    df = df.drop_duplicates(subset=["item_sku"], keep="last")

    logger.info(
        "Stock file loaded: %d rows -> %d unique SKUs (%s)",
        initial_rows,
        len(df),
        path.name,
    )
    return df.reset_index(drop=True)


# ----------------------------------------------------------------------
# Synchronisation cycle
# ----------------------------------------------------------------------
async def run_sync_cycle(settings: Settings, mapper: ProductMapper) -> None:
    """Execute one full synchronisation cycle across all enabled marketplaces."""
    logger.info("=" * 78)
    logger.info("SYNC CYCLE STARTED")

    df = load_stocks(settings)
    if df is None or df.empty:
        logger.warning("No stock data available, cycle skipped")
        logger.info("=" * 78)
        return

    try:
        mapping: MappingResult = mapper.map_dataframe(df)
    except MappingError:
        logger.exception("Mapping failed, cycle aborted")
        logger.info("=" * 78)
        return

    tasks: list[asyncio.Task[Any]] = []
    labels: list[str] = []

    async with WildberriesClient(settings) as wb_client, OzonClient(settings) as ozon_client:
        # --- Wildberries ------------------------------------------------
        # Comment out this block to ship an Ozon-only ("economy") build.
        if settings.enable_wb:
            tasks.append(asyncio.create_task(wb_client.update_stocks(mapping.wb_items)))
            labels.append("wildberries")
        else:
            logger.info("Wildberries pipeline disabled by configuration")

        # --- Ozon --------------------------------------------------------
        # Comment out this block to ship a WB-only ("economy") build.
        if settings.enable_ozon:
            tasks.append(asyncio.create_task(ozon_client.update_stocks(mapping.ozon_items)))
            labels.append("ozon")
        else:
            logger.info("Ozon pipeline disabled by configuration")

        if not tasks:
            logger.warning("All marketplaces are disabled, nothing to do")
            logger.info("=" * 78)
            return

        results = await asyncio.gather(*tasks, return_exceptions=True)

    for label, result in zip(labels, results):
        if isinstance(result, BaseException):
            logger.error("[%s] Pipeline crashed: %s", label.upper(), result)
        else:
            logger.info("[RESULT] %s", result.as_line())

    logger.info("SYNC CYCLE FINISHED")
    logger.info("=" * 78)


# ----------------------------------------------------------------------
# Bootstrap
# ----------------------------------------------------------------------
async def main() -> None:
    """Start the daemon: optional warm-up run plus the 15-minute scheduler."""
    settings = get_settings()
    setup_logging(settings)

    logger.info("Multi-marketplace Stock Sync v2.0")
    logger.info(
        "Marketplaces: WB=%s, Ozon=%s | interval=%d min | batch=%d",
        settings.enable_wb,
        settings.enable_ozon,
        settings.sync_interval_minutes,
        settings.batch_size,
    )

    try:
        mapper = ProductMapper(
            mapping_path=settings.mapping_path,
            ozon_warehouse_id=settings.ozon_warehouse_id,
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