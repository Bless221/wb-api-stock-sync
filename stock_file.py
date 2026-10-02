from __future__ import annotations

import asyncio
import csv
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd

from config import Settings
from exceptions import StockFileError, StockFileUnavailableError

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"item_sku", "quantity"}
BACKUP_MAX_COUNT = 10


class StockFileManager:

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._csv_path = settings.csv_path
        self._backup_dir = Path(settings.database_path).parent / "backups"
        self._backup_dir.mkdir(parents=True, exist_ok=True)

    def validate_file(self) -> bool:
        if not self._csv_path.exists():
            raise StockFileError(f"Stock file not found: {self._csv_path}")

        try:
            file_size = os.path.getsize(self._csv_path)
        except OSError as exc:
            raise StockFileError(f"Cannot access stock file: {exc}") from exc

        if file_size == 0:
            raise StockFileError(f"Stock file is empty: {self._csv_path}")

        try:
            with open(self._csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if reader.fieldnames is None:
                    raise StockFileError("Stock file has no header row")

                missing = REQUIRED_COLUMNS - set(reader.fieldnames)
                if missing:
                    raise StockFileError(
                        f"Stock file missing required columns: {sorted(missing)}"
                    )
        except UnicodeDecodeError as exc:
            raise StockFileError(f"Stock file is not UTF-8 encoded: {exc}") from exc
        except Exception as exc:
            raise StockFileError(f"Cannot read stock file header: {exc}") from exc

        logger.info(
            "Stock file validation passed: %s (%d bytes)",
            self._csv_path,
            file_size,
        )
        return True

    def create_backup(self) -> Path:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"stocks_{timestamp}.csv"
        backup_path = self._backup_dir / backup_name

        try:
            shutil.copy2(self._csv_path, backup_path)
            logger.info("Stock file backed up to: %s", backup_path)
        except Exception as exc:
            raise StockFileError(f"Failed to create backup: {exc}") from exc

        self._rotate_backups()
        return backup_path

    def _rotate_backups(self) -> None:
        try:
            backups = sorted(
                self._backup_dir.glob("stocks_*.csv"),
                key=lambda p: p.stat().st_mtime,
                reverse=True,
            )

            if len(backups) > BACKUP_MAX_COUNT:
                for old_backup in backups[BACKUP_MAX_COUNT:]:
                    old_backup.unlink()
                    logger.debug("Deleted old backup: %s", old_backup.name)
        except Exception as exc:
            logger.warning("Failed to rotate backups: %s", exc)

    async def read_with_retry(
            self, max_retries: int = 3, retry_delays: Optional[list[float]] = None
    ) -> tuple[int, pd.DataFrame]:
        if retry_delays is None:
            retry_delays = [1.0, 3.0, 5.0]

        while len(retry_delays) < max_retries:
            retry_delays.append(retry_delays[-1] + 2.0)

        attempt = 0

        while attempt <= max_retries:
            try:
                total_rows, df = await self._async_read_csv()
                logger.info(
                    "Stock file read successfully (attempt %d/%d): %d rows",
                    attempt + 1,
                    max_retries + 1,
                    total_rows,
                )
                return total_rows, df

            except (OSError, IOError, PermissionError) as exc:
                if attempt >= max_retries:
                    logger.error(
                        "Stock file read failed after %d attempts: %s",
                        max_retries + 1,
                        exc,
                    )
                    raise StockFileUnavailableError(
                        f"Cannot read stock file after {max_retries + 1} attempts: {exc}"
                    ) from exc

                delay = retry_delays[attempt]
                logger.warning(
                    "Stock file read failed (attempt %d/%d), retrying in %.1fs: %s",
                    attempt + 1,
                    max_retries + 1,
                    delay,
                    exc,
                )

                await asyncio.sleep(delay)
                attempt += 1

            except StockFileError:
                raise
            except Exception as exc:
                logger.error("Unexpected error reading stock file: %s", exc)
                raise StockFileUnavailableError(f"Unexpected error: {exc}") from exc

    async def _async_read_csv(self) -> tuple[int, pd.DataFrame]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._read_csv_sync)

    def _read_csv_sync(self) -> tuple[int, pd.DataFrame]:
        accumulated: dict[str, dict[str, Any]] = {}
        total_rows = 0

        try:
            with open(self._csv_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)

                if reader.fieldnames is None:
                    raise StockFileError("Stock file has no header row")

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
                        logger.debug(
                            "Row %d: invalid quantity for SKU=%s, skipped", row_number, sku
                        )
                        continue

                    accumulated[sku] = {"item_sku": sku, "quantity": qty}

        except Exception as exc:
            logger.exception("Failed to read stock file: %s", self._csv_path)
            raise

        final_df = pd.DataFrame(list(accumulated.values()))
        logger.info(
            "Stock file processed in background thread: %d total lines parsed, %d unique SKUs loaded",
            total_rows,
            len(final_df),
        )
        return total_rows, final_df
