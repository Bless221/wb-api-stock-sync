from __future__ import annotations

import asyncio
import csv
import logging
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd

from config import Settings
from exceptions import StockFileError, StockFileUnavailableError

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {"item_sku", "quantity"}
BACKUP_MAX_COUNT = 10


class StockFileManager:

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._csv_path = Path(settings.csv_path)
        self._backup_dir = Path("/app/backups")
        self._backup_dir.mkdir(parents=True, exist_ok=True)

    async def download_from_ftp_if_enabled(self) -> None:
        if not self._settings.enable_ftp_download:
            return

        logger.info("[FTP/Storage] Начинаю импорт свежего файла %s из общего Docker-тома...",
                    self._settings.ftp_remote_path)
        loop = asyncio.get_running_loop()

        try:
            await loop.run_in_executor(None, self._download_ftp_sync)
        except Exception as exc:
            logger.error("[FTP/Storage] Ошибка импорта файла остатков: %s", exc)
            raise StockFileError(f"FTP shared import failed: {exc}") from exc

    def _download_ftp_sync(self) -> None:
        ftp_shared_file = Path("/app/ftp_data") / self._settings.ftp_remote_path

        if not ftp_shared_file.exists():
            logger.warning(
                "[FTP/Storage] Свежий файл от 1С еще не загружен на FTP-сервер. Использую текущий локальный.")
            return

        self._csv_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self._csv_path.with_suffix(".tmp")

        try:
            shutil.copy2(ftp_shared_file, tmp_path)

            if tmp_path.exists():
                if self._csv_path.exists():
                    self._csv_path.unlink()
                tmp_path.rename(self._csv_path)
                logger.info("[FTP/Storage] Свежий stocks.csv успешно импортирован из папки FTP-сервера.")
                # ИСПРАВЛЕНО: Вызов ftp_shared_file.unlink() полностью удален.
                # Исходный файл выгрузки от 1С теперь не удаляется с FTP-сервера.

        except Exception as exc:
            logger.error("[FTP/Storage] Критический сбой атомарного импорта файла: %s", exc)
            raise OSError(f"Shared FTP file import failed: {exc}") from exc
        finally:
            if tmp_path.exists():
                tmp_path.unlink()

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
                    raise StockFileError(f"Stock file missing required columns: {sorted(missing)}")
        except UnicodeDecodeError as exc:
            raise StockFileError(f"Stock file is not UTF-8 encoded: {exc}") from exc
        except Exception as exc:
            raise StockFileError(f"Cannot read stock file header: {exc}") from exc

        logger.info("Stock file validation passed: %s (%d bytes)", self._csv_path, file_size)
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
            retry_delays = [
                self._settings.csv_check_interval,
                self._settings.csv_stability_window,
                float(self._settings.csv_wait_timeout)
            ]

        while len(retry_delays) < max_retries:
            retry_delays.append(retry_delays[-1] + 2.0)

        attempt = 0
        while attempt <= max_retries:
            try:
                if await self._verify_file_stability():
                    total_rows, df = await self._async_read_csv()
                    logger.info("Stock file read successfully (attempt %d/%d): %d rows", attempt + 1, max_retries + 1,
                                total_rows)
                    return total_rows, df
                else:
                    raise OSError("File size is unstable (currently being modified by an external process)")
            except (OSError, IOError, PermissionError) as exc:
                if attempt >= max_retries:
                    logger.error("Stock file read failed after %d attempts: %s", max_retries + 1, exc)
                    raise StockFileUnavailableError(
                        f"Cannot read stock file after {max_retries + 1} attempts: {exc}") from exc

                delay = retry_delays[attempt]
                logger.warning("Stock file read failed or file is unstable (attempt %d/%d), retrying in %.1fs: %s",
                               attempt + 1, max_retries + 1, delay, exc)
                await asyncio.sleep(delay)
                attempt += 1
            except StockFileError:
                raise
            except Exception as exc:
                logger.error("Unexpected error reading stock file: %s", exc)
                raise StockFileUnavailableError(f"Unexpected error: {exc}") from exc

    async def _verify_file_stability(self) -> bool:
        try:
            size_init = os.path.getsize(self._csv_path)
            await asyncio.sleep(self._settings.csv_stability_window)
            size_final = os.path.getsize(self._csv_path)
            return size_init == size_final
        except OSError:
            return False

    async def _async_read_csv(self) -> tuple[int, pd.DataFrame]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, self._read_csv_sync)

    def _read_csv_sync(self) -> tuple[int, pd.DataFrame]:
        total_rows = 0
        chunk_list = []
        try:
            with pd.read_csv(
                    self._csv_path,
                    usecols=list(REQUIRED_COLUMNS),
                    chunksize=self._settings.csv_chunk_size,
                    encoding="utf-8"
            ) as reader:
                for chunk in reader:
                    total_rows += len(chunk)
                    chunk = chunk.dropna(subset=["item_sku"])
                    chunk["item_sku"] = chunk["item_sku"].astype(str).str.strip()
                    chunk = chunk[chunk["item_sku"] != ""]
                    chunk_list.append(chunk)

            if not chunk_list:
                return 0, pd.DataFrame(columns=["item_sku", "quantity"])

            final_df = pd.concat(chunk_list, ignore_index=True)
            final_df = final_df.drop_duplicates(subset=["item_sku"], keep="last")
        except Exception as exc:
            logger.exception("Failed to read stock file via pandas streaming: %s", self._csv_path)
            raise StockFileError(f"Pandas streaming read failed: {exc}") from exc

        logger.info("Stock file processed in background thread: %d total lines parsed, %d unique SKUs loaded",
                    total_rows, len(final_df))
        return total_rows, final_df
