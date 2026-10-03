from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any, Optional

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR: Path = Path(__file__).resolve().parent


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=BASE_DIR / ".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # Wildberries
    # ------------------------------------------------------------------
    wb_api_token: SecretStr = Field(..., alias="WB_API_TOKEN")
    wb_warehouse_id: int = Field(..., gt=0, alias="WB_WAREHOUSE_ID")
    wb_base_url: str = Field(default="https://wildberries.ru", alias="WB_BASE_URL")

    # ------------------------------------------------------------------
    # Ozon
    # ------------------------------------------------------------------
    ozon_client_id: SecretStr = Field(..., alias="OZON_CLIENT_ID")
    ozon_api_key: SecretStr = Field(..., alias="OZON_API_KEY")
    ozon_base_url: str = Field(default="https://ozon.ru", alias="OZON_BASE_URL")
    ozon_warehouse_id: Optional[int] = Field(default=None, alias="OZON_WAREHOUSE_ID")

    # ------------------------------------------------------------------
    # Marketplace toggles
    # ------------------------------------------------------------------
    enable_wb: bool = Field(default=True, alias="ENABLE_WB")
    enable_ozon: bool = Field(default=True, alias="ENABLE_OZON")

    # ------------------------------------------------------------------
    # Data sources
    # ------------------------------------------------------------------
    csv_path: Path = Field(default=Path("stocks.csv"), alias="CSV_PATH")
    mapping_path: Path = Field(default=Path("mapping.json"), alias="MAPPING_PATH")
    database_path: Path = Field(default=Path("data/stocks.db"), alias="DATABASE_PATH")

    # ------------------------------------------------------------------
    # Batching and rate limits
    # ------------------------------------------------------------------
    batch_size: int = Field(default=100, ge=1, alias="BATCH_SIZE")

    wb_request_delay: float = Field(default=1.0, alias="WB_REQUEST_DELAY")
    ozon_request_delay: float = Field(default=0.8, alias="OZON_REQUEST_DELAY")

    wb_backoff_base: float = Field(default=2.0, alias="WB_BACKOFF_BASE")
    wb_backoff_max: float = Field(default=120.0, alias="WB_BACKOFF_MAX")
    wb_max_retries: int = Field(default=5, alias="WB_MAX_RETRIES")

    ozon_backoff_base: float = Field(default=2.0, alias="OZON_BACKOFF_BASE")
    ozon_backoff_max: float = Field(default=120.0, alias="OZON_BACKOFF_MAX")
    ozon_max_retries: int = Field(default=5, alias="OZON_MAX_RETRIES")

    # ------------------------------------------------------------------
    # Networking and scheduling
    # ------------------------------------------------------------------
    request_timeout: int = Field(default=30, alias="REQUEST_TIMEOUT")
    sync_interval_minutes: int = Field(default=15, alias="SYNC_INTERVAL_MINUTES")
    run_on_startup: bool = Field(default=True, alias="RUN_ON_STARTUP")

    # ------------------------------------------------------------------
    # File stability checking
    # ------------------------------------------------------------------
    csv_wait_timeout: int = Field(default=10, alias="CSV_WAIT_TIMEOUT")
    csv_stability_window: int = Field(default=2, alias="CSV_STABILITY_WINDOW")
    csv_check_interval: float = Field(default=1.0, alias="CSV_CHECK_INTERVAL")

    # ------------------------------------------------------------------
    # Streaming chunk size
    # ------------------------------------------------------------------
    csv_chunk_size: int = Field(default=10000, alias="CSV_CHUNK_SIZE")

    # ------------------------------------------------------------------
    # Parallel batch settings
    # ------------------------------------------------------------------
    max_concurrent_batches: int = Field(default=3, alias="MAX_CONCURRENT_BATCHES")

    # ------------------------------------------------------------------
    # FTP / SFTP Integration
    # ------------------------------------------------------------------
    enable_ftp_download: bool = Field(default=False, alias="ENABLE_FTP_DOWNLOAD")
    ftp_host: Optional[str] = Field(default=None, alias="FTP_HOST")
    ftp_port: int = Field(default=21, alias="FTP_PORT")
    ftp_user: Optional[str] = Field(default=None, alias="FTP_USER")
    ftp_password: Optional[SecretStr] = Field(default=None, alias="FTP_PASSWORD")
    ftp_remote_path: str = Field(default="stocks.csv", alias="FTP_REMOTE_PATH")

    # ------------------------------------------------------------------
    # Notifications (Telegram)
    # ------------------------------------------------------------------
    telegram_bot_token: Optional[SecretStr] = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    telegram_chat_id: Optional[str] = Field(default=None, alias="TELEGRAM_CHAT_ID")

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    log_level: str = Field(default="INFO", alias="LOG_LEVEL")
    log_file: Path = Field(default=Path("logs/sync.log"), alias="LOG_FILE")

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("wb_base_url", "ozon_base_url", mode="before")
    @classmethod
    def _strip_trailing_slash(cls, value: Any) -> str:
        if not isinstance(value, str):
            return str(value)
        value = value.strip().rstrip("/")
        if not value.startswith(("http://", "https://")):
            raise ValueError("Base URL must start with http:// or https://")
        return value

    @field_validator("log_level")
    @classmethod
    def _validate_log_level(cls, value: str) -> str:
        allowed = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        normalized = value.strip().upper()
        if normalized not in allowed:
            raise ValueError(f"LOG_LEVEL must be one of {sorted(allowed)}")
        return normalized

    @field_validator("csv_path", "mapping_path", "log_file", "database_path", mode="before")
    @classmethod
    def _resolve_path(cls, value: Any) -> Path:
        p = Path(value)
        return p if p.is_absolute() else (BASE_DIR / p)

    @field_validator("ozon_warehouse_id", mode="before")
    @classmethod
    def _empty_string_to_none(cls, value: Any) -> Optional[int]:
        if isinstance(value, str) and not value.strip():
            return None
        if value is None:
            return None
        return int(value)

    @model_validator(mode="after")
    def _validate_backoff_bounds(self) -> "Settings":
        if self.wb_backoff_max < self.wb_backoff_base:
            raise ValueError("WB_BACKOFF_MAX must be >= WB_BACKOFF_BASE")
        if self.ozon_backoff_max < self.ozon_backoff_base:
            raise ValueError("OZON_BACKOFF_MAX must be >= OZON_BACKOFF_BASE")
        if not (self.enable_wb or self.enable_ozon):
            raise ValueError("At least one marketplace must be enabled")

        # Валидация зависимостей FTP параметров
        if self.enable_ftp_download:
            if not self.ftp_host or not self.ftp_user or not self.ftp_password:
                raise ValueError("FTP download is enabled, but HOST, USER or PASSWORD fields are missing")

        return self

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    @property
    def wb_stocks_url(self) -> str:
        return f"{self.wb_base_url}/api/v3/stocks/{self.wb_warehouse_id}"

    @property
    def ozon_stocks_url(self) -> str:
        # актуальный эндпоинт v2 для Ozon Seller API
        return f"{self.ozon_base_url}/v2/products/stocks"


@lru_cache()
def get_settings() -> Settings:
    return Settings()
