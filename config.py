from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Optional

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
    wb_api_token: SecretStr = Field(..., description="Wildberries JWT token")
    wb_warehouse_id: int = Field(..., gt=0, description="WB seller warehouse id")
    wb_base_url: str = Field(default="https://wildberries.ru")

    # ------------------------------------------------------------------
    # Ozon
    # ------------------------------------------------------------------
    ozon_client_id: SecretStr = Field(..., description="Ozon Client-Id header")
    ozon_api_key: SecretStr = Field(..., description="Ozon Api-Key header")
    ozon_base_url: str = Field(default="https://ozon.ru")
    ozon_warehouse_id: Optional[int] = Field(default=None, gt=0)

    # ------------------------------------------------------------------
    # Marketplace toggles (commercial tiers)
    # ------------------------------------------------------------------
    enable_wb: bool = Field(default=True)
    enable_ozon: bool = Field(default=True)

    # ------------------------------------------------------------------
    # Data sources
    # ------------------------------------------------------------------
    csv_path: Path = Field(default=Path("stocks.csv"))
    mapping_path: Path = Field(default=Path("mapping.json"))
    database_path: Path = Field(default=Path("data/stocks.db"))

    # ------------------------------------------------------------------
    # Batching and rate limits
    # ------------------------------------------------------------------
    batch_size: int = Field(default=100, ge=1, le=1000)

    wb_request_delay: float = Field(default=1.0, ge=0.0, le=60.0)
    ozon_request_delay: float = Field(default=0.8, ge=0.0, le=60.0)

    wb_backoff_base: float = Field(default=2.0, gt=0.0)
    wb_backoff_max: float = Field(default=120.0, gt=0.0)
    wb_max_retries: int = Field(default=5, ge=0, le=15)

    ozon_backoff_base: float = Field(default=2.0, gt=0.0)
    ozon_backoff_max: float = Field(default=120.0, gt=0.0)
    ozon_max_retries: int = Field(default=5, ge=0, le=15)

    # ------------------------------------------------------------------
    # Networking and scheduling
    # ------------------------------------------------------------------
    request_timeout: int = Field(default=30, ge=1, le=300)
    sync_interval_minutes: int = Field(default=15, ge=1, le=1440)
    run_on_startup: bool = Field(default=True)

    # ------------------------------------------------------------------
    # File stability checking
    # ------------------------------------------------------------------
    csv_wait_timeout: int = Field(default=10, ge=1, le=300)
    csv_stability_window: int = Field(default=2, ge=1, le=10)
    csv_check_interval: float = Field(default=1.0, ge=0.1, le=5.0)

    # ------------------------------------------------------------------
    # Streaming chunk size
    # ------------------------------------------------------------------
    csv_chunk_size: int = Field(default=10000, ge=1000, le=100000)

    # ------------------------------------------------------------------
    # Parallel batch settings
    # ------------------------------------------------------------------
    max_concurrent_batches: int = Field(default=3, ge=1, le=10)

    # ------------------------------------------------------------------
    # Notifications (Telegram)
    # ------------------------------------------------------------------
    telegram_bot_token: Optional[SecretStr] = Field(default=None)
    telegram_chat_id: Optional[str] = Field(default=None)

    # ------------------------------------------------------------------
    # Logging
    # ------------------------------------------------------------------
    log_level: str = Field(default="INFO")
    log_file: Path = Field(default=Path("logs/sync.log"))

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------
    @field_validator("wb_base_url", "ozon_base_url")
    @classmethod
    def _strip_trailing_slash(cls, value: str) -> str:
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

    @field_validator("csv_path", "mapping_path", "log_file", "database_path")
    @classmethod
    def _resolve_path(cls, value: Path) -> Path:
        return value if value.is_absolute() else (BASE_DIR / value)

    @field_validator("wb_api_token", "ozon_api_key", "ozon_client_id")
    @classmethod
    def _reject_empty_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("Credential must not be empty")
        return value

    @model_validator(mode="after")
    def _validate_backoff_bounds(self) -> "Settings":
        if self.wb_backoff_max < self.wb_backoff_base:
            raise ValueError("WB_BACKOFF_MAX must be >= WB_BACKOFF_BASE")
        if self.ozon_backoff_max < self.ozon_backoff_base:
            raise ValueError("OZON_BACKOFF_MAX must be >= OZON_BACKOFF_BASE")
        if not (self.enable_wb or self.enable_ozon):
            raise ValueError("At least one marketplace must be enabled")
        if self.telegram_bot_token and not self.telegram_bot_token.get_secret_value().strip():
            self.telegram_bot_token = None
        if self.telegram_chat_id and not self.telegram_chat_id.strip():
            self.telegram_chat_id = None

        has_token = self.telegram_bot_token is not None
        has_chat_id = self.telegram_chat_id is not None
        if has_token != has_chat_id:
            raise ValueError("Both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be provided together")

        return self

    # ------------------------------------------------------------------
    # Convenience helpers (Исправлено формирование путей)
    # ------------------------------------------------------------------
    @property
    def wb_stocks_url(self) -> str:
        return f"{self.wb_base_url}/api/v3/stocks/{self.wb_warehouse_id}"

    @property
    def ozon_stocks_url(self) -> str:
        return f"{self.ozon_base_url}/v1/product/import/stocks"


@lru_cache()
def get_settings() -> Settings:
    """Возвращает кэшированный синглтон настроек приложения."""
    return Settings()
