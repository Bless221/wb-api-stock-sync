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
    wb_base_url: str = Field(default="https://marketplace-api.wildberries.ru")

    # ------------------------------------------------------------------
    # Ozon
    # ------------------------------------------------------------------
    ozon_client_id: SecretStr = Field(..., description="Ozon Client-Id header")
    ozon_api_key: SecretStr = Field(..., description="Ozon Api-Key header")
    ozon_base_url: str = Field(default="https://api-seller.ozon.ru")
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
    # File stability checking (защита от недописанных файлов из 1С)
    # ------------------------------------------------------------------
    csv_wait_timeout: int = Field(
        default=10,
        ge=1,
        le=300,
        description="Maximum seconds to wait for CSV file to stabilize",
    )
    csv_stability_window: int = Field(
        default=2,
        ge=1,
        le=10,
        description="Consecutive seconds size must be unchanged to consider file stable",
    )
    csv_check_interval: float = Field(
        default=1.0,
        ge=0.1,
        le=5.0,
        description="Interval in seconds between file size checks",
    )

    # ------------------------------------------------------------------
    # Streaming chunk size (защита от OOM при загрузке больших CSV)
    # ------------------------------------------------------------------
    csv_chunk_size: int = Field(
        default=10000,
        ge=1000,
        le=100000,
        description="Rows per chunk when streaming CSV to prevent OOM",
    )

    # ------------------------------------------------------------------
    # Parallel batch settings
    # ------------------------------------------------------------------
    max_concurrent_batches: int = Field(
        default=3,
        ge=1,
        le=10,
        description="Maximum number of concurrent batch uploads per marketplace",
    )

    # ------------------------------------------------------------------
    # Notifications (Telegram)
    # ------------------------------------------------------------------
    telegram_bot_token: Optional[SecretStr] = Field(
        default=None,
        description="Telegram bot token for notifications",
    )
    telegram_chat_id: Optional[str] = Field(
        default=None,
        description="Telegram chat ID for notifications",
    )

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
        """Normalise base URLs so path concatenation is always predictable."""
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
        """Resolve relative paths against the project root."""
        return value if value.is_absolute() else (BASE_DIR / value)

    @field_validator("wb_api_token", "ozon_api_key", "ozon_client_id")
    @classmethod
    def _reject_empty_secret(cls, value: SecretStr) -> SecretStr:
        if not value.get_secret_value().strip():
            raise ValueError("Credential must not be empty")
        return value

    @field_validator("telegram_bot_token")
    @classmethod
    def _validate_telegram_token(cls, value: Optional[SecretStr]) -> Optional[SecretStr]:
        if value is not None and not value.get_secret_value().strip():
            raise ValueError("Telegram token must not be empty if provided")
        return value

    @model_validator(mode="after")
    def _validate_backoff_bounds(self) -> "Settings":
        if self.wb_backoff_max < self.wb_backoff_base:
            raise ValueError("WB_BACKOFF_MAX must be >= WB_BACKOFF_BASE")
        if self.ozon_backoff_max < self.ozon_backoff_base:
            raise ValueError("OZON_BACKOFF_MAX must be >= OZON_BACKOFF_BASE")
        if not (self.enable_wb or self.enable_ozon):
            raise ValueError("At least one marketplace must be enabled")

        # Telegram validation: both token and chat_id must be provided together
        has_token = self.telegram_bot_token is not None
        has_chat_id = self.telegram_chat_id is not None
        if has_token != has_chat_id:
            raise ValueError(
                "Both TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be provided together"
            )

        return self

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------
    @property
    def wb_stocks_url(self) -> str:
        """Full URL of the WB v3 stocks endpoint for the configured warehouse."""
        return f"{self.wb_base_url}/api/v3/stocks/{self.wb_warehouse_id}"

    @property
    def ozon_stocks_url(self) -> str:
        """Full URL of the Ozon stocks import endpoint."""
        return f"{self.ozon_base_url}/v1/product/import/stocks"

    @property
    def telegram_enabled(self) -> bool:
        """Check if Telegram notifications are configured."""
        return self.telegram_bot_token is not None and self.telegram_chat_id is not None

    def wb_headers(self) -> dict[str, str]:
        """Authorization headers for Wildberries.

        IMPORTANT: Tokens are extracted via .get_secret_value() to avoid
        sending the masked "SecretStr(***)" string instead of the real token.
        """
        return {
            "Authorization": self.wb_api_token.get_secret_value(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def ozon_headers(self) -> dict[str, str]:
        """Authorization headers for Ozon.

        IMPORTANT: Tokens are extracted via .get_secret_value() to avoid
        sending the masked "SecretStr(***)" string instead of the real token.
        """
        return {
            "Client-Id": self.ozon_client_id.get_secret_value(),
            "Api-Key": self.ozon_api_key.get_secret_value(),
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def telegram_token_str(self) -> Optional[str]:
        """Get Telegram token as plain string for API calls."""
        return (
            self.telegram_bot_token.get_secret_value()
            if self.telegram_bot_token
            else None
        )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """Return a cached singleton of validated settings."""
    return Settings()


settings: Settings = get_settings()
