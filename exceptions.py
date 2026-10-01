"""Custom exception types for critical failures and graceful shutdown."""

from __future__ import annotations


class SyncError(Exception):
    """Base exception for all synchronisation errors."""


class MappingError(SyncError):
    """Raised when the mapping file is missing, malformed or inconsistent."""


class CriticalAPIError(SyncError):
    """Raised when an API returns a critical, non-recoverable error (401, 403).

 When caught at the scheduler level, triggers immediate shutdown to prevent
 spam and wasted retry cycles.
 """

    def __init__(self, marketplace: str, status_code: int, message: str) -> None:
        self.marketplace = marketplace
        self.status_code = status_code
        self.message = message
        super().__init__(
            f"[{marketplace.upper()}] Critical API error (HTTP {status_code}): {message}"
        )