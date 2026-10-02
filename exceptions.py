from __future__ import annotations


class SyncError(Exception):
    pass


class MappingError(SyncError):
    pass


class DatabaseError(SyncError):
    pass


class StockFileError(SyncError):
    pass


class CriticalAPIError(SyncError):
    def __init__(self, marketplace: str, status_code: int, message: str) -> None:
        self.marketplace = marketplace
        self.status_code = status_code
        self.message = message
        super().__init__(
            f"[{marketplace.upper()}] Critical API error (HTTP {status_code}): {message}"
        )


class NotifiableError(SyncError):
    def __init__(self, message: str, alert_title: str = "⚠️ Sync Error") -> None:
        self.message = message
        self.alert_title = alert_title
        super().__init__(message)


class CriticalDatabaseError(NotifiableError):
    def __init__(self, message: str) -> None:
        super().__init__(message, alert_title="🚨 CRITICAL: Database Error")


class StockFileUnavailableError(NotifiableError):
    def __init__(self, message: str) -> None:
        super().__init__(message, alert_title="🚨 CRITICAL: Stock File Unavailable")


class MaxRetriesExceededError(NotifiableError):
    def __init__(self, marketplace: str, batch_number: int) -> None:
        msg = f"Batch {batch_number} on {marketplace.upper()} exhausted max retries"
        super().__init__(msg, alert_title="🚨 Max Retries Exceeded")
        self.marketplace = marketplace
        self.batch_number = batch_number
