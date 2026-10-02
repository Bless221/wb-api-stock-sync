from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional

import aiohttp

from config import Settings

logger = logging.getLogger(__name__)


class TelegramNotifier:

    def __init__(self, settings: Settings, session: Optional[aiohttp.ClientSession] = None) -> None:
        self._settings = settings
        self._session = session
        self._owns_session = session is None

        if not settings.telegram_enabled:
            logger.debug("Telegram notifications disabled (no token/chat_id)")
            return

        self._bot_token = settings.telegram_token_str()
        self._chat_id = settings.telegram_chat_id
        self._base_url = "https://api.telegram.org"

    async def __aenter__(self) -> "TelegramNotifier":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Create HTTP session lazily if not injected."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._settings.request_timeout)
            connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        """Close session if owned by this notifier."""
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def notify_critical_error(
            self, title: str, message: str, marketplace: str = ""
    ) -> bool:
        if not self._settings.telegram_enabled:
            logger.debug("Telegram notifications disabled, skipping: %s", title)
            return False

        text = f"{title}\n\n{message}"
        if marketplace:
            text = f"{text}\n\n📊 Marketplace: {marketplace}"

        return await self._send_message(text)

    async def notify_sync_success(self, summary: str) -> bool:
        if not self._settings.telegram_enabled:
            return False

        text = f"✅ Sync completed successfully\n\n{summary}"
        return await self._send_message(text)

    async def notify_sync_warning(self, title: str, details: str) -> bool:
        if not self._settings.telegram_enabled:
            return False

        text = f"⚠️ {title}\n\n{details}"
        return await self._send_message(text)

    async def _send_message(self, text: str) -> bool:
        if not self._bot_token or not self._chat_id:
            logger.warning("Telegram credentials missing, cannot send notification")
            return False

        # Truncate if too long (Telegram limit is 4096)
        if len(text) > 4096:
            text = text[:4090] + "\n..."

        async def _do_send() -> bool:
            """Actual send logic."""
            try:
                session = await self._ensure_session()
                url = f"{self._base_url}/bot{self._bot_token}/sendMessage"

                payload = {
                    "chat_id": self._chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                }

                async with session.post(url, json=payload) as response:
                    if response.status == 200:
                        logger.debug("Telegram notification sent successfully")
                        return True

                    error_text = await response.text()
                    logger.error(
                        "Telegram API error (HTTP %d): %s",
                        response.status,
                        error_text[:200],
                    )
                    return False

            except asyncio.TimeoutError:
                logger.error("Telegram notification timeout (>%d seconds)", self._settings.request_timeout)
                return False
            except aiohttp.ClientError as exc:
                logger.error("Telegram notification network error: %s", exc)
                return False
            except Exception as exc:
                logger.exception("Unexpected error sending Telegram notification: %s", exc)
                return False

        # Use shield to prevent cancellation if main loop is shutting down
        try:
            return await asyncio.shield(_do_send())
        except asyncio.CancelledError:
            logger.warning("Telegram notification cancelled (main loop shutting down)")
            return False

    @staticmethod
    def format_error_message(
            error_type: str,
            error_details: str,
            suggestions: Optional[str] = None,
    ) -> str:
        msg = f"<b>Error Type:</b> {error_type}\n"
        msg += f"<b>Details:</b> <code>{error_details[:500]}</code>"

        if suggestions:
            msg += f"\n\n<b>Suggestions:</b>\n{suggestions}"

        return msg