from __future__ import annotations

import asyncio
import json
import logging
from typing import Optional, Any

import aiohttp

from config import Settings

logger = logging.getLogger("stock_sync")


class TelegramNotifier:

    def __init__(self, settings: Settings, session: Optional[aiohttp.ClientSession] = None) -> None:
        self._settings = settings
        self._session = session
        self._owns_session = session is None
        # Telegram Bot API
        self._base_url = "https://telegram.org"
        self._has_alerts = settings.telegram_bot_token is not None and settings.telegram_chat_id is not None

        if not self._has_alerts:
            logger.debug("Telegram notifications are disabled (missing bot token or chat id)")
            self._bot_token = None
            self._chat_id = None
            return

        self._bot_token = settings.telegram_bot_token.get_secret_value()
        self._chat_id = settings.telegram_chat_id

    async def __aenter__(self) -> "TelegramNotifier":
        await self._ensure_session()
        return self

    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        await self.close()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self._settings.request_timeout)
            connector = aiohttp.TCPConnector(limit=5, ttl_dns_cache=300)
            self._session = aiohttp.ClientSession(timeout=timeout, connector=connector)
            self._owns_session = True
        return self._session

    async def close(self) -> None:
        if self._owns_session and self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def notify_critical_error(
            self, title: str, message: str, marketplace: str = ""
    ) -> bool:
        if not self._has_alerts:
            return False

        text = f"<b>{title}</b>\n\n{message}"
        if marketplace:
            text = f"{text}\n\n📊 <b>Marketplace:</b> <code>{marketplace.upper()}</code>"

        return await self._send_message(text)

    async def notify_sync_success(self, summary: str) -> bool:
        if not self._has_alerts:
            return False

        text = f"✅ <b>Sync completed successfully</b>\n\n{summary}"
        return await self._send_message(text)

    async def notify_sync_warning(self, title: str, details: str) -> bool:
        if not self._has_alerts:
            return False

        text = f"⚠️ <b>{title}</b>\n\n{details}"
        return await self._send_message(text)

    async def _send_message(self, text: str) -> bool:
        if not self._bot_token or not self._chat_id:
            logger.warning("Telegram credentials missing, cannot send notification")
            return False

        if len(text) > 4096:
            text = text[:4090] + "\n..."

        async def _do_send() -> bool:
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

        try:
            return await asyncio.shield(_do_send())
        except asyncio.CancelledError:
            logger.warning("Telegram notification cancelled during system shutdown process")
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
