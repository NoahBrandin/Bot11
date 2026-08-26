"""Minimal Telegram Bot API client: outbound notifications (sendMessage)
plus inbound command polling (getUpdates, long-poll) for /status and /stop.
"""
from __future__ import annotations

import logging
from typing import Optional

import aiohttp

logger = logging.getLogger(__name__)

SEND_MESSAGE_URL = "https://api.telegram.org/bot{token}/sendMessage"
GET_UPDATES_URL = "https://api.telegram.org/bot{token}/getUpdates"

DEFAULT_LONGPOLL_TIMEOUT = 25


class TelegramNotifier:
    def __init__(self, bot_token: str, chat_id: str) -> None:
        self._send_url = SEND_MESSAGE_URL.format(token=bot_token)
        self._get_updates_url = GET_UPDATES_URL.format(token=bot_token)
        self.chat_id = chat_id

    async def send(self, text: str) -> None:
        async with aiohttp.ClientSession() as session:
            async with session.post(self._send_url, json={"chat_id": self.chat_id, "text": text}) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("Telegram send failed (status=%s): %s", resp.status, body)

    async def get_updates(self, offset: Optional[int], timeout: int = DEFAULT_LONGPOLL_TIMEOUT) -> list[dict]:
        """Long-polls for new inbound messages (commands). `offset` should be
        one past the highest update_id already processed, so Telegram
        doesn't redeliver it."""
        params: dict = {"timeout": timeout}
        if offset is not None:
            params["offset"] = offset
        client_timeout = aiohttp.ClientTimeout(total=timeout + 10)
        async with aiohttp.ClientSession(timeout=client_timeout) as session:
            async with session.get(self._get_updates_url, params=params) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    logger.error("Telegram getUpdates failed (status=%s): %s", resp.status, body)
                    return []
                data = await resp.json()
                return data.get("result", [])
