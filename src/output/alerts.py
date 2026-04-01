"""Alert system: DingTalk (钉钉) webhook notifications.

Security: DingTalk custom robot requires every message to contain the
configured keyword. We prepend "PolyGod" to all messages automatically.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)


class DingTalkAlert:
    def __init__(self, webhook_url: str = "", keyword: str = "PolyGod"):
        self._url = webhook_url or os.getenv("DINGTALK_WEBHOOK", "")
        self._keyword = keyword

    @property
    def enabled(self) -> bool:
        return bool(self._url)

    async def send_text(self, content: str):
        if not self._url:
            return
        body = {
            "msgtype": "text",
            "text": {"content": f"{self._keyword} {content}"},
        }
        await self._post(body)

    async def send_markdown(self, title: str, text: str):
        if not self._url:
            return
        if self._keyword not in text:
            text = f"**{self._keyword}**\n\n{text}"
        body = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text},
        }
        await self._post(body)

    async def send_trade(
        self,
        symbol: str,
        direction: str,
        price: float,
        shares: float,
        cost: float,
        momentum: float,
        market_question: str,
        is_paper: bool,
        order_id: str,
    ):
        mode = "PAPER" if is_paper else "LIVE"
        ts = datetime.now().strftime("%H:%M:%S")
        text = (
            f"**{self._keyword} Trade #{order_id}**\n\n"
            f"> **{direction}** {symbol} @ ${price:.3f}\n\n"
            f"- Shares: {shares:.1f}\n"
            f"- Cost: ${cost:.2f}\n"
            f"- Momentum: {momentum:+.2%}\n"
            f"- Mode: **{mode}**\n"
            f"- Time: {ts}\n"
            f"- Market: {market_question[:80]}\n"
        )
        await self.send_markdown(f"{mode} {direction} {symbol}", text)

    async def send_signal(self, symbol: str, direction: str, momentum: float, price: float):
        ts = datetime.now().strftime("%H:%M:%S")
        arrow = "🔺" if direction == "UP" else "🔻"
        await self.send_text(
            f"{arrow} Signal: {direction} {symbol} "
            f"momentum={momentum:+.2%} @ ${price:,.2f} [{ts}]"
        )

    async def send_startup(self, mode: str, symbols: list[str]):
        sym_str = ", ".join(symbols)
        await self.send_markdown(
            "Pipeline Started",
            f"**{self._keyword} Pipeline Started**\n\n"
            f"- Mode: **{mode}**\n"
            f"- Symbols: {sym_str}\n"
            f"- Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n",
        )

    async def _post(self, body: dict):
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self._url,
                    json=body,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    result = await resp.json()
                    if result.get("errcode") != 0:
                        logger.warning(f"DingTalk error: {result}")
        except Exception as e:
            logger.warning(f"DingTalk send failed: {e}")
