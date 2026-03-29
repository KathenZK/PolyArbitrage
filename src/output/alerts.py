"""Alert system: terminal output and Telegram push notifications."""

from __future__ import annotations

import logging
from typing import Any

import aiohttp
from rich.console import Console

from src.strategies.base import Opportunity

logger = logging.getLogger(__name__)
console = Console()


class AlertSystem:
    def __init__(self, telegram_token: str = "", telegram_chat_id: str = ""):
        self._tg_token = telegram_token
        self._tg_chat_id = telegram_chat_id
        self._sent: set[str] = set()

    async def send(self, opp: Opportunity, min_edge: float = 3.0):
        if opp.edge_pct < min_edge:
            return

        key = f"{opp.strategy}:{opp.event_title}:{opp.action}"
        if key in self._sent:
            return
        self._sent.add(key)

        self._print_terminal(opp)

        if self._tg_token and self._tg_chat_id:
            await self._send_telegram(opp)

    def _print_terminal(self, opp: Opportunity):
        console.print(f"\n[bold green]>>> OPPORTUNITY DETECTED <<<[/bold green]")
        console.print(f"  Strategy:  {opp.strategy}")
        console.print(f"  Event:     {opp.event_title}")
        console.print(f"  Action:    {opp.action.value}")
        console.print(f"  Edge:      {opp.edge_pct:.2f}%")
        if opp.settlement_date:
            console.print(f"  Settles:   {opp.settlement_date[:10]}")
        details = opp.details
        if "yes_sum" in details:
            console.print(f"  YES Sum:   ${details['yes_sum']}")
        if "profit_pct" in details:
            console.print(f"  Profit:    {details['profit_pct']:.2f}%")

    async def _send_telegram(self, opp: Opportunity):
        text = (
            f"*{opp.strategy}*\n"
            f"Event: {opp.event_title}\n"
            f"Action: `{opp.action.value}`\n"
            f"Edge: {opp.edge_pct:.2f}%\n"
        )
        if opp.settlement_date:
            text += f"Settles: {opp.settlement_date[:10]}\n"

        url = f"https://api.telegram.org/bot{self._tg_token}/sendMessage"
        payload = {"chat_id": self._tg_chat_id, "text": text, "parse_mode": "Markdown"}

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=10)):
                    pass
        except Exception as e:
            logger.warning(f"Telegram send failed: {e}")

    def clear_cache(self):
        self._sent.clear()
