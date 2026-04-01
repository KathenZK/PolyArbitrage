"""Binance aggTrade WebSocket stream for real-time tick data.

Uses aggTrade (not klines) for sub-second latency — critical when the
edge window against Polymarket is only 30-90 seconds.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

import websockets

logger = logging.getLogger(__name__)


@dataclass
class Tick:
    symbol: str
    timestamp: float
    price: float
    quantity: float


class BinanceStream:
    """Streams aggTrade ticks for one or more symbols via Binance combined WebSocket."""

    def __init__(
        self,
        symbols: list[str],
        on_tick: Callable[[Tick], Coroutine[Any, Any, None]],
    ):
        self._symbols = [s.lower() for s in symbols]
        self._on_tick = on_tick
        self._running = False
        self._ws: Any = None

    @property
    def url(self) -> str:
        if len(self._symbols) == 1:
            return f"wss://stream.binance.com:9443/ws/{self._symbols[0]}@aggTrade"
        streams = "/".join(f"{s}@aggTrade" for s in self._symbols)
        return f"wss://stream.binance.com:9443/stream?streams={streams}"

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._running

    async def run(self):
        self._running = True
        backoff = 1
        while self._running:
            try:
                async with websockets.connect(
                    self.url, ping_interval=10, ping_timeout=5
                ) as ws:
                    self._ws = ws
                    backoff = 1
                    logger.info(f"Binance WS connected: {self._symbols}")
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            msg = json.loads(raw)
                            data = msg.get("data", msg)
                            tick = Tick(
                                symbol=data["s"].lower(),
                                timestamp=data["T"] / 1000.0,
                                price=float(data["p"]),
                                quantity=float(data["q"]),
                            )
                            await self._on_tick(tick)
                        except (KeyError, ValueError, TypeError) as e:
                            logger.debug(f"Tick parse error: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._ws = None
                if not self._running:
                    break
                logger.warning(f"Binance WS error: {e}, reconnecting in {backoff}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)
        self._ws = None

    def stop(self):
        self._running = False
