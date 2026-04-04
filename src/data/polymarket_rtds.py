"""Polymarket Real-Time Data Socket client for official crypto price streams."""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

import websockets

logger = logging.getLogger(__name__)

RTDS_ENDPOINT = "wss://ws-live-data.polymarket.com"


@dataclass
class RTDSPriceUpdate:
    topic: str
    symbol: str
    timestamp: float
    value: float


class PolymarketRTDSStream:
    """Streams Polymarket RTDS crypto prices.

    We currently use the Chainlink-backed topic as the official real-time
    source for 15-minute crypto markets. The stream can optionally subscribe to
    the Binance-backed RTDS topic too, but the trading pipeline already uses
    Binance's native aggTrade socket for the faster source.
    """

    CHAINLINK_TOPIC = "crypto_prices_chainlink"
    BINANCE_TOPIC = "crypto_prices"

    def __init__(
        self,
        *,
        chainlink_symbols: list[str],
        on_chainlink_price: Callable[[RTDSPriceUpdate], Coroutine[Any, Any, None]],
        binance_symbols: list[str] | None = None,
        on_binance_price: Callable[[RTDSPriceUpdate], Coroutine[Any, Any, None]] | None = None,
        endpoint: str = RTDS_ENDPOINT,
    ):
        self._chainlink_symbols = sorted({s.lower() for s in chainlink_symbols if s})
        self._binance_symbols = sorted({s.lower() for s in (binance_symbols or []) if s})
        self._on_chainlink_price = on_chainlink_price
        self._on_binance_price = on_binance_price
        self._endpoint = endpoint
        self._running = False
        self._ws: Any = None

    @staticmethod
    def to_chainlink_symbol(binance_symbol: str) -> str:
        symbol = str(binance_symbol or "").lower()
        if symbol.endswith("usdt"):
            return f"{symbol[:-4]}/usd"
        return symbol.replace("usdt", "/usd")

    @classmethod
    def _parse_update(cls, message: Any) -> RTDSPriceUpdate | None:
        if not isinstance(message, dict):
            return None
        topic = str(message.get("topic", "") or "")
        if topic not in {cls.CHAINLINK_TOPIC, cls.BINANCE_TOPIC}:
            return None
        payload = message.get("payload")
        if not isinstance(payload, dict):
            return None

        symbol = str(payload.get("symbol", "") or "").lower()
        if not symbol:
            return None

        timestamp_ms = 0.0
        value = 0.0
        if "value" in payload:
            try:
                timestamp_ms = float(payload["timestamp"])
                value = float(payload["value"])
            except (KeyError, TypeError, ValueError):
                return None
        else:
            data = payload.get("data")
            if not isinstance(data, list) or not data:
                return None
            latest = data[-1]
            if not isinstance(latest, dict):
                return None
            try:
                timestamp_ms = float(latest["timestamp"])
                value = float(latest["value"])
            except (KeyError, TypeError, ValueError):
                return None
        if not symbol or value <= 0:
            return None

        effective_topic = topic
        if symbol.count("/") == 1:
            effective_topic = cls.CHAINLINK_TOPIC

        return RTDSPriceUpdate(
            topic=effective_topic,
            symbol=symbol,
            timestamp=timestamp_ms / 1000.0,
            value=value,
        )

    def _subscription_payload(self) -> dict[str, Any]:
        subscriptions: list[dict[str, Any]] = []
        if self._chainlink_symbols:
            # Empirically, per-symbol Chainlink subscriptions often return only
            # an initial snapshot and then stall. Subscribing to the full topic
            # and filtering locally is much more reliable for continuous updates.
            subscriptions.append(
                {
                    "topic": self.CHAINLINK_TOPIC,
                    "type": "*",
                    "filters": "",
                }
            )
        for symbol in self._binance_symbols:
            subscriptions.append(
                {
                    "topic": self.BINANCE_TOPIC,
                    "type": "update",
                    "filters": symbol,
                }
            )
        return {"action": "subscribe", "subscriptions": subscriptions}

    async def _keepalive(self, ws: Any):
        while self._running and ws is self._ws:
            await asyncio.sleep(5.0)
            try:
                await ws.send("PING")
            except Exception:
                break

    async def run(self):
        if not self._chainlink_symbols and not self._binance_symbols:
            logger.info("RTDS disabled: no subscriptions configured")
            return

        self._running = True
        backoff = 1.0
        while self._running:
            keepalive_task: asyncio.Task | None = None
            try:
                async with websockets.connect(self._endpoint, ping_interval=None) as ws:
                    self._ws = ws
                    backoff = 1.0
                    await ws.send(json.dumps(self._subscription_payload()))
                    keepalive_task = asyncio.create_task(self._keepalive(ws))
                    logger.info(
                        "RTDS connected: chainlink=%s binance=%s",
                        self._chainlink_symbols,
                        self._binance_symbols,
                    )
                    async for raw in ws:
                        if not self._running:
                            break
                        if raw in {"PING", "PONG"}:
                            continue
                        try:
                            message = json.loads(raw)
                        except json.JSONDecodeError:
                            continue
                        update = self._parse_update(message)
                        if update is None:
                            continue
                        if update.topic == self.CHAINLINK_TOPIC:
                            if self._chainlink_symbols and update.symbol not in self._chainlink_symbols:
                                continue
                            await self._on_chainlink_price(update)
                        elif update.topic == self.BINANCE_TOPIC and self._on_binance_price is not None:
                            if self._binance_symbols and update.symbol not in self._binance_symbols:
                                continue
                            await self._on_binance_price(update)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                self._ws = None
                if not self._running:
                    break
                logger.warning(f"RTDS error: {exc}, reconnecting in {backoff:.0f}s")
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)
            finally:
                self._ws = None
                if keepalive_task is not None:
                    keepalive_task.cancel()

    def stop(self):
        self._running = False
