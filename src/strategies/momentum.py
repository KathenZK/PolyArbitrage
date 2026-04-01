"""Price-vs-opening comparator for 15-min crypto markets.

Replaces the rolling-window momentum detector. Instead of detecting "price
moved 0.3% in the last 60 seconds" (indirect), we directly compare the
current Binance price to the market's recorded opening price (direct).

This answers the exact question the market asks:
  "Will BTC be higher at the end of this 15-min window?"
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from src.data.market_registry import CryptoMarket, MarketRegistry


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"


@dataclass
class Signal:
    asset: str
    binance_symbol: str
    direction: Direction
    current_price: float
    opening_price: float
    deviation_pct: float
    market: CryptoMarket
    timestamp: float


class PriceComparator:
    """Generates signals by comparing current Binance price to the market opening price."""

    def __init__(
        self,
        registry: MarketRegistry,
        threshold_pct: float = 0.003,
        min_secs_remaining: float = 30,
        min_secs_elapsed: float = 30,
    ):
        self._registry = registry
        self._threshold = threshold_pct
        self._min_remaining = min_secs_remaining
        self._min_elapsed = min_secs_elapsed

    def check(self, binance_symbol: str, price: float, timestamp: float) -> Signal | None:
        market = self._registry.get_market(binance_symbol)
        if not market:
            return None

        if not market.has_opening_price:
            return None

        if market.secs_remaining < self._min_remaining:
            return None

        if market.secs_elapsed < self._min_elapsed:
            return None

        deviation = (price - market.opening_price) / market.opening_price

        if deviation >= self._threshold:
            direction = Direction.UP
        elif deviation <= -self._threshold:
            direction = Direction.DOWN
        else:
            return None

        return Signal(
            asset=market.asset.upper(),
            binance_symbol=binance_symbol,
            direction=direction,
            current_price=price,
            opening_price=market.opening_price,
            deviation_pct=deviation,
            market=market,
            timestamp=timestamp,
        )
