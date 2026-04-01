"""Rolling-window momentum detector for Binance tick data.

Maintains a 60-second deque of price ticks per symbol and fires a signal
when net price movement exceeds the threshold (default 0.3%).

Calibration notes (BTC):
  0.15% / 30s  → 15-20 signals/day, mostly noise
  0.3%  / 60s  → 3-8 signals/day, ~62% directional accuracy  ← default
  0.5%  / 60s  → 0-2 signals/day, too infrequent
"""

from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from enum import Enum


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"


@dataclass
class Signal:
    symbol: str
    direction: Direction
    price: float
    momentum_pct: float
    timestamp: float


class MomentumDetector:
    """Per-symbol rolling window momentum detector."""

    def __init__(self, threshold_pct: float = 0.003, window_secs: float = 60):
        self._threshold = threshold_pct
        self._window_secs = window_secs
        self._windows: dict[str, deque[tuple[float, float]]] = {}

    def update(self, symbol: str, timestamp: float, price: float) -> Signal | None:
        if symbol not in self._windows:
            self._windows[symbol] = deque()

        window = self._windows[symbol]
        window.append((timestamp, price))

        cutoff = timestamp - self._window_secs
        while window and window[0][0] < cutoff:
            window.popleft()

        if len(window) < 2:
            return None

        oldest_price = window[0][1]
        pct_move = (price - oldest_price) / oldest_price

        if pct_move >= self._threshold:
            return Signal(
                symbol=symbol,
                direction=Direction.UP,
                price=price,
                momentum_pct=pct_move,
                timestamp=timestamp,
            )
        if pct_move <= -self._threshold:
            return Signal(
                symbol=symbol,
                direction=Direction.DOWN,
                price=price,
                momentum_pct=pct_move,
                timestamp=timestamp,
            )
        return None
