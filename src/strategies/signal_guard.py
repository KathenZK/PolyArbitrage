"""Signal guard: prevents duplicate and conflicting entries within the same 15-min window.

Rules:
  1. One trade per direction per window (cooldown-based)
  2. NO opposite-direction trade in the same window — prevents buying both
     Up and Down on the same market when price oscillates around the threshold
"""

from __future__ import annotations

import time

from src.strategies.momentum import Direction, Signal


class SignalGuard:
    """One trade per market window. Blocks both same-direction repeats and reversals."""

    def __init__(self, cooldown_secs: float = 120):
        self._cooldown = cooldown_secs
        self._traded_slugs: set[str] = set()
        self._last: dict[str, tuple[Direction, float]] = {}
        self.suppressed_count = 0
        self.blocked_reversal_count = 0

    def should_trade(self, signal: Signal) -> bool:
        slug = signal.market.slug
        now = signal.timestamp or time.time()

        if slug in self._traded_slugs:
            self.suppressed_count += 1
            return False

        key = signal.binance_symbol
        if key in self._last:
            last_dir, last_ts = self._last[key]
            if signal.direction == last_dir and (now - last_ts) < self._cooldown:
                self.suppressed_count += 1
                return False

        self._last[key] = (signal.direction, now)
        self._traded_slugs.add(slug)
        return True

    def on_window_change(self):
        """Call when a new 15-min window starts to reset all tracking."""
        self._traded_slugs.clear()
        self._last.clear()
