"""Signal guard: prevents duplicate entries within the same 15-min window.

Keyed by market slug (unique per window per asset), so switching to a new
15-min window automatically resets the guard for that asset.
"""

from __future__ import annotations

import time

from src.strategies.momentum import Direction, Signal


class SignalGuard:
    """One trade per direction per 15-min window, with configurable cooldown."""

    def __init__(self, cooldown_secs: float = 120):
        self._cooldown = cooldown_secs
        self._last: dict[str, tuple[Direction, float]] = {}
        self.suppressed_count = 0

    def should_trade(self, signal: Signal) -> bool:
        key = signal.market.slug
        now = signal.timestamp or time.time()

        if key in self._last:
            last_dir, last_ts = self._last[key]
            if signal.direction == last_dir and (now - last_ts) < self._cooldown:
                self.suppressed_count += 1
                return False

        self._last[key] = (signal.direction, now)
        return True
