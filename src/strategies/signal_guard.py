"""Signal guard: prevents duplicate entries on sustained moves.

During a sustained BTC rally, the momentum detector fires multiple times.
Without the guard, the bot would open 3 positions on what is effectively one
trade. The guard suppresses same-direction signals within a cooldown window
while allowing immediate entry on direction reversals.

Cooldown calibration (5-min markets):
  < 60s  → stacks positions on single move (too aggressive)
  120s   → prevents stacking, allows genuine reversals  ← default
  > 180s → suppresses legitimate second signals in volatile markets
"""

from __future__ import annotations

import time

from src.strategies.momentum import Direction, Signal


class SignalGuard:
    """Per-symbol cooldown-based signal deduplication."""

    def __init__(self, cooldown_secs: float = 120):
        self._cooldown = cooldown_secs
        self._last: dict[str, tuple[Direction, float]] = {}
        self.suppressed_count = 0

    def should_trade(self, signal: Signal) -> bool:
        key = signal.symbol
        now = signal.timestamp or time.time()

        if key in self._last:
            last_dir, last_ts = self._last[key]
            if signal.direction == last_dir and (now - last_ts) < self._cooldown:
                self.suppressed_count += 1
                return False

        self._last[key] = (signal.direction, now)
        return True
