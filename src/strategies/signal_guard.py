"""Signal guard: prevents duplicate and conflicting entries within the same 15-min window.

Rules:
  1. One trade per direction per window (cooldown-based)
  2. NO opposite-direction trade in the same window — prevents buying both
     Up and Down on the same market when price oscillates around the threshold
"""

from __future__ import annotations

import logging
import time

from src.strategies.momentum import Direction, Signal

logger = logging.getLogger(__name__)


class SignalGuard:
    """One trade per market window. Blocks both same-direction repeats and reversals."""

    def __init__(self, cooldown_secs: float = 120):
        self._cooldown = cooldown_secs
        self._traded_slugs: set[str] = set()
        self._inflight_slugs: set[str] = set()
        self._last: dict[str, tuple[Direction, float]] = {}
        self.suppressed_count = 0
        self.blocked_reversal_count = 0
        self._block_counts: dict[str, int] = {}
        self._last_log_ts: dict[str, float] = {}
        self._log_interval_sec = 30.0
        self.last_block_reason = ""
        self.last_block_detail = ""

    def _record_block(self, reason: str, detail: str):
        self.last_block_reason = reason
        self.last_block_detail = detail
        count = self._block_counts.get(reason, 0) + 1
        self._block_counts[reason] = count
        now = time.time()
        last_ts = self._last_log_ts.get(reason, 0.0)
        if count <= 3 or now - last_ts >= self._log_interval_sec:
            logger.info(f"Guard blocked [{reason}] x{count}: {detail}")
            self._last_log_ts[reason] = now

    def should_trade(self, signal: Signal) -> bool:
        slug = signal.market.slug
        now = signal.timestamp or time.time()
        if slug in self._traded_slugs:
            self.suppressed_count += 1
            self._record_block("window_already_traded", f"{signal.asset} {signal.direction.value} slug={slug}")
            return False
        if slug in self._inflight_slugs:
            self.suppressed_count += 1
            self._record_block("window_inflight", f"{signal.asset} {signal.direction.value} slug={slug}")
            return False
        key = signal.binance_symbol
        if key in self._last:
            last_dir, last_ts = self._last[key]
            if signal.direction == last_dir and (now - last_ts) < self._cooldown:
                self.suppressed_count += 1
                self._record_block(
                    "cooldown_same_direction",
                    f"{signal.asset} {signal.direction.value} dt={now - last_ts:.1f}s < {self._cooldown:.1f}s",
                )
                return False

        self._inflight_slugs.add(slug)
        return True

    def on_trade_rejected(self, signal: Signal):
        self._inflight_slugs.discard(signal.market.slug)

    def on_trade_submitted(self, signal: Signal):
        slug = signal.market.slug
        now = signal.timestamp or time.time()
        self._inflight_slugs.discard(slug)
        self._traded_slugs.add(slug)

        key = signal.binance_symbol
        self._last[key] = (signal.direction, now)

    def on_window_change(self):
        """Call when a new 15-min window starts to reset all tracking."""
        self._traded_slugs.clear()
        self._inflight_slugs.clear()
        self._last.clear()
