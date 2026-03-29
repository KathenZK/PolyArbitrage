"""Risk gate: pre-trade checks that must all pass before execution."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from src.strategies.base import Opportunity

logger = logging.getLogger(__name__)


@dataclass
class RiskConfig:
    daily_loss_limit_pct: float = 0.10
    max_position_pct: float = 0.02
    cooldown_sec: int = 60
    min_edge_pct: float = 2.0
    max_open_positions: int = 10


class RiskGate:
    """Validates opportunities against risk limits before execution."""

    def __init__(self, config: RiskConfig, total_capital: float = 10_000):
        self.config = config
        self.total_capital = total_capital
        self._daily_pnl: float = 0.0
        self._last_trade_time: float = 0.0
        self._open_positions: int = 0
        self._today_start: float = 0.0

    def check(self, opp: Opportunity) -> tuple[bool, str]:
        """Returns (pass, reason) tuple."""
        now = time.time()

        if now - self._today_start > 86400:
            self._daily_pnl = 0.0
            self._today_start = now

        loss_limit = self.total_capital * self.config.daily_loss_limit_pct
        if self._daily_pnl < -loss_limit:
            return False, f"Daily loss limit hit (${self._daily_pnl:.2f})"

        if opp.edge_pct < self.config.min_edge_pct:
            return False, f"Edge {opp.edge_pct:.2f}% below minimum {self.config.min_edge_pct}%"

        elapsed = now - self._last_trade_time
        if elapsed < self.config.cooldown_sec:
            remaining = self.config.cooldown_sec - elapsed
            return False, f"Cooldown: {remaining:.0f}s remaining"

        if self._open_positions >= self.config.max_open_positions:
            return False, f"Max open positions ({self.config.max_open_positions}) reached"

        return True, "OK"

    def record_trade(self, cost: float):
        self._last_trade_time = time.time()
        self._open_positions += 1

    def record_pnl(self, pnl: float):
        self._daily_pnl += pnl

    def record_close(self):
        self._open_positions = max(0, self._open_positions - 1)
