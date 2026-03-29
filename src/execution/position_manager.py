"""Position manager: Kelly Criterion sizing and position tracking."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class Position:
    strategy: str
    event_title: str
    market_id: str
    side: str
    entry_price: float
    size: float
    cost: float
    current_price: float = 0.0
    is_paper: bool = True

    @property
    def unrealized_pnl(self) -> float:
        if self.current_price <= 0:
            return 0.0
        return (self.current_price - self.entry_price) * self.size

    @property
    def unrealized_pnl_pct(self) -> float:
        if self.cost <= 0:
            return 0.0
        return (self.unrealized_pnl / self.cost) * 100


class PositionManager:
    """Tracks open positions and computes Kelly Criterion bet sizes."""

    def __init__(self, total_capital: float = 10_000):
        self.total_capital = total_capital
        self.positions: dict[str, Position] = {}

    def kelly_size(
        self,
        edge: float,
        odds: float,
        max_pct: float = 0.02,
        fraction: float = 0.5,
    ) -> float:
        """
        Half-Kelly bet sizing.

        Args:
            edge: estimated edge (model_prob - market_price)
            odds: decimal odds (1/price - 1)
            max_pct: maximum fraction of capital per trade
            fraction: Kelly fraction (0.5 = half-Kelly)
        """
        if edge <= 0 or odds <= 0:
            return 0.0

        kelly = (edge * (odds + 1) - 1) / odds
        kelly = max(0.0, kelly * fraction)
        kelly = min(kelly, max_pct)

        return round(kelly * self.total_capital, 2)

    def open_position(self, pos: Position):
        key = f"{pos.market_id}:{pos.side}"
        self.positions[key] = pos
        logger.info(
            f"Opened position: {pos.strategy} {pos.side} on '{pos.event_title}' "
            f"@ ${pos.entry_price:.3f} x {pos.size:.1f}"
        )

    def close_position(self, market_id: str, side: str, exit_price: float) -> float:
        key = f"{market_id}:{side}"
        pos = self.positions.pop(key, None)
        if not pos:
            return 0.0

        pos.current_price = exit_price
        pnl = pos.unrealized_pnl
        self.total_capital += pos.cost + pnl

        logger.info(
            f"Closed position: {pos.strategy} PnL=${pnl:+.2f} ({pos.unrealized_pnl_pct:+.1f}%)"
        )
        return pnl

    @property
    def total_exposure(self) -> float:
        return sum(p.cost for p in self.positions.values())

    @property
    def total_unrealized_pnl(self) -> float:
        return sum(p.unrealized_pnl for p in self.positions.values())

    @property
    def available_capital(self) -> float:
        return self.total_capital - self.total_exposure
