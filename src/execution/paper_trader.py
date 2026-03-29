"""Paper trading engine: simulates execution and tracks P&L without real money."""

from __future__ import annotations

import logging
import time

from src.output.db import get_connection, insert_opportunity, insert_trade
from src.strategies.base import Opportunity

logger = logging.getLogger(__name__)


class PaperTrader:
    """Simulates trade execution, logs to SQLite, tracks performance."""

    def __init__(self, initial_capital: float = 10_000):
        self.capital = initial_capital
        self.initial_capital = initial_capital
        self._trade_count = 0

    def execute(self, opp: Opportunity, position_size_pct: float = 0.02) -> dict | None:
        """Simulate executing an opportunity. Returns trade record or None."""
        max_spend = self.capital * position_size_pct
        if max_spend < 1.0:
            logger.info(f"Insufficient capital for trade (${self.capital:.2f})")
            return None

        conn = get_connection()

        insert_opportunity(
            conn,
            strategy=opp.strategy,
            event_title=opp.event_title,
            action=opp.action.value,
            edge_pct=opp.edge_pct,
            details=opp.details,
            market_ids=opp.market_ids,
            settlement_date=opp.settlement_date,
        )

        details = opp.details
        price = details.get("market_yes", details.get("cheapest_yes", details.get("cost", 0.5)))
        if not price or price <= 0:
            price = 0.5

        size = max_spend / price
        cost = size * price

        insert_trade(
            conn,
            strategy=opp.strategy,
            event_title=opp.event_title,
            action=opp.action.value,
            side="BUY",
            market_id=opp.market_ids[0] if opp.market_ids else "",
            token_id="",
            price=price,
            size=size,
            cost_usd=cost,
            is_paper=True,
        )

        self.capital -= cost
        self._trade_count += 1

        conn.close()

        trade_record = {
            "trade_id": self._trade_count,
            "strategy": opp.strategy,
            "event": opp.event_title,
            "action": opp.action.value,
            "price": price,
            "size": size,
            "cost": cost,
            "remaining_capital": self.capital,
            "timestamp": time.time(),
        }

        logger.info(
            f"[PAPER] {opp.strategy}: {opp.action.value} on '{opp.event_title}' "
            f"@ ${price:.3f} x {size:.1f} = ${cost:.2f} (edge: {opp.edge_pct:.2f}%)"
        )

        return trade_record

    @property
    def pnl(self) -> float:
        return self.capital - self.initial_capital

    @property
    def pnl_pct(self) -> float:
        return (self.pnl / self.initial_capital) * 100 if self.initial_capital > 0 else 0
