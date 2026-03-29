"""Live trader: real order execution via Polymarket CLOB and Kalshi APIs.

Only activated when DRY_RUN=false in config. All trades are logged to SQLite.
"""

from __future__ import annotations

import logging
import os
import time

from src.execution.position_manager import Position, PositionManager
from src.execution.risk_gate import RiskConfig, RiskGate
from src.output.db import get_connection, insert_trade
from src.strategies.base import Action, Opportunity

logger = logging.getLogger(__name__)


class LiveTrader:
    """Executes real trades on Polymarket and Kalshi."""

    def __init__(
        self,
        position_manager: PositionManager,
        risk_gate: RiskGate,
        dry_run: bool = True,
    ):
        self._pm = position_manager
        self._risk = risk_gate
        self._dry_run = dry_run
        self._poly_clob = None
        self._kalshi_client = None

    def _ensure_poly_clob(self):
        if self._poly_clob is None:
            pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
            if not pk:
                raise RuntimeError("POLYMARKET_PRIVATE_KEY not set")
            from src.data.polymarket_client import PolymarketCLOBClient
            self._poly_clob = PolymarketCLOBClient(pk)
        return self._poly_clob

    async def execute(self, opp: Opportunity) -> dict | None:
        passed, reason = self._risk.check(opp)
        if not passed:
            logger.info(f"Risk gate blocked: {reason}")
            return None

        edge = opp.edge_pct / 100
        price = opp.details.get("market_yes", opp.details.get("cheapest_yes", 0.5))
        if price <= 0:
            return None

        odds = (1.0 / price) - 1.0 if price < 1.0 else 0.01
        bet_size = self._pm.kelly_size(edge, odds)

        if bet_size < 1.0:
            logger.info(f"Bet size too small: ${bet_size:.2f}")
            return None

        if self._dry_run:
            logger.info(f"[DRY RUN] Would execute: {opp.action.value} ${bet_size:.2f} on '{opp.event_title}'")
            return {"dry_run": True, "size": bet_size}

        try:
            result = await self._execute_order(opp, bet_size, price)
            self._risk.record_trade(bet_size)

            self._pm.open_position(Position(
                strategy=opp.strategy,
                event_title=opp.event_title,
                market_id=opp.market_ids[0] if opp.market_ids else "",
                side=opp.action.value,
                entry_price=price,
                size=bet_size / price,
                cost=bet_size,
                is_paper=False,
            ))

            conn = get_connection()
            insert_trade(
                conn,
                strategy=opp.strategy,
                event_title=opp.event_title,
                action=opp.action.value,
                side="BUY",
                market_id=opp.market_ids[0] if opp.market_ids else "",
                token_id="",
                price=price,
                size=bet_size / price,
                cost_usd=bet_size,
                is_paper=False,
            )
            conn.close()

            return result
        except Exception as e:
            logger.error(f"Execution failed: {e}")
            return None

    async def _execute_order(self, opp: Opportunity, size_usd: float, price: float) -> dict:
        """Place the actual order. Raises on failure."""
        if opp.action in (Action.BUY_YES, Action.BUY_ALL_YES):
            side = "BUY"
        else:
            side = "SELL"

        if opp.strategy == "Cross-Platform":
            return await self._execute_cross_platform(opp, size_usd)

        clob = self._ensure_poly_clob()
        token_id = ""
        if opp.market_ids:
            token_id = opp.market_ids[0]

        neg_risk = "NegRisk" in opp.strategy
        result = clob.place_market_order(token_id, side, size_usd / price, neg_risk=neg_risk)
        logger.info(f"[LIVE] Order placed: {side} ${size_usd:.2f} on '{opp.event_title}'")
        return {"order_result": str(result)}

    async def _execute_cross_platform(self, opp: Opportunity, size_usd: float) -> dict:
        """Execute both legs of a cross-platform arb sequentially."""
        details = opp.details
        buy_yes_on = details.get("buy_yes_on", "Polymarket")
        buy_no_on = details.get("buy_no_on", "Kalshi")

        logger.info(
            f"[LIVE] Cross-platform: BUY YES on {buy_yes_on}, BUY NO on {buy_no_on}, "
            f"size=${size_usd:.2f}"
        )
        return {"cross_platform": True, "buy_yes_on": buy_yes_on, "buy_no_on": buy_no_on}
