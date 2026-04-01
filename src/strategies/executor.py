"""Order executor: limit orders (maker, 0% fee) on Polymarket CLOB.

Buys the Up token on UP signals, Down token on DOWN signals.
Supports paper mode (log only) and live mode (real orders).
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass

from src.data.market_registry import CryptoMarket
from src.output.db import get_connection, insert_trade
from src.strategies.momentum import Direction, Signal

logger = logging.getLogger(__name__)

MIN_SHARES = 5


@dataclass
class TradeResult:
    signal: Signal
    market: CryptoMarket
    direction: str
    token_side: str
    token_id: str
    price: float
    shares: float
    cost_usd: float
    order_id: str
    is_paper: bool
    timestamp: float


class Executor:
    """Places limit orders (maker) to avoid taker fees."""

    def __init__(
        self,
        bet_size_usd: float = 15.0,
        dry_run: bool = True,
    ):
        self._bet_size = bet_size_usd
        self._dry_run = dry_run
        self._clob = None
        self._trade_count = 0
        self._total_cost = 0.0
        self._results: list[TradeResult] = []

    @property
    def trade_count(self) -> int:
        return self._trade_count

    @property
    def total_cost(self) -> float:
        return self._total_cost

    @property
    def recent_trades(self) -> list[TradeResult]:
        return list(self._results[-50:])

    def _ensure_clob(self):
        if self._clob is None:
            pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
            if not pk:
                raise RuntimeError("POLYMARKET_PRIVATE_KEY not set")
            from src.data.polymarket_client import PolymarketCLOBClient

            self._clob = PolymarketCLOBClient(pk)
        return self._clob

    async def execute(self, signal: Signal) -> TradeResult | None:
        market = signal.market

        if signal.direction == Direction.UP:
            token_id = market.up_token_id
            token_price = market.up_price
            token_side = "Up"
        else:
            token_id = market.down_token_id
            token_price = market.down_price
            token_side = "Down"

        if token_price <= 0.01 or token_price >= 0.99:
            return None

        shares = self._bet_size / token_price
        if shares < MIN_SHARES:
            return None

        cost = shares * token_price

        if self._dry_run:
            order_id = f"paper-{self._trade_count + 1}"
            logger.info(
                f"[PAPER] {signal.asset} {signal.direction.value} → buy {token_side} "
                f"@ ${token_price:.3f} x {shares:.1f} = ${cost:.2f} "
                f"(dev: {signal.deviation_pct:+.2%}, open: ${signal.opening_price:,.2f})"
            )
        else:
            try:
                clob = self._ensure_clob()
                result = clob.place_limit_order(
                    token_id=token_id,
                    side="BUY",
                    price=token_price,
                    size=round(shares, 2),
                )
                order_id = str(result) if result else "unknown"
                logger.info(
                    f"[LIVE] {signal.asset} {signal.direction.value} → buy {token_side} "
                    f"@ ${token_price:.3f} x {shares:.1f} = ${cost:.2f} order={order_id}"
                )
            except Exception as e:
                logger.error(f"Order failed: {e}")
                return None

        self._trade_count += 1
        self._total_cost += cost

        trade = TradeResult(
            signal=signal,
            market=market,
            direction=signal.direction.value,
            token_side=token_side,
            token_id=token_id,
            price=token_price,
            shares=shares,
            cost_usd=cost,
            order_id=order_id,
            is_paper=self._dry_run,
            timestamp=time.time(),
        )
        self._results.append(trade)

        try:
            conn = get_connection()
            insert_trade(
                conn,
                strategy="LatencyArb",
                event_title=market.question,
                action=f"BUY_{token_side.upper()}",
                side="BUY",
                market_id=market.market_id,
                token_id=token_id,
                price=token_price,
                size=shares,
                cost_usd=cost,
                is_paper=self._dry_run,
            )
            conn.close()
        except Exception as e:
            logger.debug(f"DB insert error: {e}")

        return trade
