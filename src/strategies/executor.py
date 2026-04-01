"""Order executor: places trades on Polymarket CLOB or simulates in paper mode.

In paper mode, logs the trade at current market price without submitting.
In live mode, builds a signed order and posts it to the CLOB.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass

from src.data.market_registry import CryptoMarket, MarketRegistry
from src.output.db import get_connection, insert_trade
from src.strategies.momentum import Signal

logger = logging.getLogger(__name__)

MIN_SHARES = 5


@dataclass
class TradeResult:
    signal: Signal
    market: CryptoMarket
    direction: str
    price: float
    shares: float
    cost_usd: float
    order_id: str
    is_paper: bool
    timestamp: float


class Executor:
    """Paper or live order execution against Polymarket CLOB."""

    def __init__(
        self,
        registry: MarketRegistry,
        bet_size_usd: float = 15.0,
        dry_run: bool = True,
        min_secs_remaining: float = 30,
    ):
        self._registry = registry
        self._bet_size = bet_size_usd
        self._dry_run = dry_run
        self._min_secs = min_secs_remaining
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
                raise RuntimeError("POLYMARKET_PRIVATE_KEY not set for live trading")
            from src.data.polymarket_client import PolymarketCLOBClient

            self._clob = PolymarketCLOBClient(pk)
        return self._clob

    async def execute(self, signal: Signal) -> TradeResult | None:
        market = self._registry.get_active_market(
            signal.symbol, signal.direction.value, min_secs=self._min_secs
        )
        if not market:
            logger.debug(f"No active market for {signal.symbol} {signal.direction.value}")
            return None

        price = market.mid_price
        if price <= 0 or price >= 1.0:
            return None

        shares = self._bet_size / price
        if shares < MIN_SHARES:
            logger.debug(f"Shares too small: {shares:.1f} < {MIN_SHARES}")
            return None

        cost = shares * price

        if self._dry_run:
            order_id = f"paper-{self._trade_count + 1}"
            sym = signal.symbol.replace("usdt", "").upper()
            logger.info(
                f"[PAPER] {signal.direction.value} {sym} "
                f"@ ${price:.3f} x {shares:.1f} = ${cost:.2f} "
                f"(momentum: {signal.momentum_pct:+.3%})"
            )
        else:
            try:
                clob = self._ensure_clob()
                token_id = market.yes_token_id
                result = clob.place_market_order(token_id, "BUY", shares)
                order_id = str(result) if result else "unknown"
                sym = signal.symbol.replace("usdt", "").upper()
                logger.info(
                    f"[LIVE] {signal.direction.value} {sym} "
                    f"@ ${price:.3f} x {shares:.1f} = ${cost:.2f} order={order_id}"
                )
            except Exception as e:
                logger.error(f"Order execution failed: {e}")
                return None

        self._trade_count += 1
        self._total_cost += cost

        trade = TradeResult(
            signal=signal,
            market=market,
            direction=signal.direction.value,
            price=price,
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
                action=f"BUY_{signal.direction.value}",
                side="BUY",
                market_id=market.market_id,
                token_id=market.yes_token_id,
                price=price,
                size=shares,
                cost_usd=cost,
                is_paper=self._dry_run,
            )
            conn.close()
        except Exception as e:
            logger.debug(f"DB insert error: {e}")

        return trade
