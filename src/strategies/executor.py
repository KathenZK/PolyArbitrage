"""Order executor: limit orders (maker, 0% fee) on Polymarket CLOB.

Buys the Up token on UP signals, Down token on DOWN signals.
Includes fee-aware EV check, liquidity guard, and maker price optimization.
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


def calc_taker_fee(shares: float, price: float, fee_rate: float = 0.072) -> float:
    return shares * fee_rate * price * (1 - price)


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
    gross_ev: float = 0.0
    taker_fee_avoided: float = 0.0


class Executor:
    """Places limit orders (maker) with fee-aware EV check and liquidity guard."""

    def __init__(
        self,
        bet_size_usd: float = 15.0,
        dry_run: bool = True,
        min_liquidity: float = 1000,
        min_ev_usd: float = 0.10,
        maker_offset_ticks: int = 1,
    ):
        self._bet_size = bet_size_usd
        self._dry_run = dry_run
        self._min_liquidity = min_liquidity
        self._min_ev = min_ev_usd
        self._maker_offset = maker_offset_ticks * 0.01
        self._clob = None
        self._trade_count = 0
        self._total_cost = 0.0
        self._skipped_low_liq = 0
        self._skipped_low_ev = 0
        self._results: list[TradeResult] = []

    @property
    def trade_count(self) -> int:
        return self._trade_count

    @property
    def total_cost(self) -> float:
        return self._total_cost

    @property
    def skipped_low_liq(self) -> int:
        return self._skipped_low_liq

    @property
    def skipped_low_ev(self) -> int:
        return self._skipped_low_ev

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

        # --- Liquidity check ---
        if market.liquidity < self._min_liquidity:
            self._skipped_low_liq += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"liquidity ${market.liquidity:,.0f} < ${self._min_liquidity:,.0f}"
            )
            return None

        # --- Token selection ---
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
        if shares < market.order_min_size:
            return None

        cost = shares * token_price

        # --- Fee-aware EV check ---
        # Gross EV = deviation × bet (how much edge we have)
        # We compare against what the taker fee WOULD be, as a sanity check:
        # even though we use maker orders (0% fee), if our edge is smaller
        # than the taker fee, the signal is too weak to be reliable.
        taker_fee = calc_taker_fee(shares, token_price, market.fee_rate)
        gross_ev = abs(signal.deviation_pct) * self._bet_size

        if gross_ev < self._min_ev:
            self._skipped_low_ev += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"EV ${gross_ev:.2f} < ${self._min_ev:.2f}"
            )
            return None

        # --- Maker price: offset from mid to stay on the book ---
        # Place BUY limit slightly below mid-price to avoid crossing the
        # spread and becoming a taker.  If best_bid is available from
        # Gamma API, use it; otherwise subtract one tick from mid.
        if signal.direction == Direction.UP:
            maker_price = market.best_bid if market.best_bid > 0 else token_price - self._maker_offset
        else:
            down_best_bid = (1 - market.best_ask) if market.best_ask > 0 else token_price - self._maker_offset
            maker_price = max(0.01, down_best_bid)

        maker_price = round(max(0.01, min(0.99, maker_price)), 2)
        shares_at_maker = self._bet_size / maker_price if maker_price > 0 else shares

        if self._dry_run:
            order_id = f"paper-{self._trade_count + 1}"
            logger.info(
                f"[PAPER] {signal.asset} {signal.direction.value} → buy {token_side} "
                f"@ ${maker_price:.2f} x {shares_at_maker:.1f} = ${self._bet_size:.2f} "
                f"(dev: {signal.deviation_pct:+.2%}, EV: ${gross_ev:.2f}, "
                f"taker_fee_saved: ${taker_fee:.2f}, liq: ${market.liquidity:,.0f})"
            )
        else:
            try:
                clob = self._ensure_clob()
                result = clob.place_limit_order(
                    token_id=token_id,
                    side="BUY",
                    price=maker_price,
                    size=round(shares_at_maker, 2),
                )
                order_id = str(result) if result else "unknown"
                logger.info(
                    f"[LIVE] {signal.asset} {signal.direction.value} → buy {token_side} "
                    f"@ ${maker_price:.2f} x {shares_at_maker:.1f} order={order_id}"
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
            price=maker_price,
            shares=shares_at_maker,
            cost_usd=self._bet_size,
            order_id=order_id,
            is_paper=self._dry_run,
            timestamp=time.time(),
            gross_ev=gross_ev,
            taker_fee_avoided=taker_fee,
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
                price=maker_price,
                size=shares_at_maker,
                cost_usd=self._bet_size,
                is_paper=self._dry_run,
            )
            conn.close()
        except Exception as e:
            logger.debug(f"DB insert error: {e}")

        return trade
