"""Order executor with real EV calculation.

Real binary-contract EV per trade:
    EV = B × (p / q - 1)

where:
    B = bet size in USD
    p = estimated win probability (from Brownian motion model)
    q = entry price (Polymarket token price we buy at)

The trade is only placed when EV > min_ev_usd AND p > q (positive edge).

For post-only maker orders, the actual EV is further reduced by:
    - fill probability f (order might not fill)
    - adverse selection (p_fill < p — being filled often means price is reverting)

We apply a configurable adverse_selection_haircut (default 5%) to p before
computing EV as a conservative adjustment.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from enum import Enum

from src.data.market_registry import CryptoMarket
from src.strategies.momentum import Direction, Signal

logger = logging.getLogger(__name__)


def calc_taker_fee(shares: float, price: float, fee_rate: float = 0.072) -> float:
    return shares * fee_rate * price * (1 - price)


class OrderStatus(str, Enum):
    PENDING = "pending"
    FILLED = "filled"
    REJECTED = "rejected"
    EXPIRED = "expired"


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
    status: OrderStatus = OrderStatus.PENDING
    win_prob: float = 0.0
    real_ev: float = 0.0
    taker_fee_avoided: float = 0.0


class Executor:
    """Places limit orders when real EV is positive: p/q > 1."""

    def __init__(
        self,
        bet_size_usd: float = 15.0,
        dry_run: bool = True,
        min_liquidity: float = 1000,
        min_ev_usd: float = 0.10,
        maker_offset_ticks: int = 1,
        adverse_selection_haircut: float = 0.05,
    ):
        self._bet_size = bet_size_usd
        self._dry_run = dry_run
        self._min_liquidity = min_liquidity
        self._min_ev = min_ev_usd
        self._maker_offset = maker_offset_ticks * 0.01
        self._haircut = adverse_selection_haircut
        self._clob = None
        self._orders: list[TradeResult] = []
        self._skipped_low_liq = 0
        self._skipped_low_ev = 0
        self._skipped_no_edge = 0

    @property
    def trade_count(self) -> int:
        return sum(1 for o in self._orders if o.status == OrderStatus.FILLED)

    @property
    def pending_count(self) -> int:
        return sum(1 for o in self._orders if o.status == OrderStatus.PENDING)

    @property
    def total_cost(self) -> float:
        return sum(o.cost_usd for o in self._orders if o.status == OrderStatus.FILLED)

    @property
    def total_committed(self) -> float:
        return sum(o.cost_usd for o in self._orders if o.status in (OrderStatus.PENDING, OrderStatus.FILLED))

    @property
    def skipped_low_liq(self) -> int:
        return self._skipped_low_liq

    @property
    def skipped_low_ev(self) -> int:
        return self._skipped_low_ev

    @property
    def skipped_no_edge(self) -> int:
        return self._skipped_no_edge

    @property
    def recent_trades(self) -> list[TradeResult]:
        return list(self._orders[-50:])

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

        # --- Maker price: fetch live or use Gamma ---
        live_bid = 0.0
        if not self._dry_run:
            try:
                clob = self._ensure_clob()
                live_bid = clob.get_price(token_id, "buy")
            except Exception as e:
                logger.debug(f"CLOB price fetch failed, using Gamma: {e}")

        if live_bid > 0:
            q = round(live_bid, 2)
        elif signal.direction == Direction.UP:
            q = market.best_bid if market.best_bid > 0 else token_price - self._maker_offset
        else:
            down_best_bid = (1 - market.best_ask) if market.best_ask > 0 else token_price - self._maker_offset
            q = max(0.01, down_best_bid)

        q = round(max(0.01, min(0.99, q)), 2)

        # --- Real EV calculation ---
        p_raw = signal.win_prob
        p = max(0.0, p_raw - self._haircut)

        if p <= q:
            self._skipped_no_edge += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"p={p:.3f} <= q={q:.3f} (no edge after {self._haircut:.0%} haircut)"
            )
            return None

        shares = self._bet_size / q
        if shares < market.order_min_size:
            return None

        real_ev = self._bet_size * (p / q - 1)
        taker_fee = calc_taker_fee(shares, q, market.fee_rate)

        if real_ev < self._min_ev:
            self._skipped_low_ev += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"EV ${real_ev:.2f} < ${self._min_ev:.2f} (p={p:.3f}, q={q:.2f})"
            )
            return None

        # --- Place order ---
        expiration = int(market.end_time) + 60 if market.end_time > 0 else 0

        if self._dry_run:
            order_id = f"paper-{len(self._orders) + 1}"
            status = OrderStatus.FILLED
            logger.info(
                f"[PAPER] {signal.asset} {signal.direction.value} → buy {token_side} "
                f"@ ${q:.2f} x {shares:.1f} = ${self._bet_size:.2f} | "
                f"p={p_raw:.1%} p_adj={p:.1%} q={q:.2f} "
                f"EV=${real_ev:.2f} fee_saved=${taker_fee:.2f}"
            )
        else:
            try:
                clob = self._ensure_clob()
                result = clob.place_limit_order(
                    token_id=token_id,
                    side="BUY",
                    price=q,
                    size=round(shares, 2),
                    expiration=expiration,
                    post_only=True,
                )
                resp = result if isinstance(result, dict) else {}
                order_id = resp.get("orderID", "")
                resp_status = resp.get("status", "")
                err = resp.get("errorMsg", "")

                if not order_id:
                    logger.warning(f"Order rejected: {err or resp}")
                    return None

                status = OrderStatus.FILLED if resp_status == "matched" else OrderStatus.PENDING
                logger.info(
                    f"[LIVE] {signal.asset} {signal.direction.value} → buy {token_side} "
                    f"@ ${q:.2f} x {shares:.1f} | "
                    f"p={p:.1%} EV=${real_ev:.2f} order={order_id} status={resp_status}"
                )
            except Exception as e:
                logger.error(f"Order failed: {e}")
                return None

        trade = TradeResult(
            signal=signal,
            market=market,
            direction=signal.direction.value,
            token_side=token_side,
            token_id=token_id,
            price=q,
            shares=shares,
            cost_usd=self._bet_size,
            order_id=order_id,
            is_paper=self._dry_run,
            timestamp=time.time(),
            status=status,
            win_prob=p,
            real_ev=real_ev,
            taker_fee_avoided=taker_fee,
        )
        self._orders.append(trade)

        return trade
