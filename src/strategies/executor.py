"""Order executor with reconciliation-aware maker order tracking.

Real binary-contract EV per filled trade:
    EV = B x (p / q - 1)

For post-only maker orders the submitted-order EV is lower because:
    - the order may not fill
    - fills can be adversely selected

This module still gates on filled-bet EV, but now persists every order and
reconciles pending orders against Polymarket so the runtime state matches
what actually happened on the venue.
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

from src.data.market_registry import CryptoMarket
from src.output.db import get_fill_rate_stats, get_pending_trades, insert_trade, update_trade
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
    signal: Signal | None
    market: CryptoMarket | None
    asset: str
    direction: str
    token_side: str
    token_id: str
    price: float
    shares: float
    matched_shares: float
    cost_usd: float
    matched_cost_usd: float
    order_id: str
    is_paper: bool
    timestamp: float
    status: OrderStatus = OrderStatus.PENDING
    win_prob: float = 0.0
    fill_prob: float = 0.0
    filled_ev: float = 0.0
    submitted_ev: float = 0.0
    taker_fee_avoided: float = 0.0
    expiration_ts: int = 0
    db_id: int | None = None
    last_error: str = ""
    raw_status: str = ""

    @property
    def matched_ratio(self) -> float:
        if self.shares <= 0:
            return 0.0
        if self.is_paper and self.status == OrderStatus.FILLED and self.matched_shares == 0:
            return 1.0
        return max(0.0, min(1.0, self.matched_shares / self.shares))

    @property
    def realized_ev(self) -> float:
        return self.filled_ev * self.matched_ratio

    @property
    def display_status(self) -> str:
        if self.status == OrderStatus.PENDING and self.matched_shares > 0:
            return "partial"
        return self.status.value


@dataclass
class TradePlan:
    signal: Signal
    market: CryptoMarket
    direction: str
    token_side: str
    token_id: str
    price: float
    shares: float
    cost_usd: float
    win_prob: float
    fill_prob: float
    filled_ev: float
    submitted_ev: float
    taker_fee_avoided: float
    expiration_ts: int


class Executor:
    """Places maker orders and keeps their states reconciled with Polymarket."""

    _PENDING_STATUSES = {"live", "open", "pending", "active", "delayed", "unmatched"}
    _FILLED_STATUSES = {"matched", "filled", "complete", "completed", "executed"}
    _EXPIRED_STATUSES = {"expired", "canceled", "cancelled"}
    _REJECTED_STATUSES = {"rejected", "failed", "error"}

    def __init__(
        self,
        bet_size_usd: float = 15.0,
        dry_run: bool = True,
        min_liquidity: float = 1000,
        min_ev_usd: float = 0.10,
        maker_offset_ticks: int = 1,
        adverse_selection_haircut: float = 0.05,
        reconcile_interval_secs: float = 2.0,
        fill_rate_prior: float = 0.35,
        fill_min_samples: int = 20,
        fill_lookback_hours: float = 168.0,
    ):
        self._bet_size = bet_size_usd
        self._dry_run = dry_run
        self._min_liquidity = min_liquidity
        self._min_ev = min_ev_usd
        self._maker_offset = maker_offset_ticks * 0.01
        self._haircut = adverse_selection_haircut
        self._reconcile_interval = reconcile_interval_secs
        self._fill_rate_prior = fill_rate_prior
        self._fill_min_samples = fill_min_samples
        self._fill_lookback_hours = fill_lookback_hours
        self._clob = None
        self._db = None
        self._orders: list[TradeResult] = []
        self._skipped_low_liq = 0
        self._skipped_low_ev = 0
        self._skipped_no_edge = 0
        self._last_reconcile = 0.0
        self._fill_stats_cache: dict[str, tuple[float, float]] = {}

    @property
    def trade_count(self) -> int:
        return sum(1 for o in self._orders if o.matched_shares > 0 or o.status == OrderStatus.FILLED)

    @property
    def pending_count(self) -> int:
        return sum(1 for o in self._orders if o.status == OrderStatus.PENDING)

    @property
    def total_cost(self) -> float:
        return sum(o.matched_cost_usd for o in self._orders)

    @property
    def total_committed(self) -> float:
        return sum(max(0.0, o.cost_usd - o.matched_cost_usd) for o in self._orders if o.status == OrderStatus.PENDING)

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

    def attach_db(self, conn):
        self._db = conn

    def _ensure_clob(self):
        if self._clob is None:
            pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
            if not pk:
                raise RuntimeError("POLYMARKET_PRIVATE_KEY not set")
            from src.data.polymarket_client import PolymarketCLOBClient

            self._clob = PolymarketCLOBClient(pk)
        return self._clob

    @staticmethod
    def _field(payload: Any, *names: str, default: Any = None) -> Any:
        if payload is None:
            return default
        for name in names:
            if isinstance(payload, dict) and name in payload:
                return payload[name]
            if hasattr(payload, name):
                return getattr(payload, name)
        return default

    def _extract_order_id(self, payload: Any) -> str:
        value = self._field(payload, "orderID", "order_id", "id", default="")
        return str(value or "")

    def _extract_error(self, payload: Any) -> str:
        value = self._field(payload, "errorMsg", "error", "message", default="")
        return str(value or "")

    def _extract_status(self, payload: Any) -> str:
        value = self._field(payload, "status", "orderStatus", default="")
        return str(value or "").strip().lower()

    def _extract_float(self, payload: Any, *names: str) -> float:
        value = self._field(payload, *names, default=0)
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _clamp(value: float, low: float, high: float) -> float:
        return max(low, min(high, value))

    def _empirical_fill_ratio(self, asset: str) -> float:
        cache_key = asset.upper()
        cached = self._fill_stats_cache.get(cache_key)
        now = time.time()
        if cached and now - cached[0] < 60:
            return cached[1]

        fill_ratio = self._fill_rate_prior
        if self._db is not None:
            asset_stats = get_fill_rate_stats(
                self._db,
                asset=cache_key,
                lookback_hours=self._fill_lookback_hours,
            )
            if asset_stats["samples"] >= self._fill_min_samples:
                fill_ratio = asset_stats["avg_fill_ratio"]
            else:
                global_stats = get_fill_rate_stats(
                    self._db,
                    lookback_hours=self._fill_lookback_hours,
                )
                if global_stats["samples"] >= self._fill_min_samples:
                    fill_ratio = global_stats["avg_fill_ratio"]

        fill_ratio = self._clamp(fill_ratio, 0.05, 0.95)
        self._fill_stats_cache[cache_key] = (now, fill_ratio)
        return fill_ratio

    def _estimate_fill_probability(
        self,
        *,
        asset: str,
        market: CryptoMarket,
        quote_price: float,
    ) -> float:
        prior = self._empirical_fill_ratio(asset)

        best_bid = market.best_bid if market.best_bid > 0 else quote_price
        best_ask = market.best_ask if market.best_ask > 0 else max(quote_price + 0.01, best_bid + 0.01)
        spread = max(0.01, best_ask - best_bid)
        ticks_behind_top = max(0.0, (best_bid - quote_price) / 0.01)

        queue_factor = 1.0 / (1.0 + ticks_behind_top)
        spread_factor = self._clamp(spread / 0.03, 0.5, 1.25)
        time_factor = self._clamp(market.secs_remaining / 300.0, 0.25, 1.0)
        liquidity_factor = self._clamp(market.liquidity / max(self._min_liquidity, 1.0), 0.5, 1.5)

        fill_prob = prior
        fill_prob *= 0.7 + 0.3 * queue_factor
        fill_prob *= 0.85 + 0.15 * min(1.0, spread_factor)
        fill_prob *= 0.6 + 0.4 * time_factor
        fill_prob *= 0.75 + 0.25 * min(1.0, liquidity_factor)

        return self._clamp(fill_prob, 0.02, 0.98)

    def _normalize_status(self, raw_status: str, expiration_ts: int = 0) -> OrderStatus | None:
        if raw_status in self._FILLED_STATUSES:
            return OrderStatus.FILLED
        if raw_status in self._PENDING_STATUSES:
            return OrderStatus.PENDING
        if raw_status in self._EXPIRED_STATUSES:
            return OrderStatus.EXPIRED
        if raw_status in self._REJECTED_STATUSES:
            return OrderStatus.REJECTED
        if expiration_ts and time.time() >= expiration_ts:
            return OrderStatus.EXPIRED
        return None

    def _parse_order_state(
        self,
        payload: Any,
        *,
        target_shares: float,
        price: float,
        expiration_ts: int = 0,
    ) -> tuple[OrderStatus | None, float, float, str, str]:
        raw_status = self._extract_status(payload)
        status = self._normalize_status(raw_status, expiration_ts=expiration_ts)

        matched_shares = max(
            self._extract_float(
                payload,
                "matched_size",
                "matchedSize",
                "size_matched",
                "sizeMatched",
                "filled_size",
                "filledSize",
            ),
            self._extract_float(payload, "maker_fill_size", "makerFillSize"),
        )
        if matched_shares <= 0 and status == OrderStatus.FILLED:
            matched_shares = target_shares

        matched_shares = min(target_shares, matched_shares)
        matched_cost_usd = round(matched_shares * price, 6)
        error = self._extract_error(payload)
        return status, matched_shares, matched_cost_usd, raw_status, error

    def _append_trade(self, trade: TradeResult):
        self._orders = [t for t in self._orders if not (trade.order_id and t.order_id == trade.order_id)]
        self._orders.append(trade)

    def _persist_trade(self, trade: TradeResult, raw_data: Any | None = None):
        if self._db is None:
            return

        if trade.db_id is None:
            trade.db_id = insert_trade(
                self._db,
                strategy="latency_arb",
                event_title=trade.market.question if trade.market else trade.asset,
                action=trade.direction,
                side=trade.token_side,
                asset=trade.asset,
                market_id=trade.market.market_id if trade.market else "",
                market_slug=trade.market.slug if trade.market else "",
                token_id=trade.token_id,
                price=trade.price,
                size=trade.shares,
                matched_size=trade.matched_shares,
                cost_usd=trade.cost_usd,
                matched_cost_usd=trade.matched_cost_usd,
                is_paper=trade.is_paper,
                status=trade.status.value,
                order_id=trade.order_id,
                win_prob=trade.win_prob,
                fill_prob=trade.fill_prob,
                filled_ev_usd=trade.filled_ev,
                expected_value_usd=trade.submitted_ev,
                taker_fee_avoided=trade.taker_fee_avoided,
                expiration_ts=trade.expiration_ts,
                last_error=trade.last_error,
                raw_data=raw_data,
            )
        else:
            update_trade(
                self._db,
                trade.db_id,
                status=trade.status.value,
                matched_size=trade.matched_shares,
                matched_cost_usd=trade.matched_cost_usd,
                order_id=trade.order_id,
                last_error=trade.last_error,
                raw_data=raw_data,
            )

    def bootstrap_pending_orders(self):
        if self._db is None:
            return

        known = {t.order_id for t in self._orders if t.order_id}
        for row in get_pending_trades(self._db):
            order_id = str(row.get("order_id", "") or "")
            if not order_id or order_id in known:
                continue

            status_raw = str(row.get("status", OrderStatus.PENDING.value))
            try:
                status = OrderStatus(status_raw)
            except ValueError:
                status = OrderStatus.PENDING

            trade = TradeResult(
                signal=None,
                market=None,
                asset=str(row.get("asset", "") or ""),
                direction=str(row.get("action", "") or ""),
                token_side=str(row.get("side", "") or ""),
                token_id=str(row.get("token_id", "") or ""),
                price=float(row.get("price", 0) or 0),
                shares=float(row.get("size", 0) or 0),
                matched_shares=float(row.get("matched_size", 0) or 0),
                cost_usd=float(row.get("cost_usd", 0) or 0),
                matched_cost_usd=float(row.get("matched_cost_usd", 0) or 0),
                order_id=order_id,
                is_paper=bool(row.get("is_paper", 0)),
                timestamp=float(row.get("timestamp", 0) or 0),
                status=status,
                win_prob=float(row.get("win_prob", 0) or 0),
                fill_prob=float(row.get("fill_prob", 0) or 0),
                filled_ev=float(row.get("filled_ev_usd", row.get("expected_value_usd", 0)) or 0),
                submitted_ev=float(row.get("expected_value_usd", 0) or 0),
                taker_fee_avoided=float(row.get("taker_fee_avoided", 0) or 0),
                expiration_ts=int(row.get("expiration_ts", 0) or 0),
                db_id=int(row.get("id", 0) or 0),
                last_error=str(row.get("last_error", "") or ""),
                raw_status=status.value,
            )
            self._append_trade(trade)

    async def reconcile_pending_orders(self, force: bool = False) -> int:
        if self._dry_run:
            return 0

        now = time.time()
        if not force and now - self._last_reconcile < self._reconcile_interval:
            return 0
        self._last_reconcile = now

        pending = [trade for trade in self._orders if trade.status == OrderStatus.PENDING]
        if not pending:
            return 0

        clob = self._ensure_clob()
        updated = 0

        open_orders_by_id: dict[str, Any] = {}
        try:
            for order in clob.get_open_orders() or []:
                order_id = self._extract_order_id(order)
                if order_id:
                    open_orders_by_id[order_id] = order
        except Exception as exc:
            logger.debug(f"Open-order fetch failed: {exc}")

        for trade in pending:
            payload = open_orders_by_id.get(trade.order_id)
            if payload is not None and trade.expiration_ts and now >= trade.expiration_ts:
                try:
                    clob.cancel_order(trade.order_id)
                    payload = {"status": "expired"}
                except Exception as exc:
                    logger.debug(f"Cancel after expiry failed for {trade.order_id}: {exc}")

            if payload is None:
                try:
                    payload = clob.get_order(trade.order_id)
                except Exception as exc:
                    if trade.expiration_ts and now >= trade.expiration_ts:
                        payload = {"status": "expired", "errorMsg": str(exc)}
                    else:
                        logger.debug(f"Order fetch failed for {trade.order_id}: {exc}")
                        continue

            status, matched_shares, matched_cost_usd, raw_status, error = self._parse_order_state(
                payload,
                target_shares=trade.shares,
                price=trade.price,
                expiration_ts=trade.expiration_ts,
            )

            new_status = status or trade.status
            new_matched_shares = max(trade.matched_shares, matched_shares)
            new_matched_cost = max(trade.matched_cost_usd, matched_cost_usd)
            changed = (
                new_status != trade.status
                or abs(new_matched_shares - trade.matched_shares) > 1e-9
                or abs(new_matched_cost - trade.matched_cost_usd) > 1e-9
                or error != trade.last_error
                or raw_status != trade.raw_status
            )
            if not changed:
                continue

            trade.status = new_status
            trade.matched_shares = new_matched_shares
            trade.matched_cost_usd = new_matched_cost
            trade.last_error = error
            trade.raw_status = raw_status
            self._persist_trade(trade, raw_data=payload)
            updated += 1

            logger.info(
                f"Order {trade.order_id} -> {trade.display_status} "
                f"filled={trade.matched_shares:.2f}/{trade.shares:.2f}"
            )

        return updated

    def evaluate_signal(self, signal: Signal) -> TradePlan | None:
        market = signal.market
        if market.liquidity < self._min_liquidity:
            self._skipped_low_liq += 1
            return None

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

        clob_bid = 0.0
        if not self._dry_run:
            try:
                clob = self._ensure_clob()
                clob_bid = clob.get_best_bid(token_id)
            except Exception as exc:
                logger.debug(f"CLOB orderbook fetch failed, using Gamma: {exc}")

        if clob_bid > 0:
            q = clob_bid
        elif signal.direction == Direction.UP:
            q = market.best_bid if market.best_bid > 0 else token_price - self._maker_offset
        else:
            down_best_bid = (1 - market.best_ask) if market.best_ask > 0 else token_price - self._maker_offset
            q = max(0.01, down_best_bid)

        q = round(max(0.01, min(0.99, q)), 2)

        p_raw = signal.win_prob
        p = max(0.0, p_raw - self._haircut)
        if p <= q:
            self._skipped_no_edge += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"p={p:.3f} <= q={q:.3f} after {self._haircut:.0%} haircut"
            )
            return None

        shares = self._bet_size / q
        if shares < market.order_min_size:
            return None

        filled_ev = self._bet_size * (p / q - 1)
        fill_prob = self._estimate_fill_probability(
            asset=signal.asset,
            market=market,
            quote_price=q,
        )
        submitted_ev = filled_ev * fill_prob
        taker_fee = calc_taker_fee(shares, q, market.fee_rate)

        if submitted_ev < self._min_ev:
            self._skipped_low_ev += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"submitted_EV ${submitted_ev:.2f} < ${self._min_ev:.2f} "
                f"(filled_EV=${filled_ev:.2f}, fill={fill_prob:.1%}, p={p:.3f}, q={q:.2f})"
            )
            return None

        expiration = int(market.end_time) + 60 if market.end_time > 0 else 0
        return TradePlan(
            signal=signal,
            market=market,
            direction=signal.direction.value,
            token_side=token_side,
            token_id=token_id,
            price=q,
            shares=shares,
            cost_usd=self._bet_size,
            win_prob=p,
            fill_prob=fill_prob,
            filled_ev=filled_ev,
            submitted_ev=submitted_ev,
            taker_fee_avoided=taker_fee,
            expiration_ts=expiration,
        )

    async def execute(self, signal: Signal) -> TradeResult | None:
        plan = self.evaluate_signal(signal)
        if plan is None:
            return None

        market = plan.market
        raw_response: Any = {}
        last_error = ""

        if self._dry_run:
            order_id = f"paper-{len(self._orders) + 1}"
            status = OrderStatus.FILLED
            matched_shares = plan.shares
            matched_cost_usd = round(plan.cost_usd, 6)
            logger.info(
                f"[PAPER] {signal.asset} {signal.direction.value} -> buy {plan.token_side} "
                f"@ ${plan.price:.2f} x {plan.shares:.1f} = ${plan.cost_usd:.2f} | "
                f"p_adj={plan.win_prob:.1%} fill={plan.fill_prob:.1%} "
                f"fEV=${plan.filled_ev:.2f} sEV=${plan.submitted_ev:.2f}"
            )
        else:
            try:
                clob = self._ensure_clob()
                raw_response = clob.place_limit_order(
                    token_id=plan.token_id,
                    side="BUY",
                    price=plan.price,
                    size=round(plan.shares, 2),
                    expiration=plan.expiration_ts,
                    post_only=True,
                )
                order_id = self._extract_order_id(raw_response)
                last_error = self._extract_error(raw_response)
                if not order_id:
                    logger.warning(f"Order rejected: {last_error or raw_response}")
                    return None

                status, matched_shares, matched_cost_usd, raw_status, _ = self._parse_order_state(
                    raw_response,
                    target_shares=plan.shares,
                    price=plan.price,
                    expiration_ts=plan.expiration_ts,
                )
                status = status or OrderStatus.PENDING
                logger.info(
                    f"[LIVE] {signal.asset} {signal.direction.value} -> buy {plan.token_side} "
                    f"@ ${plan.price:.2f} x {plan.shares:.1f} | "
                    f"p={plan.win_prob:.1%} fill={plan.fill_prob:.1%} "
                    f"fEV=${plan.filled_ev:.2f} sEV=${plan.submitted_ev:.2f} "
                    f"order={order_id} status={raw_status or status.value}"
                )
            except Exception as exc:
                logger.error(f"Order failed: {exc}")
                return None

        trade = TradeResult(
            signal=signal,
            market=market,
            asset=signal.asset,
            direction=plan.direction,
            token_side=plan.token_side,
            token_id=plan.token_id,
            price=plan.price,
            shares=plan.shares,
            matched_shares=matched_shares,
            cost_usd=plan.cost_usd,
            matched_cost_usd=matched_cost_usd,
            order_id=order_id,
            is_paper=self._dry_run,
            timestamp=time.time(),
            status=status,
            win_prob=plan.win_prob,
            fill_prob=plan.fill_prob,
            filled_ev=plan.filled_ev,
            submitted_ev=plan.submitted_ev,
            taker_fee_avoided=plan.taker_fee_avoided,
            expiration_ts=plan.expiration_ts,
            last_error=last_error,
            raw_status=status.value if self._dry_run else self._extract_status(raw_response),
        )
        self._append_trade(trade)
        self._persist_trade(trade, raw_data=raw_response)
        return trade
