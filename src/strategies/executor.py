"""Order executor with reconciliation-aware maker order tracking.

Real binary-contract EV per filled trade:
    EV = B x (p / q - 1)

For post-only maker orders the submitted-order EV is lower because:
    - the order may not fill
    - fills can be adversely selected

This module gates on fill-adjusted submitted EV, persists every order, and
reconciles pending orders against Polymarket so the runtime state matches
what actually happened on the venue.
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import time
from dataclasses import dataclass
from enum import Enum
from math import exp, floor, sqrt
from typing import Any

from src.data.market_registry import CryptoMarket
from src.output.db import (
    get_fill_calibration_rows,
    get_live_daily_usage,
    get_pending_trades,
    insert_trade,
    update_trade,
)
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
    binance_symbol: str
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
    expected_fill_ratio: float = 0.0
    fill_ratio_lower_bound: float = 0.0
    fill_confidence: float = 0.0
    fill_effective_samples: float = 0.0
    fill_source: str = ""
    filled_ev: float = 0.0
    submitted_ev: float = 0.0
    taker_fee_avoided: float = 0.0
    expiration_ts: int = 0
    secs_remaining_at_submit: float = 0.0
    liquidity_at_submit: float = 0.0
    spread_at_submit: float = 0.0
    queue_ticks_at_submit: float = 0.0
    tick_size_at_submit: float = 0.01
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

    @property
    def fill_prob(self) -> float:
        return self.expected_fill_ratio


@dataclass
class FillEstimate:
    expected_fill_ratio: float
    conservative_fill_ratio: float
    confidence: float
    effective_samples: float
    source: str


@dataclass
class TokenQuote:
    token_id: str
    best_bid: float
    best_ask: float
    spread: float
    tick_size: float


@dataclass
class LivePreflight:
    ok: bool
    signer_address: str
    funder_address: str
    signature_type: int
    collateral_balance: float
    max_allowance: float
    issues: list[str]
    warnings: list[str]


@dataclass
class TradePlan:
    signal: Signal
    market: CryptoMarket
    binance_symbol: str
    direction: str
    token_side: str
    token_id: str
    price: float
    shares: float
    cost_usd: float
    win_prob: float
    expected_fill_ratio: float
    fill_ratio_lower_bound: float
    fill_confidence: float
    fill_effective_samples: float
    fill_source: str
    filled_ev: float
    submitted_ev: float
    taker_fee_avoided: float
    expiration_ts: int
    secs_remaining_at_submit: float
    liquidity_at_submit: float
    spread_at_submit: float
    queue_ticks_at_submit: float
    tick_size_at_submit: float

    @property
    def fill_prob(self) -> float:
        return self.expected_fill_ratio


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
        adverse_selection_time_ramp_sec: float = 300.0,
        fill_adverse_coeff: float = 0.03,
        max_bet_multiplier: float = 2.5,
        reconcile_interval_secs: float = 2.0,
        fill_rate_prior: float = 0.35,
        fill_min_samples: int = 20,
        fill_lookback_hours: float = 168.0,
        fill_decay_half_life_hours: float = 24.0,
        fill_prior_strength: float = 12.0,
        fill_confidence_scale: float = 8.0,
        fill_lower_bound_z: float = 1.0,
        max_live_orders_per_day: int = 0,
        max_live_notional_usd_per_day: float = 0.0,
        max_consecutive_expired: int = 0,
        circuit_breaker_cooldown_sec: float = 900.0,
        max_directional_exposure_usd: float = 0.0,
    ):
        self._bet_size = bet_size_usd
        self._dry_run = dry_run
        self._min_liquidity = min_liquidity
        self._min_ev = min_ev_usd
        self._maker_offset_ticks = maker_offset_ticks
        self._haircut = adverse_selection_haircut
        self._haircut_ramp_sec = adverse_selection_time_ramp_sec
        self._fill_adverse_coeff = fill_adverse_coeff
        self._max_bet_multiplier = max_bet_multiplier
        self._reconcile_interval = reconcile_interval_secs
        self._fill_rate_prior = fill_rate_prior
        self._fill_min_samples = fill_min_samples
        self._fill_lookback_hours = fill_lookback_hours
        self._fill_decay_half_life_hours = fill_decay_half_life_hours
        self._fill_prior_strength = fill_prior_strength
        self._fill_confidence_scale = fill_confidence_scale
        self._fill_lower_bound_z = fill_lower_bound_z
        self._max_live_orders_per_day = max_live_orders_per_day
        self._max_live_notional_usd_per_day = max_live_notional_usd_per_day
        self._max_consecutive_expired = max_consecutive_expired
        self._circuit_breaker_cooldown = circuit_breaker_cooldown_sec
        self._max_directional_exposure = max_directional_exposure_usd
        self._clob = None
        self._db = None
        self._orders: list[TradeResult] = []
        self._skipped_low_liq = 0
        self._skipped_low_ev = 0
        self._skipped_no_edge = 0
        self._skipped_live_limits = 0
        self._skipped_circuit_breaker = 0
        self._skipped_bet_size = 0
        self._consecutive_expired = 0
        self._circuit_breaker_until = 0.0
        self._last_reconcile = 0.0
        self._last_clob_heartbeat = 0.0
        self._heartbeat_id = ""
        self._fill_stats_cache: dict[tuple[str, str, str, str, str], tuple[float, FillEstimate]] = {}
        self._execute_lock = asyncio.Lock()

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
    def skipped_live_limits(self) -> int:
        return self._skipped_live_limits

    @property
    def skipped_circuit_breaker(self) -> int:
        return self._skipped_circuit_breaker

    @property
    def skipped_bet_size(self) -> int:
        return self._skipped_bet_size

    @property
    def recent_trades(self) -> list[TradeResult]:
        return list(self._orders[-50:])

    @property
    def bet_size(self) -> float:
        return self._bet_size

    @property
    def min_ev(self) -> float:
        return self._min_ev

    @property
    def haircut(self) -> float:
        return self._haircut

    @staticmethod
    def _asset_to_binance_symbol(asset: str) -> str:
        return f"{asset.lower()}usdt"

    @staticmethod
    def _round_price_to_tick(price: float, tick_size: float) -> float:
        tick = max(tick_size, 0.001)
        upper = max(tick, 1.0 - tick)
        clipped = max(tick, min(upper, price))
        ticks = floor((clipped + 1e-12) / tick)
        rounded = round(ticks * tick, 6)
        return max(tick, min(upper, rounded))

    @staticmethod
    def _format_tick_size(tick_size: float) -> str:
        return f"{max(tick_size, 0.001):.3f}".rstrip("0").rstrip(".")

    @property
    def max_daily_orders(self) -> int:
        return self._max_live_orders_per_day

    @property
    def max_daily_notional(self) -> float:
        return self._max_live_notional_usd_per_day

    def attach_db(self, conn):
        self._db = conn

    def _ensure_clob(self):
        if self._clob is None:
            pk = os.getenv("POLYMARKET_PRIVATE_KEY", "")
            if not pk:
                raise RuntimeError("POLYMARKET_PRIVATE_KEY not set")
            signature_type_raw = os.getenv("POLYMARKET_SIGNATURE_TYPE", "0").strip() or "0"
            try:
                signature_type = int(signature_type_raw)
            except ValueError as exc:
                raise RuntimeError(
                    f"Invalid POLYMARKET_SIGNATURE_TYPE={signature_type_raw!r}; expected an integer"
                ) from exc

            funder = os.getenv("POLYMARKET_FUNDER", "").strip()
            api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
            api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
            api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
            from src.data.polymarket_client import PolymarketCLOBClient

            self._clob = PolymarketCLOBClient(
                pk,
                signature_type=signature_type,
                funder=funder,
                api_key=api_key,
                api_secret=api_secret,
                api_passphrase=api_passphrase,
            )
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

    @staticmethod
    def _time_bucket(secs_remaining: float) -> str:
        if secs_remaining < 120:
            return "short"
        if secs_remaining < 360:
            return "mid"
        return "long"

    @staticmethod
    def _spread_bucket(spread: float) -> str:
        ticks = round(max(spread, 0.01) / 0.01)
        if ticks <= 1:
            return "tight"
        if ticks <= 2:
            return "medium"
        return "wide"

    def _liquidity_bucket(self, liquidity: float) -> str:
        if liquidity < self._min_liquidity * 2:
            return "low"
        if liquidity < self._min_liquidity * 5:
            return "mid"
        return "high"

    @staticmethod
    def _queue_bucket(queue_ticks: float) -> str:
        if queue_ticks <= 0.01:
            return "top"
        if queue_ticks <= 1.0:
            return "near"
        return "deep"

    @staticmethod
    def _effective_sample_size(weights: list[float]) -> float:
        if not weights:
            return 0.0
        sum_w = sum(weights)
        sum_w_sq = sum(weight * weight for weight in weights)
        if sum_w_sq <= 0:
            return 0.0
        return (sum_w * sum_w) / sum_w_sq

    def _heuristic_fill_ratio(
        self,
        *,
        market: CryptoMarket,
        quote_price: float,
        queue_ticks: float,
        spread: float,
    ) -> float:
        fill_ratio = self._fill_rate_prior
        queue_factor = 1.0 / (1.0 + queue_ticks)
        spread_factor = self._clamp(spread / 0.03, 0.5, 1.25)
        time_factor = self._clamp(market.secs_remaining / 300.0, 0.25, 1.0)
        liquidity_factor = self._clamp(market.liquidity / max(self._min_liquidity, 1.0), 0.5, 1.5)

        fill_ratio *= 0.7 + 0.3 * queue_factor
        fill_ratio *= 0.85 + 0.15 * min(1.0, spread_factor)
        fill_ratio *= 0.6 + 0.4 * time_factor
        fill_ratio *= 0.75 + 0.25 * min(1.0, liquidity_factor)
        return self._clamp(fill_ratio, 0.02, 0.98)

    def _estimate_fill_ratio(
        self,
        *,
        asset: str,
        market: CryptoMarket,
        quote_price: float,
        queue_ticks: float,
        spread: float,
    ) -> FillEstimate:
        asset_key = asset.upper()
        time_bucket = self._time_bucket(market.secs_remaining)
        spread_bucket = self._spread_bucket(spread)
        liquidity_bucket = self._liquidity_bucket(market.liquidity)
        queue_bucket = self._queue_bucket(queue_ticks)
        cache_key = (asset_key, time_bucket, spread_bucket, liquidity_bucket, queue_bucket)

        now = time.time()
        cached = self._fill_stats_cache.get(cache_key)
        if cached and now - cached[0] < 60:
            return cached[1]

        prior_mean = self._heuristic_fill_ratio(
            market=market,
            quote_price=quote_price,
            queue_ticks=queue_ticks,
            spread=spread,
        )
        prior_strength = self._fill_prior_strength
        prior_var = max(0.01, prior_mean * (1 - prior_mean) * 0.5)

        weighted_ratios: list[tuple[float, float]] = []
        source = "heuristic"

        if self._db is not None:
            rows = get_fill_calibration_rows(
                self._db,
                lookback_hours=self._fill_lookback_hours,
            )
            decay_base = 0.6931471805599453 / max(self._fill_decay_half_life_hours, 1e-9)
            for row in rows:
                row_size = float(row.get("size", 0) or 0)
                if row_size <= 0:
                    continue

                fill_ratio = self._clamp(float(row.get("matched_size", 0) or 0) / row_size, 0.0, 1.0)
                age_hours = max(0.0, (now - float(row.get("timestamp", now) or now)) / 3600.0)
                age_weight = exp(-decay_base * age_hours)

                row_asset = str(row.get("asset", "") or "").upper()
                asset_weight = 1.0 if row_asset == asset_key else 0.15

                row_time_bucket = self._time_bucket(float(row.get("secs_remaining_at_submit", 0) or 0))
                row_spread_bucket = self._spread_bucket(float(row.get("spread_at_submit", 0.01) or 0.01))
                row_liquidity_bucket = self._liquidity_bucket(float(row.get("liquidity_at_submit", 0) or 0))
                row_queue_bucket = self._queue_bucket(float(row.get("queue_ticks_at_submit", 0) or 0))

                time_weight = 1.0 if row_time_bucket == time_bucket else 0.55
                spread_weight = 1.0 if row_spread_bucket == spread_bucket else 0.75
                liquidity_weight = 1.0 if row_liquidity_bucket == liquidity_bucket else 0.75
                queue_weight = 1.0 if row_queue_bucket == queue_bucket else 0.6

                weight = age_weight * asset_weight * time_weight * spread_weight * liquidity_weight * queue_weight
                if weight < 0.01:
                    continue
                weighted_ratios.append((weight, fill_ratio))

            if weighted_ratios and len(weighted_ratios) >= self._fill_min_samples:
                source = "decayed_history"
            elif weighted_ratios:
                weighted_ratios = []
                source = "heuristic_insufficient_samples"

        sample_weight_sum = sum(weight for weight, _ in weighted_ratios)
        sample_weight_sq_sum = sum(weight * weight for weight, _ in weighted_ratios)
        posterior_mean = prior_mean
        variance = prior_var
        effective_samples = prior_strength

        if sample_weight_sum > 0:
            sample_mean = sum(weight * ratio for weight, ratio in weighted_ratios) / sample_weight_sum
            posterior_mean = (
                prior_strength * prior_mean + sample_weight_sum * sample_mean
            ) / (prior_strength + sample_weight_sum)

            sample_var = sum(weight * ((ratio - sample_mean) ** 2) for weight, ratio in weighted_ratios) / sample_weight_sum
            variance = (
                prior_strength * prior_var + sample_weight_sum * sample_var
            ) / (prior_strength + sample_weight_sum)
            effective_samples = prior_strength + (
                (sample_weight_sum * sample_weight_sum) / sample_weight_sq_sum if sample_weight_sq_sum > 0 else 0.0
            )

        confidence = self._clamp(
            effective_samples / (effective_samples + self._fill_confidence_scale),
            0.0,
            1.0,
        )
        stderr = sqrt(max(variance, 1e-6) / max(effective_samples, 1.0))
        conservative = posterior_mean - self._fill_lower_bound_z * stderr - (1 - confidence) * 0.05
        conservative = self._clamp(conservative, 0.02, posterior_mean)
        estimate = FillEstimate(
            expected_fill_ratio=self._clamp(posterior_mean, 0.02, 0.98),
            conservative_fill_ratio=conservative,
            confidence=confidence,
            effective_samples=effective_samples,
            source=source,
        )
        self._fill_stats_cache[cache_key] = (now, estimate)
        return estimate

    @staticmethod
    def _parse_usdc_amount(value: Any) -> float:
        try:
            raw = float(value or 0)
        except (TypeError, ValueError):
            return 0.0
        if raw > 1000:
            return raw / 1_000_000.0
        return raw

    def live_preflight(self) -> LivePreflight:
        issues: list[str] = []
        warnings: list[str] = []

        signature_type_raw = os.getenv("POLYMARKET_SIGNATURE_TYPE", "0").strip() or "0"
        try:
            signature_type = int(signature_type_raw)
        except ValueError:
            signature_type = -1
            issues.append(f"Invalid POLYMARKET_SIGNATURE_TYPE={signature_type_raw!r}")

        funder = os.getenv("POLYMARKET_FUNDER", "").strip()
        signer = ""
        collateral_balance = 0.0
        max_allowance = 0.0

        try:
            clob = self._ensure_clob()
            signer = str(clob.get_signer_address() or "")
            raw_balance = clob.get_collateral_balance_allowance(signature_type=signature_type)
        except Exception as exc:
            issues.append(f"CLOB preflight failed: {exc}")
            return LivePreflight(
                ok=False,
                signer_address=signer,
                funder_address=funder,
                signature_type=signature_type,
                collateral_balance=collateral_balance,
                max_allowance=max_allowance,
                issues=issues,
                warnings=warnings,
            )

        balance_value = self._field(raw_balance, "balance", default=0)
        collateral_balance = self._parse_usdc_amount(balance_value)

        allowances = self._field(raw_balance, "allowances", default={}) or {}
        if isinstance(allowances, dict):
            max_allowance = max((self._parse_usdc_amount(v) for v in allowances.values()), default=0.0)

        if signature_type == 0 and signer and funder and signer.lower() != funder.lower():
            issues.append("EOA mode requires signer and funder to match")
        if signature_type != 0 and not funder:
            issues.append("Proxy-wallet mode requires POLYMARKET_FUNDER")
        if collateral_balance + 1e-9 < self._bet_size:
            issues.append(
                f"Insufficient collateral balance (${collateral_balance:.6f}) for bet_size ${self._bet_size:.2f}"
            )
        if max_allowance + 1e-9 < self._bet_size:
            issues.append(
                f"Insufficient collateral allowance (${max_allowance:.6f}) for bet_size ${self._bet_size:.2f}"
            )
        if not signer:
            warnings.append("Signer address unavailable")

        return LivePreflight(
            ok=not issues,
            signer_address=signer,
            funder_address=funder,
            signature_type=signature_type,
            collateral_balance=collateral_balance,
            max_allowance=max_allowance,
            issues=issues,
            warnings=warnings,
        )

    def _time_adjusted_haircut(self, secs_remaining: float) -> float:
        """Adverse selection increases as settlement approaches."""
        if secs_remaining >= self._haircut_ramp_sec:
            return self._haircut
        ramp = max(0.0, 1.0 - secs_remaining / self._haircut_ramp_sec)
        return self._haircut * (1.0 + ramp)

    def _active_directional_exposure(self) -> dict[str, float]:
        """Total outstanding exposure per direction across recent orders."""
        exposure: dict[str, float] = {"UP": 0.0, "DOWN": 0.0}
        cutoff = time.time() - 900
        for trade in self._orders:
            if trade.timestamp < cutoff:
                continue
            direction = trade.direction.upper()
            if direction not in exposure:
                continue
            if trade.status == OrderStatus.PENDING:
                exposure[direction] += trade.cost_usd
            elif trade.status == OrderStatus.FILLED:
                exposure[direction] += trade.matched_cost_usd
        return exposure

    def _fallback_token_quote(self, token_id: str, token_price: float) -> TokenQuote:
        tick_size = 0.01
        best_bid = self._round_price_to_tick(token_price - self._maker_offset_ticks * tick_size, tick_size)
        best_ask = self._round_price_to_tick(token_price + tick_size, tick_size)
        if best_ask <= best_bid:
            best_ask = min(1.0 - tick_size, round(best_bid + tick_size, 6))
        return TokenQuote(
            token_id=token_id,
            best_bid=max(tick_size, best_bid),
            best_ask=max(best_bid, best_ask),
            spread=max(tick_size, best_ask - best_bid),
            tick_size=tick_size,
        )

    def _resolve_token_quote(
        self,
        token_id: str,
        token_price: float,
        market: CryptoMarket | None = None,
        *,
        direction: str = "",
    ) -> TokenQuote:
        if self._dry_run and market is not None:
            if direction.upper() == "UP" and (market.up_best_bid > 0 or market.up_best_ask > 0):
                tick_size = max(market.up_tick_size, 0.001)
                spread = market.up_spread if market.up_spread > 0 else max(0.0, market.up_best_ask - market.up_best_bid)
                return TokenQuote(token_id, market.up_best_bid, market.up_best_ask, spread, tick_size)
            if direction.upper() == "DOWN" and (market.down_best_bid > 0 or market.down_best_ask > 0):
                tick_size = max(market.down_tick_size, 0.001)
                spread = market.down_spread if market.down_spread > 0 else max(0.0, market.down_best_ask - market.down_best_bid)
                return TokenQuote(token_id, market.down_best_bid, market.down_best_ask, spread, tick_size)

        if not self._dry_run:
            try:
                clob = self._ensure_clob()
                snapshot = clob.get_book_snapshot(token_id)
                tick_size = max(snapshot.tick_size, 0.001)
                best_bid = max(0.0, snapshot.best_bid)
                best_ask = max(0.0, snapshot.best_ask)
                if best_bid > 0 or best_ask > 0:
                    spread = snapshot.spread if snapshot.spread > 0 else max(tick_size, best_ask - best_bid)
                    return TokenQuote(
                        token_id=token_id,
                        best_bid=best_bid,
                        best_ask=best_ask,
                        spread=max(0.0, spread),
                        tick_size=tick_size,
                    )
            except Exception as exc:
                logger.debug(f"CLOB book snapshot failed for {token_id}: {exc}")
        return self._fallback_token_quote(token_id, token_price)

    async def send_heartbeat(self, force: bool = False) -> bool:
        if self._dry_run:
            return False
        pending = [trade for trade in self._orders if trade.status == OrderStatus.PENDING]
        if not pending:
            self._heartbeat_id = ""
            return False
        now = time.time()
        if not force and now - self._last_clob_heartbeat < 5.0:
            return False
        clob = self._ensure_clob()
        response = clob.post_heartbeat(self._heartbeat_id or None)
        new_heartbeat_id = self._field(response, "heartbeat_id", "heartbeatId", default="")
        if new_heartbeat_id:
            self._heartbeat_id = str(new_heartbeat_id)
        self._last_clob_heartbeat = now
        return True

    def _stale_order_reason(self, trade: TradeResult, signal: Signal | None) -> str:
        if signal is None:
            return "signal unavailable"
        if signal.direction.value != trade.direction:
            return "signal reversed"
        plan = self.evaluate_signal(signal, count_skips=False, enforce_live_limits=False)
        if plan is None:
            return "edge unavailable"
        if plan.token_id != trade.token_id:
            return "token changed"
        tick = max(trade.tick_size_at_submit, plan.tick_size_at_submit, 0.001)
        if abs(plan.price - trade.price) >= tick - 1e-9:
            return f"quote moved from {trade.price:.3f} to {plan.price:.3f}"
        return ""

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

    def today_live_usage(self) -> tuple[int, float]:
        if self._db is not None:
            usage = get_live_daily_usage(self._db)
            return int(usage["orders"]), float(usage["submitted_notional"])

        now = time.localtime()
        orders = 0
        submitted_notional = 0.0
        for trade in self._orders:
            if trade.is_paper:
                continue
            ts = time.localtime(trade.timestamp)
            if (ts.tm_year, ts.tm_yday) != (now.tm_year, now.tm_yday):
                continue
            orders += 1
            submitted_notional += trade.cost_usd
        return orders, submitted_notional

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
                condition_id=trade.market.condition_id if trade.market else "",
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
                fill_prob=trade.expected_fill_ratio,
                fill_lower_bound=trade.fill_ratio_lower_bound,
                fill_confidence=trade.fill_confidence,
                fill_effective_samples=trade.fill_effective_samples,
                fill_source=trade.fill_source,
                filled_ev_usd=trade.filled_ev,
                expected_value_usd=trade.submitted_ev,
                taker_fee_avoided=trade.taker_fee_avoided,
                expiration_ts=trade.expiration_ts,
                secs_remaining_at_submit=trade.secs_remaining_at_submit,
                liquidity_at_submit=trade.liquidity_at_submit,
                spread_at_submit=trade.spread_at_submit,
                queue_ticks_at_submit=trade.queue_ticks_at_submit,
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

    def bootstrap_wallet_orders(self):
        """Cross-check CLOB open orders against DB to detect orphaned orders."""
        if self._dry_run:
            return
        try:
            clob = self._ensure_clob()
            open_orders = clob.get_open_orders() or []
        except Exception as exc:
            logger.warning(f"Wallet order cross-check failed: {exc}")
            return

        db_order_ids = set()
        if self._db is not None:
            for row in get_pending_trades(self._db):
                oid = str(row.get("order_id", "") or "")
                if oid:
                    db_order_ids.add(oid)
        mem_order_ids = {t.order_id for t in self._orders if t.order_id}

        orphan_count = 0
        for order in open_orders:
            order_id = self._extract_order_id(order)
            if not order_id:
                continue
            if order_id in db_order_ids or order_id in mem_order_ids:
                continue

            orphan_count += 1
            logger.warning(f"Orphaned CLOB order detected: {order_id} (not in DB)")
            try:
                clob.cancel_order(order_id)
                logger.info(f"Cancelled orphaned order: {order_id}")
            except Exception as exc:
                logger.warning(f"Failed to cancel orphan {order_id}: {exc}")

        if orphan_count > 0:
            logger.warning(f"Found and cancelled {orphan_count} orphaned CLOB order(s)")

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
                binance_symbol=self._asset_to_binance_symbol(str(row.get("asset", "") or "")),
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
                expected_fill_ratio=float(row.get("fill_prob", 0) or 0),
                fill_ratio_lower_bound=float(row.get("fill_lower_bound", row.get("fill_prob", 0)) or 0),
                fill_confidence=float(row.get("fill_confidence", 0) or 0),
                fill_effective_samples=float(row.get("fill_effective_samples", 0) or 0),
                fill_source=str(row.get("fill_source", "") or ""),
                filled_ev=float(row.get("filled_ev_usd", row.get("expected_value_usd", 0)) or 0),
                submitted_ev=float(row.get("expected_value_usd", 0) or 0),
                taker_fee_avoided=float(row.get("taker_fee_avoided", 0) or 0),
                expiration_ts=int(row.get("expiration_ts", 0) or 0),
                secs_remaining_at_submit=float(row.get("secs_remaining_at_submit", 0) or 0),
                liquidity_at_submit=float(row.get("liquidity_at_submit", 0) or 0),
                spread_at_submit=float(row.get("spread_at_submit", 0) or 0),
                queue_ticks_at_submit=float(row.get("queue_ticks_at_submit", 0) or 0),
                tick_size_at_submit=0.01,
                db_id=int(row.get("id", 0) or 0),
                last_error=str(row.get("last_error", "") or ""),
                raw_status=status.value,
            )
            self._append_trade(trade)

    async def reconcile_pending_orders(self, force: bool = False, signal_lookup=None) -> int:
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
            elif payload is not None and signal_lookup is not None:
                try:
                    signal = signal_lookup(trade)
                except Exception as exc:
                    logger.debug(f"Signal lookup failed for {trade.order_id}: {exc}")
                    signal = None
                stale_reason = self._stale_order_reason(trade, signal)
                if stale_reason:
                    try:
                        clob.cancel_order(trade.order_id)
                        payload = {"status": "expired", "errorMsg": stale_reason}
                        logger.info(f"Cancelled stale order {trade.order_id}: {stale_reason}")
                    except Exception as exc:
                        logger.debug(f"Cancel stale order failed for {trade.order_id}: {exc}")

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

            if new_status == OrderStatus.EXPIRED and new_matched_shares <= 0:
                self._consecutive_expired += 1
                if (
                    self._max_consecutive_expired > 0
                    and self._consecutive_expired >= self._max_consecutive_expired
                ):
                    self._circuit_breaker_until = time.time() + self._circuit_breaker_cooldown
                    logger.warning(
                        f"Circuit breaker triggered: {self._consecutive_expired} consecutive "
                        f"expired orders, pausing for {self._circuit_breaker_cooldown:.0f}s"
                    )
            elif new_status == OrderStatus.FILLED and new_matched_shares > 0:
                self._consecutive_expired = 0

            logger.info(
                f"Order {trade.order_id} -> {trade.display_status} "
                f"filled={trade.matched_shares:.2f}/{trade.shares:.2f}"
            )

        return updated

    def evaluate_signal(
        self,
        signal: Signal,
        *,
        count_skips: bool = True,
        enforce_live_limits: bool = True,
    ) -> TradePlan | None:
        market = signal.market
        if market.liquidity < self._min_liquidity:
            if count_skips:
                self._skipped_low_liq += 1
            return None

        if self._circuit_breaker_until > 0:
            if time.time() < self._circuit_breaker_until:
                if count_skips:
                    self._skipped_circuit_breaker += 1
                return None
            self._circuit_breaker_until = 0.0
            self._consecutive_expired = 0

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

        quote = self._resolve_token_quote(token_id, token_price, market, direction=signal.direction.value)
        tick_size = max(quote.tick_size, 0.001)
        best_bid = quote.best_bid
        best_ask = quote.best_ask
        if best_bid > 0:
            q = best_bid
            reference_bid = best_bid
        else:
            q = token_price - self._maker_offset_ticks * tick_size
            reference_bid = q

        q = self._round_price_to_tick(q, tick_size)
        queue_ticks = max(0.0, (reference_bid - q) / tick_size)
        spread = quote.spread if quote.spread > 0 else max(tick_size, best_ask - best_bid)
        secs_remaining = market.secs_remaining

        shares = self._bet_size / q
        if shares < market.order_min_size:
            shares = float(market.order_min_size)
            effective_bet = shares * q
            if effective_bet > self._bet_size * self._max_bet_multiplier:
                if count_skips:
                    self._skipped_bet_size += 1
                return None
        else:
            effective_bet = self._bet_size

        haircut = self._time_adjusted_haircut(secs_remaining)
        p_raw = signal.win_prob
        p = max(0.0, p_raw - haircut)
        if p <= q:
            if count_skips:
                self._skipped_no_edge += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"p={p:.3f} <= q={q:.3f} after {haircut:.1%} haircut"
            )
            return None

        if not self._dry_run and enforce_live_limits:
            live_orders_today, live_notional_today = self.today_live_usage()
            if self._max_live_orders_per_day > 0 and live_orders_today >= self._max_live_orders_per_day:
                if count_skips:
                    self._skipped_live_limits += 1
                logger.warning(
                    f"Skip live trade: daily order limit reached "
                    f"({live_orders_today}/{self._max_live_orders_per_day})"
                )
                return None
            if (
                self._max_live_notional_usd_per_day > 0
                and live_notional_today + effective_bet > self._max_live_notional_usd_per_day + 1e-9
            ):
                if count_skips:
                    self._skipped_live_limits += 1
                logger.warning(
                    f"Skip live trade: daily notional cap reached "
                    f"(${live_notional_today:.2f} + ${effective_bet:.2f} > "
                    f"${self._max_live_notional_usd_per_day:.2f})"
                )
                return None

        if self._max_directional_exposure > 0:
            exposure = self._active_directional_exposure()
            if exposure.get(signal.direction.value, 0.0) + effective_bet > self._max_directional_exposure:
                if count_skips:
                    self._skipped_live_limits += 1
                logger.debug(
                    f"Skip {signal.asset} {signal.direction.value}: "
                    f"directional exposure ${exposure.get(signal.direction.value, 0.0):.2f} "
                    f"+ ${effective_bet:.2f} > cap ${self._max_directional_exposure:.2f}"
                )
                return None

        fill_estimate = self._estimate_fill_ratio(
            asset=signal.asset,
            market=market,
            quote_price=q,
            queue_ticks=queue_ticks,
            spread=spread,
        )

        fill_discount = self._fill_adverse_coeff * fill_estimate.conservative_fill_ratio
        p_conditional = max(0.0, p - fill_discount)
        if p_conditional <= q:
            if count_skips:
                self._skipped_no_edge += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"p_cond={p_conditional:.3f} <= q={q:.3f} after fill-adverse discount"
            )
            return None

        filled_ev = effective_bet * (p_conditional / q - 1)
        submitted_ev = filled_ev * fill_estimate.conservative_fill_ratio
        taker_fee = calc_taker_fee(shares, q, market.fee_rate)

        if submitted_ev < self._min_ev:
            if count_skips:
                self._skipped_low_ev += 1
            logger.debug(
                f"Skip {signal.asset} {signal.direction.value}: "
                f"submitted_EV ${submitted_ev:.2f} < ${self._min_ev:.2f} "
                f"(filled_EV=${filled_ev:.2f}, "
                f"fill~{fill_estimate.expected_fill_ratio:.1%}/lb {fill_estimate.conservative_fill_ratio:.1%}, "
                f"p_cond={p_conditional:.3f}, q={q:.2f})"
            )
            return None

        expiration = int(market.end_time) + 60 if market.end_time > 0 else 0
        return TradePlan(
            signal=signal,
            market=market,
            binance_symbol=signal.binance_symbol,
            direction=signal.direction.value,
            token_side=token_side,
            token_id=token_id,
            price=q,
            shares=shares,
            cost_usd=effective_bet,
            win_prob=p_conditional,
            expected_fill_ratio=fill_estimate.expected_fill_ratio,
            fill_ratio_lower_bound=fill_estimate.conservative_fill_ratio,
            fill_confidence=fill_estimate.confidence,
            fill_effective_samples=fill_estimate.effective_samples,
            fill_source=fill_estimate.source,
            filled_ev=filled_ev,
            submitted_ev=submitted_ev,
            taker_fee_avoided=taker_fee,
            expiration_ts=expiration,
            secs_remaining_at_submit=secs_remaining,
            liquidity_at_submit=market.liquidity,
            spread_at_submit=spread,
            queue_ticks_at_submit=queue_ticks,
            tick_size_at_submit=tick_size,
        )

    async def execute(self, signal: Signal) -> TradeResult | None:
        plan = self.evaluate_signal(signal)
        if plan is None:
            return None

        async with self._execute_lock:
            return await self._execute_plan(signal, plan)

    async def _execute_plan(self, signal: Signal, plan: TradePlan) -> TradeResult | None:
        market = plan.market
        raw_response: Any = {}
        last_error = ""

        if self._dry_run:
            order_id = f"paper-{len(self._orders) + 1}"
            alpha = max(0.3, plan.expected_fill_ratio * 2)
            beta_param = max(0.3, (1 - plan.expected_fill_ratio) * 2)
            sim_fill_ratio = random.betavariate(alpha, beta_param)
            if sim_fill_ratio < 0.01:
                sim_fill_ratio = 0.0
            matched_shares = round(plan.shares * sim_fill_ratio, 6)
            matched_cost_usd = round(matched_shares * plan.price, 6)
            status = OrderStatus.FILLED if matched_shares > 0 else OrderStatus.EXPIRED
            logger.info(
                f"[PAPER] {signal.asset} {signal.direction.value} -> buy {plan.token_side} "
                f"@ ${plan.price:.2f} x {plan.shares:.1f} = ${plan.cost_usd:.2f} | "
                f"p_adj={plan.win_prob:.1%} fill~{plan.expected_fill_ratio:.1%}/lb {plan.fill_ratio_lower_bound:.1%} "
                f"fEV=${plan.filled_ev:.2f} sEV=${plan.submitted_ev:.2f} "
                f"sim_fill={sim_fill_ratio:.0%}"
            )
        else:
            try:
                clob = self._ensure_clob()
                raw_response = clob.place_limit_order(
                    token_id=plan.token_id,
                    side="BUY",
                    price=plan.price,
                    size=round(plan.shares, 2),
                    tick_size=self._format_tick_size(plan.tick_size_at_submit),
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
                    f"p={plan.win_prob:.1%} fill~{plan.expected_fill_ratio:.1%}/lb {plan.fill_ratio_lower_bound:.1%} "
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
            binance_symbol=plan.binance_symbol,
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
            expected_fill_ratio=plan.expected_fill_ratio,
            fill_ratio_lower_bound=plan.fill_ratio_lower_bound,
            fill_confidence=plan.fill_confidence,
            fill_effective_samples=plan.fill_effective_samples,
            fill_source=plan.fill_source,
            filled_ev=plan.filled_ev,
            submitted_ev=plan.submitted_ev,
            taker_fee_avoided=plan.taker_fee_avoided,
            expiration_ts=plan.expiration_ts,
            secs_remaining_at_submit=plan.secs_remaining_at_submit,
            liquidity_at_submit=plan.liquidity_at_submit,
            spread_at_submit=plan.spread_at_submit,
            queue_ticks_at_submit=plan.queue_ticks_at_submit,
            tick_size_at_submit=plan.tick_size_at_submit,
            last_error=last_error,
            raw_status=status.value if self._dry_run else self._extract_status(raw_response),
        )
        self._append_trade(trade)
        self._persist_trade(trade, raw_data=raw_response)
        return trade
