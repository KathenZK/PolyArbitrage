"""Offline replay utilities for the latency-arb strategy.

Input rows can come from CSV or JSONL and should include at least:
    timestamp, symbol, binance_price, opening_price,
    up_price, down_price, best_bid, best_ask, liquidity, secs_remaining

To score realized PnL, provide either:
    settle_side    -> "UP" / "DOWN"
or
    final_price    -> compared against opening_price
"""

from __future__ import annotations

import csv
import json
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from src.data.market_registry import CryptoMarket, WINDOW_SECS
from src.strategies.executor import Executor, TradePlan
from src.strategies.momentum import DEFAULT_ANNUAL_VOL, Direction, Signal, estimate_win_prob
from src.strategies.signal_guard import SignalGuard


def _parse_float(row: dict[str, Any], key: str, default: float = 0.0) -> float:
    value = row.get(key, default)
    if value in (None, ""):
        return default
    return float(value)


def _parse_int(row: dict[str, Any], key: str, default: int = 0) -> int:
    value = row.get(key, default)
    if value in (None, ""):
        return default
    return int(float(value))


def _symbol(row: dict[str, Any]) -> str:
    return str(row.get("symbol") or row.get("binance_symbol") or "").lower()


def _asset(symbol: str) -> str:
    return symbol.replace("usdt", "").upper()


@dataclass
class ReplayTrade:
    timestamp: float
    asset: str
    direction: str
    quote_price: float
    win_prob: float
    fill_prob: float
    filled_ev: float
    submitted_ev: float
    realized_filled_pnl: float | None
    realized_submitted_pnl: float | None


@dataclass
class ReplaySummary:
    rows: int
    signals: int
    trades: int
    expected_submitted_ev: float
    expected_filled_ev: float
    realized_filled_pnl: float
    realized_submitted_pnl: float
    avg_fill_prob: float
    by_asset: dict[str, dict[str, float]]
    trade_log: list[ReplayTrade]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["trade_log"] = [asdict(t) for t in self.trade_log]
        return payload


def load_replay_rows(path: str | Path) -> list[dict[str, Any]]:
    file_path = Path(path)
    if file_path.suffix.lower() == ".jsonl":
        rows: list[dict[str, Any]] = []
        with file_path.open() as f:
            for line in f:
                line = line.strip()
                if line:
                    rows.append(json.loads(line))
        return rows

    with file_path.open(newline="") as f:
        return list(csv.DictReader(f))


def _build_market(row: dict[str, Any]) -> CryptoMarket:
    symbol = _symbol(row)
    asset = _asset(symbol).lower()
    now = time.time()
    timestamp = _parse_float(row, "timestamp", now)
    secs_remaining = _parse_float(row, "secs_remaining", 0)
    secs_elapsed = _parse_float(row, "secs_elapsed", max(0.0, WINDOW_SECS - secs_remaining))
    window_start = _parse_int(row, "window_start", int(timestamp) - int(secs_elapsed))
    end_time = _parse_float(row, "end_time", timestamp + secs_remaining)
    if end_time <= timestamp:
        end_time = now + secs_remaining

    event_start = _parse_int(row, "event_start", int(now - secs_elapsed))
    return CryptoMarket(
        market_id=str(row.get("market_id") or row.get("id") or f"{asset}-{window_start}"),
        question=str(row.get("question") or f"Will {asset.upper()} end the window up?"),
        slug=str(row.get("market_slug") or f"{asset}-updown-15m-{window_start}"),
        asset=asset,
        binance_symbol=symbol,
        up_token_id=str(row.get("up_token_id") or "up-token"),
        down_token_id=str(row.get("down_token_id") or "down-token"),
        up_price=_parse_float(row, "up_price", 0.5),
        down_price=_parse_float(row, "down_price", 0.5),
        best_bid=_parse_float(row, "best_bid", 0.0),
        best_ask=_parse_float(row, "best_ask", 0.0),
        event_start=event_start,
        end_time=timestamp + secs_remaining if secs_remaining > 0 else end_time,
        opening_price=_parse_float(row, "opening_price", 0.0),
        volume=_parse_float(row, "volume", 0.0),
        liquidity=_parse_float(row, "liquidity", 0.0),
        spread=_parse_float(row, "spread", 0.0),
        fees_enabled=bool(row.get("fees_enabled", True)),
        fee_rate=_parse_float(row, "fee_rate", 0.072),
        order_min_size=_parse_int(row, "order_min_size", 5),
    )


def signal_from_row(
    row: dict[str, Any],
    *,
    threshold_pct: float,
    min_secs_remaining: float,
    min_secs_elapsed: float,
    annual_vols: dict[str, float] | None = None,
) -> Signal | None:
    market = _build_market(row)
    opening_price = _parse_float(row, "opening_price", market.opening_price)
    current_price = _parse_float(row, "binance_price", 0.0)
    if opening_price <= 0 or current_price <= 0:
        return None

    secs_remaining = _parse_float(row, "secs_remaining", market.secs_remaining)
    secs_elapsed = _parse_float(row, "secs_elapsed", market.secs_elapsed)
    if secs_remaining < min_secs_remaining or secs_elapsed < min_secs_elapsed:
        return None

    deviation = (current_price - opening_price) / opening_price
    if abs(deviation) < threshold_pct:
        return None

    direction = Direction.UP if deviation > 0 else Direction.DOWN
    symbol = _symbol(row)
    annual_vol = (annual_vols or DEFAULT_ANNUAL_VOL).get(symbol, 0.70)
    win_prob = estimate_win_prob(abs(deviation), secs_remaining, annual_vol)
    timestamp = _parse_float(row, "timestamp", time.time())

    market.opening_price = opening_price
    return Signal(
        asset=market.asset.upper(),
        binance_symbol=symbol,
        direction=direction,
        current_price=current_price,
        opening_price=opening_price,
        deviation_pct=deviation,
        win_prob=win_prob,
        market=market,
        timestamp=timestamp,
    )


def _settle_side(row: dict[str, Any], opening_price: float) -> str | None:
    raw = str(row.get("settle_side") or row.get("settlement_side") or "").strip().upper()
    if raw in {"UP", "DOWN"}:
        return raw

    final_price = row.get("final_price", row.get("settlement_price", ""))
    if final_price in (None, ""):
        return None
    return "UP" if float(final_price) >= opening_price else "DOWN"


def _filled_pnl(plan: TradePlan, settle_side: str | None) -> float | None:
    if settle_side is None:
        return None
    if settle_side == plan.direction:
        return plan.cost_usd * (1 / plan.price - 1)
    return -plan.cost_usd


def run_replay(rows: list[dict[str, Any]], config: dict[str, Any]) -> ReplaySummary:
    strat = config.get("strategy", {})
    annual_vols = {
        sym: strat[key]
        for sym in strat.get("symbols", [])
        for key in [f"annual_vol_{sym}"]
        if key in strat
    }

    executor = Executor(
        bet_size_usd=strat.get("bet_size_usd", 15),
        dry_run=True,
        min_liquidity=strat.get("min_liquidity", 1000),
        min_ev_usd=strat.get("min_ev_usd", 0.10),
        maker_offset_ticks=strat.get("maker_offset_ticks", 1),
        adverse_selection_haircut=strat.get("adverse_selection_haircut", 0.05),
        fill_rate_prior=strat.get("fill_rate_prior", 0.35),
        fill_min_samples=strat.get("fill_min_samples", 20),
        fill_lookback_hours=strat.get("fill_lookback_hours", 168),
    )
    guard = SignalGuard(cooldown_secs=strat.get("signal_cooldown_sec", 120))

    signals = 0
    trades: list[ReplayTrade] = []
    by_asset: dict[str, dict[str, float]] = {}

    for row in rows:
        signal = signal_from_row(
            row,
            threshold_pct=strat.get("edge_threshold_pct", 0.003),
            min_secs_remaining=strat.get("min_secs_remaining", 30),
            min_secs_elapsed=strat.get("min_secs_elapsed", 30),
            annual_vols=annual_vols if annual_vols else None,
        )
        if signal is None:
            continue

        signals += 1
        if not guard.should_trade(signal):
            continue

        plan = executor.evaluate_signal(signal)
        if plan is None:
            continue

        settle_side = _settle_side(row, signal.opening_price)
        realized_filled_pnl = _filled_pnl(plan, settle_side)
        actual_fill_ratio = row.get("actual_fill_ratio", row.get("fill_ratio", ""))
        if actual_fill_ratio in ("", None):
            fill_ratio = plan.fill_prob
        else:
            fill_ratio = float(actual_fill_ratio)
        realized_submitted_pnl = None
        if realized_filled_pnl is not None:
            realized_submitted_pnl = realized_filled_pnl * fill_ratio

        trade = ReplayTrade(
            timestamp=signal.timestamp,
            asset=signal.asset,
            direction=plan.direction,
            quote_price=plan.price,
            win_prob=plan.win_prob,
            fill_prob=plan.fill_prob,
            filled_ev=plan.filled_ev,
            submitted_ev=plan.submitted_ev,
            realized_filled_pnl=realized_filled_pnl,
            realized_submitted_pnl=realized_submitted_pnl,
        )
        trades.append(trade)

        asset_bucket = by_asset.setdefault(signal.asset, {"trades": 0.0, "submitted_ev": 0.0, "realized_submitted_pnl": 0.0})
        asset_bucket["trades"] += 1
        asset_bucket["submitted_ev"] += plan.submitted_ev
        if realized_submitted_pnl is not None:
            asset_bucket["realized_submitted_pnl"] += realized_submitted_pnl

    expected_submitted_ev = sum(t.submitted_ev for t in trades)
    expected_filled_ev = sum(t.filled_ev for t in trades)
    realized_filled_pnl = sum(t.realized_filled_pnl or 0.0 for t in trades)
    realized_submitted_pnl = sum(t.realized_submitted_pnl or 0.0 for t in trades)
    avg_fill_prob = sum(t.fill_prob for t in trades) / len(trades) if trades else 0.0

    return ReplaySummary(
        rows=len(rows),
        signals=signals,
        trades=len(trades),
        expected_submitted_ev=expected_submitted_ev,
        expected_filled_ev=expected_filled_ev,
        realized_filled_pnl=realized_filled_pnl,
        realized_submitted_pnl=realized_submitted_pnl,
        avg_fill_prob=avg_fill_prob,
        by_asset=by_asset,
        trade_log=trades,
    )
