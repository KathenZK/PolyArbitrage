"""Dual-source comparator with a Brownian motion probability model.

The strategy trades a Polymarket event, not raw Binance spot. We therefore:
  - use Binance as the fast source for sub-second price moves
  - use Polymarket's Chainlink-derived `priceToBeat` and official current price
    snapshot as the settlement anchor

The probability model still uses a simple Brownian approximation:

    p ≈ Φ( |d| / (σ × √τ) )

where:
    d = current deviation from opening price
    σ = annualized volatility (asset-specific)
    τ = remaining time in years
    Φ = standard normal CDF

The Signal carries the estimated p so the executor can compute real EV:
    EV = B × (p / q - 1)
where q is the Polymarket entry price.
"""

from __future__ import annotations

import json
from enum import Enum
from math import erf, sqrt
from pathlib import Path
from dataclasses import dataclass

from src.data.market_registry import CryptoMarket, MarketRegistry

SECS_PER_YEAR = 365.25 * 24 * 3600

DEFAULT_ANNUAL_VOL: dict[str, float] = {
    "btcusdt": 0.60,
    "ethusdt": 0.75,
    "solusdt": 1.00,
    "xrpusdt": 1.10,
}


def normal_cdf(x: float) -> float:
    return 0.5 * (1.0 + erf(x / sqrt(2.0)))


def estimate_win_prob(
    deviation_abs: float,
    remaining_secs: float,
    annual_vol: float,
) -> float:
    """P(price stays on the same side of opening at settlement).

    Under zero-drift GBM, if current price is d% above opening with τ seconds
    remaining and annualized vol σ, the probability of finishing above opening is
    Φ(d / (σ × √τ_in_years)).
    """
    if remaining_secs <= 0:
        return 1.0 if deviation_abs > 0 else 0.5

    tau = remaining_secs / SECS_PER_YEAR
    sigma_remaining = annual_vol * sqrt(tau)

    if sigma_remaining <= 1e-12:
        return 1.0 if deviation_abs > 0 else 0.5

    z = deviation_abs / sigma_remaining
    return normal_cdf(z)


class Direction(str, Enum):
    UP = "UP"
    DOWN = "DOWN"


@dataclass
class Signal:
    asset: str
    binance_symbol: str
    direction: Direction
    current_price: float
    opening_price: float
    deviation_pct: float
    win_prob: float
    market: CryptoMarket
    timestamp: float
    price_source: str = "binance_only"
    binance_deviation_pct: float = 0.0
    official_deviation_pct: float = 0.0
    official_opening_price: float = 0.0
    official_current_price: float = 0.0
    projected_official_price: float = 0.0
    source_gap_pct: float = 0.0


@dataclass
class MarketEstimate:
    asset: str
    binance_symbol: str
    market: CryptoMarket
    timestamp: float
    current_price: float
    opening_price: float
    effective_deviation_pct: float
    price_source: str
    binance_deviation_pct: float
    official_deviation_pct: float
    official_opening_price: float
    official_current_price: float
    projected_official_price: float
    source_gap_pct: float
    up_win_prob: float
    down_win_prob: float
    calibration_source: str = "brownian"
    calibration_samples: int = 0


def deviation_bucket_label(deviation_abs: float) -> str:
    bounds = [0.003, 0.005, 0.0075, 0.01, 0.015, 0.02]
    for bound in bounds:
        if deviation_abs < bound:
            return f"<{bound:.4f}"
    return ">=0.0200"


def secs_bucket_label(secs_remaining: float) -> str:
    if secs_remaining < 60:
        return "<60"
    if secs_remaining < 180:
        return "60-180"
    if secs_remaining < 420:
        return "180-420"
    return ">=420"


def source_gap_bucket_label(source_gap: float) -> str:
    if source_gap < 0.001:
        return "<0.001"
    if source_gap < 0.0025:
        return "0.001-0.0025"
    if source_gap < 0.005:
        return "0.0025-0.005"
    return ">=0.005"


class ProbabilityCalibrator:
    """Bucketed empirical calibration for same-side settlement probability."""

    def __init__(
        self,
        path: str | None = None,
        *,
        min_samples: int = 50,
        prior_strength: float = 20.0,
    ):
        self._path = Path(path).expanduser() if path else None
        self._min_samples = min_samples
        self._prior_strength = prior_strength
        self._buckets: dict[str, dict[str, float]] = {}
        if self._path and self._path.exists():
            self._load()

    def _load(self):
        if not self._path:
            return
        try:
            payload = json.loads(self._path.read_text())
        except Exception:
            self._buckets = {}
            return
        raw = payload.get("buckets", {}) if isinstance(payload, dict) else {}
        self._buckets = {
            str(key): {
                "samples": float(value.get("samples", 0) or 0),
                "win_rate": float(value.get("win_rate", 0.5) or 0.5),
            }
            for key, value in raw.items()
            if isinstance(value, dict)
        }

    @staticmethod
    def bucket_key(*, asset: str, deviation_abs: float, secs_remaining: float, source_gap: float) -> str:
        return "|".join(
            [
                asset.upper(),
                secs_bucket_label(secs_remaining),
                deviation_bucket_label(deviation_abs),
                source_gap_bucket_label(source_gap),
            ]
        )

    def calibrate(
        self,
        *,
        asset: str,
        deviation_abs: float,
        secs_remaining: float,
        source_gap: float,
        model_prob: float,
    ) -> tuple[float, str, int]:
        if not self._buckets:
            return model_prob, "brownian", 0
        key = self.bucket_key(
            asset=asset,
            deviation_abs=deviation_abs,
            secs_remaining=secs_remaining,
            source_gap=source_gap,
        )
        row = self._buckets.get(key)
        if not row:
            return model_prob, "brownian", 0
        samples = int(row.get("samples", 0) or 0)
        if samples < self._min_samples:
            return model_prob, "brownian", samples
        empirical = float(row.get("win_rate", model_prob) or model_prob)
        weight = samples / (samples + self._prior_strength)
        blended = model_prob * (1.0 - weight) + empirical * weight
        return blended, "empirical_blend", samples


def calibrated_same_side_prob(
    *,
    asset: str,
    deviation_abs: float,
    secs_remaining: float,
    annual_vol: float,
    source_gap: float,
    source_gap_penalty_mult: float,
    calibrator: ProbabilityCalibrator | None = None,
) -> tuple[float, str, int]:
    win_prob = estimate_win_prob(deviation_abs, secs_remaining, annual_vol)
    if source_gap > 0:
        win_prob = max(0.50, win_prob - min(0.20, source_gap * source_gap_penalty_mult))
    if calibrator is None:
        return win_prob, "brownian", 0
    return calibrator.calibrate(
        asset=asset,
        deviation_abs=deviation_abs,
        secs_remaining=secs_remaining,
        source_gap=source_gap,
        model_prob=win_prob,
    )


class PriceComparator:
    """Generates signals with estimated win probability."""

    def __init__(
        self,
        registry: MarketRegistry,
        threshold_pct: float = 0.003,
        min_secs_remaining: float = 30,
        min_secs_elapsed: float = 30,
        annual_vols: dict[str, float] | None = None,
        require_official_source: bool = False,
        official_max_age_secs: float = 90,
        max_source_divergence_pct: float = 0.0025,
        source_gap_penalty_mult: float = 8.0,
        use_realized_vol: bool = False,
        prob_calibration_path: str | None = None,
        prob_calibration_min_samples: int = 50,
        prob_calibration_prior_strength: float = 20.0,
    ):
        self._registry = registry
        self._threshold = threshold_pct
        self._min_remaining = min_secs_remaining
        self._min_elapsed = min_secs_elapsed
        self._vols = annual_vols or DEFAULT_ANNUAL_VOL
        self._require_official_source = require_official_source
        self._official_max_age = official_max_age_secs
        self._max_source_divergence = max_source_divergence_pct
        self._source_gap_penalty_mult = source_gap_penalty_mult
        self._use_realized_vol = use_realized_vol
        self._calibrator = ProbabilityCalibrator(
            prob_calibration_path,
            min_samples=prob_calibration_min_samples,
            prior_strength=prob_calibration_prior_strength,
        )

    @staticmethod
    def _project_official_price(market: CryptoMarket, binance_price: float) -> float:
        if not market.has_official_calibration or binance_price <= 0:
            return 0.0

        ratio = market.official_current_price / market.official_binance_ref_price
        if ratio <= 0:
            return 0.0
        projected = binance_price * ratio

        cal_age = market.official_calibration_age
        if cal_age > 30:
            blend = min(1.0, (cal_age - 30) / 60.0)
            projected = projected * (1 - blend) + binance_price * blend
        return projected

    def estimate(self, binance_symbol: str, price: float, timestamp: float) -> MarketEstimate | None:
        if self._registry.in_transition:
            return None

        market = self._registry.get_market(binance_symbol)
        if not market:
            return None

        remaining = market.secs_remaining
        if remaining < self._min_remaining:
            return None

        if market.secs_elapsed < self._min_elapsed:
            return None

        binance_deviation = 0.0
        if market.has_opening_price and market.opening_price > 0:
            binance_deviation = (price - market.opening_price) / market.opening_price

        projected_official_price = 0.0
        official_deviation = 0.0
        using_official = False
        cal_age = market.official_calibration_age
        if market.has_official_calibration and cal_age <= self._official_max_age:
            projected_official_price = self._project_official_price(market, price)
            if projected_official_price > 0 and market.official_opening_price > 0:
                official_deviation = (
                    projected_official_price - market.official_opening_price
                ) / market.official_opening_price
                using_official = True

        if not using_official and self._require_official_source:
            return None

        effective_deviation = official_deviation if using_official else binance_deviation
        if using_official and market.has_opening_price:
            if (
                abs(binance_deviation) >= self._threshold
                and abs(official_deviation) >= self._threshold
                and binance_deviation * official_deviation < 0
            ):
                return None
            if abs(binance_deviation - official_deviation) > self._max_source_divergence:
                return None

        annual_vol = self._vols.get(binance_symbol, 0.70)
        if self._use_realized_vol:
            realized = self._registry.realized_vol(binance_symbol)
            if realized > 0.10:
                annual_vol = realized
        source_gap = abs(binance_deviation - official_deviation) if using_official and market.has_opening_price else 0.0
        same_side_prob, calibration_source, calibration_samples = calibrated_same_side_prob(
            asset=market.asset,
            deviation_abs=abs(effective_deviation),
            secs_remaining=remaining,
            annual_vol=annual_vol,
            source_gap=source_gap if using_official else 0.0,
            source_gap_penalty_mult=self._source_gap_penalty_mult,
            calibrator=self._calibrator,
        )
        if effective_deviation > 0:
            up_win_prob = same_side_prob
            down_win_prob = 1.0 - same_side_prob
        elif effective_deviation < 0:
            up_win_prob = 1.0 - same_side_prob
            down_win_prob = same_side_prob
        else:
            up_win_prob = down_win_prob = 0.5

        opening_price = market.official_opening_price if using_official else market.opening_price
        current_price = projected_official_price if using_official else price

        return MarketEstimate(
            asset=market.asset.upper(),
            binance_symbol=binance_symbol,
            market=market,
            timestamp=timestamp,
            current_price=current_price,
            opening_price=opening_price,
            effective_deviation_pct=effective_deviation,
            price_source="dual_calibrated" if using_official else "binance_only",
            binance_deviation_pct=binance_deviation,
            official_deviation_pct=official_deviation,
            official_opening_price=market.official_opening_price,
            official_current_price=market.official_current_price,
            projected_official_price=projected_official_price,
            source_gap_pct=source_gap,
            up_win_prob=up_win_prob,
            down_win_prob=down_win_prob,
            calibration_source=calibration_source,
            calibration_samples=calibration_samples,
        )

    def check(self, binance_symbol: str, price: float, timestamp: float) -> Signal | None:
        estimate = self.estimate(binance_symbol, price, timestamp)
        if estimate is None:
            return None
        if abs(estimate.effective_deviation_pct) < self._threshold:
            return None
        direction = Direction.UP if estimate.effective_deviation_pct > 0 else Direction.DOWN
        win_prob = estimate.up_win_prob if direction == Direction.UP else estimate.down_win_prob
        return Signal(
            asset=estimate.asset,
            binance_symbol=estimate.binance_symbol,
            direction=direction,
            current_price=estimate.current_price,
            opening_price=estimate.opening_price,
            deviation_pct=estimate.effective_deviation_pct,
            win_prob=win_prob,
            market=estimate.market,
            timestamp=estimate.timestamp,
            price_source=estimate.price_source,
            binance_deviation_pct=estimate.binance_deviation_pct,
            official_deviation_pct=estimate.official_deviation_pct,
            official_opening_price=estimate.official_opening_price,
            official_current_price=estimate.official_current_price,
            projected_official_price=estimate.projected_official_price,
            source_gap_pct=estimate.source_gap_pct,
        )
