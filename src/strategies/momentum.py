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

from dataclasses import dataclass
from enum import Enum
from math import erf, sqrt

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

    @staticmethod
    def _project_official_price(market: CryptoMarket, binance_price: float) -> float:
        if not market.has_official_calibration or binance_price <= 0:
            return 0.0

        ratio = market.official_current_price / market.official_binance_ref_price
        if ratio <= 0:
            return 0.0
        return binance_price * ratio

    def check(self, binance_symbol: str, price: float, timestamp: float) -> Signal | None:
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
        official_age = market.official_price_age
        if market.has_official_calibration and official_age <= self._official_max_age:
            projected_official_price = self._project_official_price(market, price)
            if projected_official_price > 0 and market.official_opening_price > 0:
                official_deviation = (
                    projected_official_price - market.official_opening_price
                ) / market.official_opening_price
                using_official = True

        if not using_official and self._require_official_source:
            return None

        effective_deviation = official_deviation if using_official else binance_deviation
        if abs(effective_deviation) < self._threshold:
            return None

        if using_official and market.has_opening_price:
            if (
                abs(binance_deviation) >= self._threshold
                and abs(official_deviation) >= self._threshold
                and binance_deviation * official_deviation < 0
            ):
                return None
            if abs(binance_deviation - official_deviation) > self._max_source_divergence:
                return None

        direction = Direction.UP if effective_deviation > 0 else Direction.DOWN

        annual_vol = self._vols.get(binance_symbol, 0.70)
        win_prob = estimate_win_prob(abs(effective_deviation), remaining, annual_vol)
        source_gap = abs(binance_deviation - official_deviation) if using_official and market.has_opening_price else 0.0
        if using_official and source_gap > 0:
            win_prob = max(0.50, win_prob - min(0.20, source_gap * self._source_gap_penalty_mult))

        opening_price = market.official_opening_price if using_official else market.opening_price
        current_price = projected_official_price if using_official else price

        return Signal(
            asset=market.asset.upper(),
            binance_symbol=binance_symbol,
            direction=direction,
            current_price=current_price,
            opening_price=opening_price,
            deviation_pct=effective_deviation,
            win_prob=win_prob,
            market=market,
            timestamp=timestamp,
            price_source="dual_calibrated" if using_official else "binance_only",
            binance_deviation_pct=binance_deviation,
            official_deviation_pct=official_deviation,
            official_opening_price=market.official_opening_price,
            official_current_price=market.official_current_price,
            projected_official_price=projected_official_price,
            source_gap_pct=source_gap,
        )
