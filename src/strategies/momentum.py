"""Price-vs-opening comparator with Brownian motion probability model.

Estimates the true probability that the price will remain above (or below)
the opening price at settlement, using:

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


class PriceComparator:
    """Generates signals with estimated win probability."""

    def __init__(
        self,
        registry: MarketRegistry,
        threshold_pct: float = 0.003,
        min_secs_remaining: float = 30,
        min_secs_elapsed: float = 30,
        annual_vols: dict[str, float] | None = None,
    ):
        self._registry = registry
        self._threshold = threshold_pct
        self._min_remaining = min_secs_remaining
        self._min_elapsed = min_secs_elapsed
        self._vols = annual_vols or DEFAULT_ANNUAL_VOL

    def check(self, binance_symbol: str, price: float, timestamp: float) -> Signal | None:
        market = self._registry.get_market(binance_symbol)
        if not market:
            return None

        if not market.has_opening_price:
            return None

        remaining = market.secs_remaining
        if remaining < self._min_remaining:
            return None

        if market.secs_elapsed < self._min_elapsed:
            return None

        deviation = (price - market.opening_price) / market.opening_price

        if abs(deviation) < self._threshold:
            return None

        direction = Direction.UP if deviation > 0 else Direction.DOWN

        annual_vol = self._vols.get(binance_symbol, 0.70)
        win_prob = estimate_win_prob(abs(deviation), remaining, annual_vol)

        return Signal(
            asset=market.asset.upper(),
            binance_symbol=binance_symbol,
            direction=direction,
            current_price=price,
            opening_price=market.opening_price,
            deviation_pct=deviation,
            win_prob=win_prob,
            market=market,
            timestamp=timestamp,
        )
