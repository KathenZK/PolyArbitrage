from __future__ import annotations

import time
import unittest

from src.data.market_registry import CryptoMarket
from src.strategies.momentum import PriceComparator, estimate_win_prob


class _StubRegistry:
    def __init__(self, market: CryptoMarket):
        self._market = market
        self.in_transition = False

    def get_market(self, binance_symbol: str):
        if binance_symbol == self._market.binance_symbol:
            return self._market
        return None

    def realized_vol(self, binance_symbol: str) -> float:
        return 0.0


def _build_market() -> CryptoMarket:
    now = time.time()
    return CryptoMarket(
        market_id="m1",
        question="Will BTC finish above the opening price?",
        slug="btc-updown-15m-1",
        asset="btc",
        binance_symbol="btcusdt",
        up_token_id="up",
        down_token_id="down",
        up_price=0.54,
        down_price=0.46,
        best_bid=0.53,
        best_ask=0.55,
        event_start=int(now - 120),
        end_time=now + 600,
        opening_price=100000.0,
        liquidity=5000.0,
        official_opening_price=99950.0,
        official_current_price=0.0,
        official_binance_ref_price=0.0,
        official_price_updated_at=now - 3,
    )


class MomentumTests(unittest.TestCase):
    def test_comparator_uses_official_open_anchor_without_official_current(self):
        market = _build_market()
        registry = _StubRegistry(market)
        comparator = PriceComparator(
            registry=registry,
            threshold_pct=0.003,
            min_secs_remaining=30,
            min_secs_elapsed=30,
            require_official_source=True,
            official_max_age_secs=15,
        )

        estimate = comparator.estimate("btcusdt", 100500.0, time.time())

        self.assertIsNotNone(estimate)
        assert estimate is not None
        self.assertEqual(estimate.price_source, "official_anchor_fast_return")
        self.assertGreater(estimate.up_win_prob, 0.5)
        self.assertAlmostEqual(estimate.effective_deviation_pct, 0.005, places=6)


    def test_fat_tail_dampening_reduces_extreme_probabilities(self):
        p_undamped = estimate_win_prob(0.005, 300, 0.60)
        p_damped = estimate_win_prob(0.005, 300, 0.60, fat_tail_dampening=0.80)
        self.assertGreater(p_undamped, p_damped)
        self.assertGreater(p_damped, 0.5)
        expected = 0.5 + (p_undamped - 0.5) * 0.80
        self.assertAlmostEqual(p_damped, expected, places=6)

    def test_max_win_prob_caps_output(self):
        p = estimate_win_prob(0.02, 30, 0.60, max_win_prob=0.92)
        self.assertLessEqual(p, 0.92)

    def test_fat_tail_dampening_preserves_fifty_fifty(self):
        p = estimate_win_prob(0.0, 300, 0.60, fat_tail_dampening=0.80)
        self.assertAlmostEqual(p, 0.5, places=6)

    def test_comparator_passes_dampening_to_model(self):
        market = _build_market()
        registry = _StubRegistry(market)
        comp_full = PriceComparator(
            registry=registry, threshold_pct=0.001,
            fat_tail_dampening=1.0, max_win_prob=1.0,
        )
        comp_damped = PriceComparator(
            registry=registry, threshold_pct=0.001,
            fat_tail_dampening=0.80, max_win_prob=0.92,
        )
        est_full = comp_full.estimate("btcusdt", 100500.0, time.time())
        est_damped = comp_damped.estimate("btcusdt", 100500.0, time.time())
        self.assertIsNotNone(est_full)
        self.assertIsNotNone(est_damped)
        assert est_full is not None and est_damped is not None
        self.assertGreater(est_full.up_win_prob, est_damped.up_win_prob)


if __name__ == "__main__":
    unittest.main()
