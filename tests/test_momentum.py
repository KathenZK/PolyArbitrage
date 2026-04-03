from __future__ import annotations

import time
import unittest

from src.data.market_registry import CryptoMarket
from src.strategies.momentum import PriceComparator


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


if __name__ == "__main__":
    unittest.main()
