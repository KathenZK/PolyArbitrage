from __future__ import annotations

import asyncio
import time
import unittest

from src.data.market_registry import CryptoMarket, MarketRegistry


class DummyGamma:
    def __init__(self, metadata: dict | None = None, trades: list[dict] | None = None):
        self.metadata = metadata or {}
        self.trades = trades or []
        self.urls: list[str] = []

    async def get_event_page_metadata(self, slug: str):
        return dict(self.metadata)

    async def _ensure_session(self):
        return self

    def get(self, url: str, params=None, timeout=None):
        self.urls.append(url)
        return _DummyResponse(self.trades)


class _DummyResponse:
    def __init__(self, payload):
        self.payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        return None

    async def json(self):
        return self.payload


def build_market(event_start: int = 1_700_000_000) -> CryptoMarket:
    return CryptoMarket(
        market_id="m1",
        question="Will BTC finish above the opening price?",
        slug="btc-updown-15m-123",
        asset="btc",
        binance_symbol="btcusdt",
        up_token_id="up",
        down_token_id="down",
        up_price=0.52,
        down_price=0.48,
        best_bid=0.51,
        best_ask=0.53,
        event_start=event_start,
        end_time=event_start + 900,
        liquidity=5000,
    )


class MarketRegistryTests(unittest.TestCase):
    def test_record_opening_price_ignores_pre_window_tick(self):
        gamma = DummyGamma()
        registry = MarketRegistry(gamma, assets=["btc"])
        market = build_market()
        registry._markets["btcusdt"] = market

        registry.record_opening_price("btcusdt", 100.0, market.event_start - 50)

        self.assertEqual(market.opening_price, 0.0)

    def test_refresh_official_metadata_sets_binance_reference_only_with_official_prices(self):
        gamma = DummyGamma(metadata={})
        registry = MarketRegistry(gamma, assets=["btc"])
        market = build_market()
        registry._markets["btcusdt"] = market
        registry.buffer_tick("btcusdt", 101.5, market.event_start + 1)

        asyncio.run(registry._refresh_official_metadata(market))

        self.assertEqual(market.official_opening_price, 0.0)
        self.assertEqual(market.official_current_price, 0.0)
        self.assertEqual(market.official_binance_ref_price, 0.0)

    def test_buffer_tick_backfills_missing_official_binance_reference(self):
        gamma = DummyGamma()
        registry = MarketRegistry(gamma, assets=["btc"])
        market = build_market(event_start=time.time() - 30)
        market.official_opening_price = 100.0
        market.official_current_price = 101.0
        market.official_price_updated_at = time.time()
        registry._markets["btcusdt"] = market

        registry.buffer_tick("btcusdt", 101.25, market.event_start + 31)

        self.assertAlmostEqual(market.official_binance_ref_price, 101.25, places=6)
        self.assertGreater(market.official_binance_ref_ts, 0.0)

    def test_apply_chainlink_price_sets_official_current_and_reference(self):
        gamma = DummyGamma()
        registry = MarketRegistry(gamma, assets=["btc"])
        market = build_market(event_start=time.time() - 30)
        market.official_opening_price = 100.0
        registry._markets["btcusdt"] = market
        registry.buffer_tick("btcusdt", 101.75, market.event_start + 31)

        registry.apply_chainlink_price("btc/usd", 101.6, market.event_start + 32)

        self.assertAlmostEqual(market.official_current_price, 101.6, places=6)
        self.assertAlmostEqual(market.official_binance_ref_price, 101.75, places=6)
        self.assertAlmostEqual(market.official_binance_ref_ts, market.event_start + 32, places=6)

    def test_backfill_opening_price_from_binance_aggtrades(self):
        event_start = time.time() - 5
        trades = [
            {"T": int((event_start + 8.0) * 1000), "p": "100.25"},
            {"T": int((event_start + 0.8) * 1000), "p": "99.95"},
        ]
        gamma = DummyGamma(trades=trades)
        registry = MarketRegistry(gamma, assets=["btc"])
        market = build_market(event_start=event_start)
        registry._markets["btcusdt"] = market

        asyncio.run(registry._backfill_opening_price(market))

        self.assertAlmostEqual(market.opening_price, 99.95, places=6)
        self.assertIn("https://api.binance.com/api/v3/aggTrades", gamma.urls)


if __name__ == "__main__":
    unittest.main()
