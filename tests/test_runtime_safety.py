from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from src.data.market_registry import MarketRegistry, TICK_BUFFER_SECS, current_window_start
from src.main import Pipeline


class DummyGamma:
    async def check_geoblock(self):
        return {"blocked": False, "country": "TEST", "ip": "127.0.0.1"}

    async def close(self):
        return None


class PipelineSafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_geoblock_failure_is_fail_closed(self):
        config = {
            "strategy": {"symbols": ["btcusdt"]},
            "risk": {"dry_run": False, "require_live_arm": False},
            "alerts": {},
        }
        pipeline = Pipeline(config)

        class FailingGamma:
            async def check_geoblock(self):
                raise RuntimeError("dns down")

            async def close(self):
                return None

        pipeline.gamma = FailingGamma()
        self.assertFalse(await pipeline._check_geoblock())


class MarketRegistrySafetyTests(unittest.IsolatedAsyncioTestCase):
    async def test_tick_buffer_is_pruned_by_time_window(self):
        registry = MarketRegistry(DummyGamma(), assets=["btc"])
        registry.buffer_tick("btcusdt", 100000.0, 1000.0)
        registry.buffer_tick("btcusdt", 100100.0, 1000.0 + TICK_BUFFER_SECS + 1)

        buf = list(registry._tick_buffer["btcusdt"])
        self.assertEqual(len(buf), 1)
        self.assertEqual(buf[0][1], 100100.0)

    async def test_prefetched_event_is_reused_on_refresh(self):
        registry = MarketRegistry(DummyGamma(), assets=["btc"])
        ws = current_window_start()
        slug = f"btc-updown-15m-{ws}"
        end_date = (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat().replace("+00:00", "Z")
        registry._prefetched_events[slug] = {
            "slug": slug,
            "markets": [
                {
                    "id": "market-1",
                    "question": "Will BTC finish above the opening price?",
                    "active": True,
                    "closed": False,
                    "outcomes": "[\"Up\", \"Down\"]",
                    "clobTokenIds": "[\"token-up\", \"token-down\"]",
                    "outcomePrices": "[0.61, 0.39]",
                    "bestBid": 0.60,
                    "bestAsk": 0.62,
                    "endDate": end_date,
                    "volume": 0,
                    "liquidity": 5000,
                    "spread": 0.02,
                    "feesEnabled": True,
                    "feeSchedule": {"rate": 0.072},
                    "orderMinSize": 5,
                }
            ],
        }

        async def fail_fetch(slug: str):
            raise AssertionError(f"unexpected network fetch for {slug}")

        registry._fetch_event_by_slug = fail_fetch  # type: ignore[method-assign]
        await registry.refresh()

        market = registry.get_market("btcusdt")
        self.assertIsNotNone(market)
        self.assertEqual(market.slug, slug)


if __name__ == "__main__":
    unittest.main()
