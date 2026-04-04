from __future__ import annotations

import json
import unittest

from src.data.polymarket_rtds import PolymarketRTDSStream


class PolymarketRTDSTests(unittest.TestCase):
    def test_subscription_payload_uses_chainlink_symbol_filters(self):
        stream = PolymarketRTDSStream(
            chainlink_symbols=["btc/usd", "eth/usd"],
            on_chainlink_price=lambda update: None,
        )

        payload = stream._subscription_payload()
        self.assertEqual(payload["action"], "subscribe")
        self.assertEqual(len(payload["subscriptions"]), 1)
        sub = payload["subscriptions"][0]
        self.assertEqual(sub["topic"], "crypto_prices_chainlink")
        self.assertEqual(sub["type"], "*")
        self.assertEqual(sub["filters"], "")

    def test_parse_update_supports_chainlink_price_messages(self):
        message = {
            "topic": "crypto_prices_chainlink",
            "type": "update",
            "timestamp": 1753314088421,
            "payload": {
                "symbol": "btc/usd",
                "timestamp": 1753314088395,
                "value": 67234.50,
            },
        }

        update = PolymarketRTDSStream._parse_update(message)

        self.assertIsNotNone(update)
        assert update is not None
        self.assertEqual(update.topic, "crypto_prices_chainlink")
        self.assertEqual(update.symbol, "btc/usd")
        self.assertAlmostEqual(update.timestamp, 1753314088.395, places=6)
        self.assertAlmostEqual(update.value, 67234.50, places=6)

    def test_parse_update_supports_chainlink_subscribe_snapshot(self):
        message = {
            "topic": "crypto_prices",
            "type": "subscribe",
            "timestamp": 1775284821387,
            "payload": {
                "symbol": "sol/usd",
                "data": [
                    {"timestamp": 1775284819000, "value": 80.24823360572144},
                    {"timestamp": 1775284820000, "value": 80.24823297117649},
                ],
            },
        }

        update = PolymarketRTDSStream._parse_update(message)

        self.assertIsNotNone(update)
        assert update is not None
        self.assertEqual(update.topic, "crypto_prices_chainlink")
        self.assertEqual(update.symbol, "sol/usd")
        self.assertAlmostEqual(update.timestamp, 1775284820.0, places=6)
        self.assertAlmostEqual(update.value, 80.24823297117649, places=6)


if __name__ == "__main__":
    unittest.main()
