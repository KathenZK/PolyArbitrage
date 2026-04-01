from __future__ import annotations

import unittest

from src.data.polymarket_client import PolymarketCLOBClient


class LevelObj:
    def __init__(self, price: str, size: str):
        self.price = price
        self.size = size


class BookObj:
    def __init__(self, bids, asks):
        self.bids = bids
        self.asks = asks


class PolymarketClientTests(unittest.TestCase):
    def test_orderbook_helpers_support_sdk_objects(self):
        client = PolymarketCLOBClient("test-key")
        client.get_orderbook = lambda token_id: BookObj(
            bids=[LevelObj("0.61", "10"), LevelObj("0.60", "8")],
            asks=[LevelObj("0.63", "12"), LevelObj("0.64", "4")],
        )

        self.assertEqual(client.get_best_bid("token"), 0.61)
        self.assertEqual(client.get_best_ask("token"), 0.63)
        bid_depth, ask_depth = client.get_book_depth("token", levels=2)
        self.assertAlmostEqual(bid_depth, 10.9, places=6)
        self.assertAlmostEqual(ask_depth, 10.12, places=6)

    def test_orderbook_helpers_support_dicts(self):
        client = PolymarketCLOBClient("test-key")
        client.get_orderbook = lambda token_id: {
            "bids": [{"price": "0.48", "size": "5"}],
            "asks": [{"price": "0.51", "size": "7"}],
        }

        self.assertEqual(client.get_best_bid("token"), 0.48)
        self.assertEqual(client.get_best_ask("token"), 0.51)
        bid_depth, ask_depth = client.get_book_depth("token")
        self.assertAlmostEqual(bid_depth, 2.4, places=6)
        self.assertAlmostEqual(ask_depth, 3.57, places=6)


if __name__ == "__main__":
    unittest.main()
