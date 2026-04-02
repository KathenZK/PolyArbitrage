from __future__ import annotations

import sys
import types
import unittest

from src.data.polymarket_client import PolymarketCLOBClient, PolymarketGammaClient


class LevelObj:
    def __init__(self, price: str, size: str):
        self.price = price
        self.size = size


class BookObj:
    def __init__(self, bids, asks):
        self.bids = bids
        self.asks = asks


class PolymarketClientTests(unittest.TestCase):
    def test_event_page_metadata_parser_extracts_price_to_beat(self):
        html = """
        <html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{"data":[
            {"slug":"btc-updown-15m-123","eventMetadata":{"finalPrice":66524.33736,"priceToBeat":66569.02175162079}},
            {"slug":"btc-updown-15m-456","eventMetadata":{"finalPrice":70000.0,"priceToBeat":69950.0}}
        ]}}}
        </script>
        </body></html>
        """
        metadata = PolymarketGammaClient._parse_event_page_metadata("btc-updown-15m-123", html)
        self.assertAlmostEqual(metadata["official_current_price"], 66524.33736, places=6)
        self.assertAlmostEqual(metadata["official_opening_price"], 66569.02175162079, places=6)
        self.assertGreater(metadata["fetched_at"], 0)

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
        snapshot = client.get_book_snapshot("token")
        self.assertEqual(snapshot.best_bid, 0.61)
        self.assertEqual(snapshot.best_ask, 0.63)
        self.assertAlmostEqual(snapshot.spread, 0.02, places=6)
        self.assertEqual(snapshot.tick_size, 0.01)

    def test_orderbook_helpers_support_dicts(self):
        client = PolymarketCLOBClient("test-key")
        client.get_orderbook = lambda token_id: {
            "bids": [{"price": "0.48", "size": "5"}],
            "asks": [{"price": "0.51", "size": "7"}],
            "tick_size": "0.001",
        }

        self.assertEqual(client.get_best_bid("token"), 0.48)
        self.assertEqual(client.get_best_ask("token"), 0.51)
        bid_depth, ask_depth = client.get_book_depth("token")
        self.assertAlmostEqual(bid_depth, 2.4, places=6)
        self.assertAlmostEqual(ask_depth, 3.57, places=6)
        snapshot = client.get_book_snapshot("token")
        self.assertEqual(snapshot.tick_size, 0.001)

    def test_client_init_passes_signature_type_funder_and_api_creds(self):
        events: list[tuple[str, object]] = []

        class FakeClobClient:
            def __init__(self, **kwargs):
                events.append(("init", kwargs))

            def set_api_creds(self, creds):
                events.append(("set_api_creds", creds))

            def create_or_derive_api_creds(self):
                events.append(("derive", None))
                return {"key": "derived", "secret": "derived", "passphrase": "derived"}

            def post_heartbeat(self, heartbeat_id):
                events.append(("heartbeat", heartbeat_id))
                return {"heartbeat_id": "hb-1"}

        fake_client_mod = types.ModuleType("py_clob_client.client")
        fake_client_mod.ClobClient = FakeClobClient
        fake_root_mod = types.ModuleType("py_clob_client")
        fake_root_mod.client = fake_client_mod

        original_root = sys.modules.get("py_clob_client")
        original_client = sys.modules.get("py_clob_client.client")
        sys.modules["py_clob_client"] = fake_root_mod
        sys.modules["py_clob_client.client"] = fake_client_mod
        try:
            client = PolymarketCLOBClient(
                "test-key",
                signature_type=0,
                funder="0xFunder",
                api_key="key123",
                api_secret="secret123",
                api_passphrase="pass123",
            )
            client._ensure_client()
        finally:
            if original_root is None:
                sys.modules.pop("py_clob_client", None)
            else:
                sys.modules["py_clob_client"] = original_root
            if original_client is None:
                sys.modules.pop("py_clob_client.client", None)
            else:
                sys.modules["py_clob_client.client"] = original_client

        init_event = next(payload for kind, payload in events if kind == "init")
        self.assertEqual(init_event["host"], "https://clob.polymarket.com")
        self.assertEqual(init_event["chain_id"], 137)
        self.assertEqual(init_event["key"], "test-key")
        self.assertEqual(init_event["signature_type"], 0)
        self.assertEqual(init_event["funder"], "0xFunder")

        self.assertIn(("set_api_creds", {"key": "key123", "secret": "secret123", "passphrase": "pass123"}), events)
        self.assertFalse(any(kind == "derive" for kind, _ in events))
        self.assertEqual(client.post_heartbeat("hb-0")["heartbeat_id"], "hb-1")
        self.assertIn(("heartbeat", "hb-0"), events)


if __name__ == "__main__":
    unittest.main()
