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

    def test_event_page_metadata_parser_supports_dehydrated_state_event_query(self):
        html = """
        <html><body>
        <script id="__NEXT_DATA__" type="application/json" crossorigin="anonymous">
        {"props":{"pageProps":{
            "key":"[\\"btc-updown-15m-123\\"]",
            "dehydratedState":{"mutations":[],"queries":[
                {
                    "queryKey":["/api/event/slug","btc-updown-15m-123"],
                    "state":{"data":{
                        "slug":"btc-updown-15m-123",
                        "eventMetadata":{"finalPrice":66499.978,"priceToBeat":66620.11899999999}
                    }}
                }
            ]}
        }}}
        </script>
        </body></html>
        """
        metadata = PolymarketGammaClient._parse_event_page_metadata("btc-updown-15m-123", html)
        self.assertAlmostEqual(metadata["official_current_price"], 66499.978, places=6)
        self.assertAlmostEqual(metadata["official_opening_price"], 66620.11899999999, places=6)
        self.assertGreater(metadata["fetched_at"], 0)

    def test_event_page_metadata_parser_uses_crypto_prices_for_active_window(self):
        html = """
        <html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{
            "data":[
                {
                    "slug":"btc-updown-15m-1775197800",
                    "eventMetadata":{"priceToBeat":66624.16057626104}
                }
            ],
            "dehydratedState":{"queries":[
                {
                    "queryKey":["crypto-prices","price","BTC","2026-04-03T06:30:00Z","fifteen","2026-04-03T06:45:00Z"],
                    "state":{"data":{"openPrice":66624.16057626104,"closePrice":66731.98535437319}}
                },
                {
                    "queryKey":["/api/series","btc-up-or-down-15m"],
                    "state":{"data":[
                        {
                            "slug":"btc-updown-15m-1775137500",
                            "eventMetadata":{"finalPrice":66252.9077185263,"priceToBeat":65844.48}
                        }
                    ]}
                }
            ]}
        }}}
        </script>
        </body></html>
        """
        metadata = PolymarketGammaClient._parse_event_page_metadata("btc-updown-15m-1775197800", html)
        self.assertAlmostEqual(metadata["official_opening_price"], 66624.16057626104, places=6)
        self.assertAlmostEqual(metadata["official_current_price"], 66731.98535437319, places=6)
        self.assertGreater(metadata["fetched_at"], 0)

    def test_event_page_metadata_parser_uses_crypto_open_when_event_metadata_missing(self):
        html = """
        <html><body>
        <script id="__NEXT_DATA__" type="application/json">
        {"props":{"pageProps":{
            "dehydratedState":{"queries":[
                {
                    "queryKey":["/api/event/slug","btc-updown-15m-1775202300"],
                    "state":{"data":{"slug":"btc-updown-15m-1775202300","eventMetadata":null}}
                },
                {
                    "queryKey":["crypto-prices","price","BTC","2026-04-03T07:45:00Z","fifteen","2026-04-03T08:00:00Z"],
                    "state":{"data":{"openPrice":67079.95,"closePrice":null}}
                }
            ]}
        }}}
        </script>
        </body></html>
        """
        metadata = PolymarketGammaClient._parse_event_page_metadata("btc-updown-15m-1775202300", html)
        self.assertAlmostEqual(metadata["official_opening_price"], 67079.95, places=6)
        self.assertNotIn("official_current_price", metadata)
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
        self.assertAlmostEqual(snapshot.best_bid_size, 10.0, places=6)
        self.assertAlmostEqual(snapshot.best_ask_size, 12.0, places=6)
        self.assertAlmostEqual(snapshot.best_bid_notional, 6.1, places=6)
        self.assertAlmostEqual(snapshot.best_ask_notional, 7.56, places=6)
        self.assertAlmostEqual(snapshot.bid_depth_usd, 10.9, places=6)
        self.assertAlmostEqual(snapshot.ask_depth_usd, 10.12, places=6)

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
        self.assertAlmostEqual(snapshot.best_bid_notional, 2.4, places=6)
        self.assertAlmostEqual(snapshot.best_ask_notional, 3.57, places=6)

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

    def test_client_supports_public_level0_mode_without_private_key(self):
        events: list[tuple[str, object]] = []

        class FakeClobClient:
            def __init__(self, *args, **kwargs):
                events.append(("init", {"args": args, "kwargs": kwargs}))

        fake_client_mod = types.ModuleType("py_clob_client.client")
        fake_client_mod.ClobClient = FakeClobClient
        fake_root_mod = types.ModuleType("py_clob_client")
        fake_root_mod.client = fake_client_mod

        original_root = sys.modules.get("py_clob_client")
        original_client = sys.modules.get("py_clob_client.client")
        sys.modules["py_clob_client"] = fake_root_mod
        sys.modules["py_clob_client.client"] = fake_client_mod
        try:
            client = PolymarketCLOBClient("")
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
        self.assertEqual(init_event["args"], ("https://clob.polymarket.com",))
        self.assertEqual(init_event["kwargs"], {})

    def test_extract_resolved_truth_prefers_resolved_outcome_prices(self):
        event = {
            "slug": "btc-updown-15m-123",
            "markets": [
                {
                    "conditionId": "0xcond",
                    "active": False,
                    "closed": True,
                    "outcomes": "[\"Up\", \"Down\"]",
                    "outcomePrices": "[1,0]",
                }
            ],
        }
        metadata = {
            "official_opening_price": 100000.0,
            "official_current_price": 100200.0,
        }

        truth = PolymarketGammaClient._extract_resolved_truth("btc-updown-15m-123", event, metadata)

        self.assertTrue(truth["resolved_truth_available"])
        self.assertEqual(truth["resolved_settle_side"], "UP")
        self.assertEqual(truth["resolved_truth_source"], "gamma_outcome_prices")
        self.assertAlmostEqual(truth["resolved_official_opening_price"], 100000.0, places=6)


if __name__ == "__main__":
    unittest.main()
