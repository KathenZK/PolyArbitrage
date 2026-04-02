from __future__ import annotations

import tempfile
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, patch

from eth_account import Account

from src.execution.redeemer import DEFAULT_COLLATERAL, ProxyRedeemer
from src.output.db import get_connection, get_pending_redeems, init_db, insert_trade


class FakeGamma:
    def __init__(self, positions: list[dict]):
        self._positions = positions

    async def get_positions(self, user: str, *, redeemable=None, limit=500, offset=0):
        return list(self._positions)


class RedeemerTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "polyarb.db"
        self.conn = get_connection(self.db_path)
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmpdir.cleanup()

    async def test_redeemer_scopes_to_strategy_condition_ids(self):
        tracked_condition = "0x" + "12" * 32
        untracked_condition = "0x" + "34" * 32
        insert_trade(
            self.conn,
            strategy="latency_arb",
            event_title="test",
            action="UP",
            side="Up",
            asset="BTC",
            market_id="market-1",
            condition_id=tracked_condition,
            market_slug="btc-updown-15m-123",
            token_id="token-up",
            price=0.60,
            size=10.0,
            matched_size=10.0,
            cost_usd=6.0,
            matched_cost_usd=6.0,
            is_paper=False,
            status="filled",
            order_id="order-1",
            win_prob=0.7,
            fill_prob=1.0,
            fill_lower_bound=1.0,
            fill_confidence=1.0,
            fill_effective_samples=5.0,
            fill_source="test",
            filled_ev_usd=0.2,
            expected_value_usd=0.2,
            taker_fee_avoided=0.0,
        )

        owner_pk = "0x" + "11" * 32
        owner = Account.from_key(owner_pk).address
        funder = ProxyRedeemer._derive_proxy_wallet(owner)

        positions = [
            {
                "conditionId": tracked_condition,
                "size": 3.0,
                "slug": "btc-updown-15m-123",
                "proxyWallet": funder,
                "outcome": "Up",
                "title": "Tracked",
            },
            {
                "conditionId": untracked_condition,
                "size": 4.0,
                "slug": "other-market",
                "proxyWallet": funder,
                "outcome": "Up",
                "title": "Untracked",
            },
        ]
        gamma = FakeGamma(positions)

        with patch.dict(
            "os.environ",
            {
                "POLYMARKET_PRIVATE_KEY": owner_pk,
                "POLYMARKET_FUNDER": funder,
                "RELAYER_API_KEY": "relayer-key",
                "RELAYER_API_KEY_ADDRESS": "0xrelay",
            },
            clear=False,
        ):
            worker = ProxyRedeemer(gamma, poll_interval_secs=1)
            worker.attach_db(self.conn)
            worker._submit_redeem = AsyncMock(  # type: ignore[method-assign]
                return_value={"transactionID": "tx-1", "transactionHash": "0xabc"}
            )
            changed = await worker.run_once()

        self.assertGreaterEqual(changed, 1)
        worker._submit_redeem.assert_awaited_once_with(tracked_condition)  # type: ignore[attr-defined]

        rows = get_pending_redeems(self.conn)
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["condition_id"], tracked_condition)
        self.assertEqual(rows[0]["status"], "submitted")

    def test_encode_redeem_positions_selector(self):
        condition_id = "0x" + "56" * 32
        calldata = ProxyRedeemer._encode_redeem_positions(DEFAULT_COLLATERAL, condition_id)
        self.assertTrue(calldata.startswith("0x01b7037c"))


if __name__ == "__main__":
    unittest.main()
