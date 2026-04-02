from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from src.data.market_registry import CryptoMarket
from src.output.db import get_connection, init_db, insert_trade
from src.strategies.executor import Executor, OrderStatus
from src.strategies.momentum import Direction, Signal


class FakeCLOB:
    def __init__(self, final_status: str = "matched"):
        self.final_status = final_status
        self.open_orders: list[dict] = []
        self.orders: dict[str, dict] = {}
        self.cancelled: list[str] = []
        self.last_order_id = 0

    def get_best_bid(self, token_id: str) -> float:
        return 0.61

    def place_limit_order(self, **kwargs):
        self.last_order_id += 1
        order_id = f"order-{self.last_order_id}"
        self.orders[order_id] = {
            "orderID": order_id,
            "status": self.final_status,
            "matched_size": kwargs["size"],
        }
        return {"orderID": order_id, "status": "live"}

    def get_open_orders(self):
        return list(self.open_orders)

    def get_order(self, order_id: str):
        return self.orders[order_id]

    def cancel_order(self, order_id: str):
        self.cancelled.append(order_id)
        self.orders[order_id] = {"orderID": order_id, "status": "expired"}
        return self.orders[order_id]


def build_signal() -> Signal:
    now = time.time()
    market = CryptoMarket(
        market_id="market-1",
        question="Will BTC finish above the opening price?",
        slug="btc-updown-15m-123",
        asset="btc",
        binance_symbol="btcusdt",
        up_token_id="token-up",
        down_token_id="token-down",
        up_price=0.62,
        down_price=0.38,
        best_bid=0.61,
        best_ask=0.63,
        event_start=int(now) - 60,
        end_time=now + 600,
        liquidity=5000,
    )
    return Signal(
        asset="BTC",
        binance_symbol="btcusdt",
        direction=Direction.UP,
        current_price=101000,
        opening_price=100000,
        deviation_pct=0.01,
        win_prob=0.85,
        market=market,
        timestamp=now,
    )


class ExecutorReconcileTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "polyarb.db"
        self.conn = get_connection(self.db_path)
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmpdir.cleanup()

    def test_live_order_reconcile_updates_status_and_db(self):
        executor = Executor(dry_run=False)
        executor.attach_db(self.conn)
        executor._clob = FakeCLOB(final_status="matched")

        trade = asyncio.run(executor.execute(build_signal()))
        self.assertIsNotNone(trade)
        self.assertEqual(trade.status, OrderStatus.PENDING)
        self.assertEqual(executor.pending_count, 1)

        asyncio.run(executor.reconcile_pending_orders(force=True))

        self.assertEqual(trade.status, OrderStatus.FILLED)
        self.assertAlmostEqual(trade.matched_cost_usd, 15.0, places=2)
        row = self.conn.execute("SELECT status, matched_cost_usd FROM trades WHERE id=?", (trade.db_id,)).fetchone()
        self.assertEqual(row["status"], "filled")
        self.assertAlmostEqual(row["matched_cost_usd"], 15.0, places=2)

    def test_pending_orders_restore_from_db_on_restart(self):
        first = Executor(dry_run=False)
        first.attach_db(self.conn)
        first._clob = FakeCLOB(final_status="matched")
        trade = asyncio.run(first.execute(build_signal()))
        self.assertIsNotNone(trade)
        self.assertEqual(trade.status, OrderStatus.PENDING)

        restored = Executor(dry_run=False)
        restored.attach_db(self.conn)
        restored._clob = first._clob
        restored.bootstrap_pending_orders()

        self.assertEqual(restored.pending_count, 1)
        restored_trade = restored.recent_trades[-1]
        self.assertEqual(restored_trade.order_id, trade.order_id)

        asyncio.run(restored.reconcile_pending_orders(force=True))

        self.assertEqual(restored_trade.status, OrderStatus.FILLED)
        row = self.conn.execute("SELECT status FROM trades WHERE order_id=?", (trade.order_id,)).fetchone()
        self.assertEqual(row["status"], "filled")

    def test_live_order_limit_blocks_new_trade(self):
        insert_trade(
            self.conn,
            strategy="latency_arb",
            event_title="seed",
            action="UP",
            side="Up",
            asset="BTC",
            market_id="seed-market",
            market_slug="seed-market",
            token_id="seed-token",
            price=0.61,
            size=24.59,
            matched_size=0.0,
            cost_usd=15.0,
            matched_cost_usd=0.0,
            is_paper=False,
            status="pending",
            order_id="seed-order",
            win_prob=0.8,
            fill_prob=0.3,
            fill_lower_bound=0.2,
            fill_confidence=0.1,
            fill_effective_samples=1.0,
            fill_source="seed",
            filled_ev_usd=0.5,
            expected_value_usd=0.1,
            taker_fee_avoided=0.0,
        )

        executor = Executor(dry_run=False, max_live_orders_per_day=1)
        executor.attach_db(self.conn)
        executor._clob = FakeCLOB(final_status="matched")

        trade = asyncio.run(executor.execute(build_signal()))

        self.assertIsNone(trade)
        self.assertEqual(executor.skipped_live_limits, 1)


if __name__ == "__main__":
    unittest.main()
