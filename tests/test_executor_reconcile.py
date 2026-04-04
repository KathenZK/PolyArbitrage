from __future__ import annotations

import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from src.data.market_registry import CryptoMarket
from src.output.db import get_connection, init_db, insert_trade
from src.strategies.executor import Executor, OpenPosition, OrderStatus, TokenQuote
from src.strategies.momentum import Direction, MarketEstimate, Signal


class FakeCLOB:
    def __init__(self, final_status: str = "matched"):
        self.final_status = final_status
        self.open_orders: list[dict] = []
        self.orders: dict[str, dict] = {}
        self.cancelled: list[str] = []
        self.last_order_id = 0
        self.heartbeats: list[str | None] = []

    def get_best_bid(self, token_id: str) -> float:
        return 0.61

    def get_book_snapshot(self, token_id: str) -> TokenQuote:
        return TokenQuote(
            token_id=token_id,
            best_bid=0.61,
            best_ask=0.63,
            spread=0.02,
            tick_size=0.01,
            executable_book=True,
            best_bid_size=40.0,
            best_ask_size=40.0,
            best_bid_notional=24.4,
            best_ask_notional=25.2,
            bid_depth_usd=40.0,
            ask_depth_usd=40.0,
        )

    def post_heartbeat(self, heartbeat_id=None):
        self.heartbeats.append(heartbeat_id)
        return {"heartbeat_id": "hb-1"}

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
            order_side="BUY",
            asset="BTC",
            market_id="seed-market",
            condition_id="0xseedcond",
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

    def test_stale_pending_order_is_cancelled_when_signal_disappears(self):
        executor = Executor(dry_run=False)
        executor.attach_db(self.conn)
        fake_clob = FakeCLOB(final_status="live")
        executor._clob = fake_clob

        trade = asyncio.run(executor.execute(build_signal()))
        self.assertIsNotNone(trade)
        fake_clob.open_orders = [{"orderID": trade.order_id, "status": "live"}]

        asyncio.run(executor.reconcile_pending_orders(force=True, signal_lookup=lambda _: None))

        self.assertIn(trade.order_id, fake_clob.cancelled)
        self.assertEqual(trade.status, OrderStatus.EXPIRED)

    def test_live_heartbeat_is_forwarded_to_clob(self):
        executor = Executor(dry_run=False)
        executor.attach_db(self.conn)
        fake_clob = FakeCLOB(final_status="live")
        executor._clob = fake_clob

        trade = asyncio.run(executor.execute(build_signal()))
        self.assertIsNotNone(trade)

        sent = asyncio.run(executor.send_heartbeat(force=True))

        self.assertTrue(sent)
        self.assertEqual(fake_clob.heartbeats, [None])

    def test_dust_token_book_is_rejected_in_live(self):
        class DustCLOB(FakeCLOB):
            def get_book_snapshot(self, token_id: str) -> TokenQuote:
                return TokenQuote(
                    token_id=token_id,
                    best_bid=0.01,
                    best_ask=0.99,
                    spread=0.98,
                    tick_size=0.01,
                )

        executor = Executor(dry_run=False)
        executor.attach_db(self.conn)
        executor._clob = DustCLOB(final_status="live")

        plan = executor.evaluate_signal(build_signal())
        self.assertIsNone(plan)

    def test_dust_token_book_still_falls_back_in_dry_run(self):
        executor = Executor(dry_run=True)
        signal = build_signal()
        signal.market.up_best_bid = 0.01
        signal.market.up_best_ask = 0.99
        signal.market.up_spread = 0.98

        plan = executor.evaluate_signal(signal)

        self.assertIsNotNone(plan)
        self.assertEqual(plan.quote_source, "synthetic_fallback")

    def test_live_entry_requires_sufficient_top_book_notional(self):
        class ThinBookCLOB(FakeCLOB):
            def get_book_snapshot(self, token_id: str) -> TokenQuote:
                return TokenQuote(
                    token_id=token_id,
                    best_bid=0.61,
                    best_ask=0.63,
                    spread=0.02,
                    tick_size=0.01,
                    executable_book=True,
                    best_bid_size=2.0,
                    best_ask_size=2.0,
                    best_bid_notional=1.22,
                    best_ask_notional=1.26,
                    bid_depth_usd=5.0,
                    ask_depth_usd=5.0,
                )

        executor = Executor(dry_run=False)
        executor.attach_db(self.conn)
        executor._clob = ThinBookCLOB(final_status="live")

        plan = executor.evaluate_signal(build_signal())
        self.assertIsNone(plan)

    def test_exit_plan_uses_sell_market_order_when_bid_beats_hold_value(self):
        executor = Executor(dry_run=False)
        executor.attach_db(self.conn)
        executor._clob = FakeCLOB(final_status="matched")

        signal = build_signal()
        estimate = MarketEstimate(
            asset="BTC",
            binance_symbol="btcusdt",
            market=signal.market,
            timestamp=time.time(),
            current_price=signal.current_price,
            opening_price=signal.opening_price,
            effective_deviation_pct=signal.deviation_pct,
            price_source=signal.price_source,
            binance_deviation_pct=signal.binance_deviation_pct,
            official_deviation_pct=signal.official_deviation_pct,
            official_opening_price=signal.official_opening_price,
            official_current_price=signal.official_current_price,
            projected_official_price=signal.projected_official_price,
            source_gap_pct=signal.source_gap_pct,
            up_win_prob=0.55,
            down_win_prob=0.45,
        )
        position = OpenPosition(
            asset="BTC",
            binance_symbol="btcusdt",
            market_id=signal.market.market_id,
            condition_id=signal.market.condition_id,
            market_slug=signal.market.slug,
            token_id=signal.market.up_token_id,
            token_side="Up",
            direction="UP",
            net_shares=10.0,
            pending_sell_shares=0.0,
            available_shares=10.0,
            avg_entry_price=0.60,
            last_trade_ts=time.time() - 60,
        )

        plan = executor.evaluate_exit_position(position, estimate)
        self.assertIsNotNone(plan)
        self.assertEqual(plan.order_side, "SELL")
        self.assertEqual(plan.order_type, "MARKET")
        self.assertEqual(plan.price, 0.61)

    def test_exit_plan_requires_sufficient_top_bid_depth(self):
        class ThinBidCLOB(FakeCLOB):
            def get_book_snapshot(self, token_id: str) -> TokenQuote:
                return TokenQuote(
                    token_id=token_id,
                    best_bid=0.61,
                    best_ask=0.63,
                    spread=0.02,
                    tick_size=0.01,
                    executable_book=True,
                    best_bid_size=5.0,
                    best_ask_size=40.0,
                    best_bid_notional=3.05,
                    best_ask_notional=25.2,
                    bid_depth_usd=5.0,
                    ask_depth_usd=40.0,
                )

        executor = Executor(dry_run=False)
        executor.attach_db(self.conn)
        executor._clob = ThinBidCLOB(final_status="matched")

        signal = build_signal()
        estimate = MarketEstimate(
            asset="BTC",
            binance_symbol="btcusdt",
            market=signal.market,
            timestamp=time.time(),
            current_price=signal.current_price,
            opening_price=signal.opening_price,
            effective_deviation_pct=signal.deviation_pct,
            price_source=signal.price_source,
            binance_deviation_pct=signal.binance_deviation_pct,
            official_deviation_pct=signal.official_deviation_pct,
            official_opening_price=signal.official_opening_price,
            official_current_price=signal.official_current_price,
            projected_official_price=signal.projected_official_price,
            source_gap_pct=signal.source_gap_pct,
            up_win_prob=0.55,
            down_win_prob=0.45,
        )
        position = OpenPosition(
            asset="BTC",
            binance_symbol="btcusdt",
            market_id=signal.market.market_id,
            condition_id=signal.market.condition_id,
            market_slug=signal.market.slug,
            token_id=signal.market.up_token_id,
            token_side="Up",
            direction="UP",
            net_shares=10.0,
            pending_sell_shares=0.0,
            available_shares=10.0,
            avg_entry_price=0.60,
            last_trade_ts=time.time() - 60,
        )

        plan = executor.evaluate_exit_position(position, estimate)
        self.assertIsNone(plan)

    def test_total_directional_exposure_uses_open_positions_not_recent_orders(self):
        old_ts = time.time() - 7200
        insert_trade(
            self.conn,
            strategy="latency_arb",
            event_title="btc old",
            action="UP",
            side="Up",
            order_side="BUY",
            asset="BTC",
            market_id="old-btc",
            condition_id="cond-btc",
            market_slug="btc-old",
            token_id="token-btc-up",
            price=0.50,
            size=20.0,
            matched_size=20.0,
            cost_usd=10.0,
            matched_cost_usd=10.0,
            is_paper=False,
            status="filled",
            order_id="btc-old-order",
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
        self.conn.execute("UPDATE trades SET timestamp=?, updated_at=? WHERE order_id='btc-old-order'", (old_ts, old_ts))
        insert_trade(
            self.conn,
            strategy="latency_arb",
            event_title="eth old",
            action="UP",
            side="Up",
            order_side="BUY",
            asset="ETH",
            market_id="old-eth",
            condition_id="cond-eth",
            market_slug="eth-old",
            token_id="token-eth-up",
            price=0.40,
            size=20.0,
            matched_size=20.0,
            cost_usd=8.0,
            matched_cost_usd=8.0,
            is_paper=False,
            status="filled",
            order_id="eth-old-order",
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
        self.conn.execute("UPDATE trades SET timestamp=?, updated_at=? WHERE order_id='eth-old-order'", (old_ts, old_ts))
        self.conn.commit()

        executor = Executor(dry_run=False, bet_size_usd=6.0, max_total_directional_exposure_usd=15.0)
        executor.attach_db(self.conn)
        executor._clob = FakeCLOB(final_status="matched")

        plan = executor.evaluate_signal(build_signal())

        self.assertIsNone(plan)
        self.assertEqual(executor.skipped_live_limits, 1)


if __name__ == "__main__":
    unittest.main()
