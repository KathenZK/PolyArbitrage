from __future__ import annotations

import asyncio
import sqlite3
import time
import unittest

from src.output.db import get_connection, init_db, settle_trade, get_unsettled_trades, get_settlement_stats
from src.strategies.settlement import SettlementTracker


class SettlementDBTests(unittest.TestCase):
    def setUp(self):
        self.conn = sqlite3.connect(":memory:")
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()

    def _insert_filled_buy(self, *, action="UP", price=0.48, matched_size=10.0,
                           market_slug="btc-updown-15m-123", expiration_ts=0):
        now = time.time()
        self.conn.execute(
            """
            INSERT INTO trades (
                timestamp, updated_at, strategy, event_title, action, side,
                market_id, token_id, price, size, cost_usd, is_paper, status,
                order_side, matched_size, matched_cost_usd, market_slug,
                expiration_ts, win_prob, asset
            ) VALUES (?, ?, 'test', 'test', ?, 'Up', 'm1', 't1',
                      ?, ?, ?, 1, 'filled', 'BUY', ?, ?, ?, ?, 0.85, 'BTC')
            """,
            (now, now, action, price, matched_size, matched_size * price,
             matched_size, matched_size * price, market_slug,
             expiration_ts),
        )
        self.conn.commit()
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def _insert_filled_sell(self, *, action="UP", price=0.60, matched_size=4.0,
                            market_slug="btc-updown-15m-123", expiration_ts=0):
        now = time.time()
        self.conn.execute(
            """
            INSERT INTO trades (
                timestamp, updated_at, strategy, event_title, action, side,
                market_id, token_id, price, size, cost_usd, is_paper, status,
                order_side, matched_size, matched_cost_usd, market_slug,
                expiration_ts, win_prob, asset
            ) VALUES (?, ?, 'test', 'test', ?, 'Up', 'm1', 't1',
                      ?, ?, ?, 1, 'filled', 'SELL', ?, ?, ?, ?, 0.85, 'BTC')
            """,
            (now, now, action, price, matched_size, matched_size * price,
             matched_size, matched_size * price, market_slug,
             expiration_ts),
        )
        self.conn.commit()
        return self.conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    def test_get_unsettled_trades_returns_expired_unsettled_buys(self):
        past_expiration = int(time.time()) - 120
        self._insert_filled_buy(expiration_ts=past_expiration)
        trades = get_unsettled_trades(self.conn)
        self.assertEqual(len(trades), 1)
        self.assertAlmostEqual(trades[0]["remaining_size"], 10.0, places=6)
        self.assertAlmostEqual(trades[0]["remaining_cost_usd"], 4.8, places=6)

    def test_get_unsettled_trades_excludes_future_expiration(self):
        future = int(time.time()) + 600
        self._insert_filled_buy(expiration_ts=future)
        trades = get_unsettled_trades(self.conn)
        self.assertEqual(len(trades), 0)

    def test_settle_trade_records_win_pnl(self):
        past = int(time.time()) - 120
        trade_id = self._insert_filled_buy(
            action="UP", price=0.48, matched_size=10.0,
            expiration_ts=past,
        )
        settle_trade(
            self.conn,
            trade_id,
            settled_side="UP",
            pnl=10.0 * (1 - 0.48),
            settled_size=10.0,
            settled_cost_usd=4.8,
        )
        row = dict(self.conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone())
        self.assertEqual(row["settled_side"], "UP")
        self.assertAlmostEqual(row["pnl"], 5.2, places=2)
        self.assertAlmostEqual(row["settled_size"], 10.0, places=6)
        self.assertAlmostEqual(row["settled_cost_usd"], 4.8, places=6)
        self.assertGreater(row["settled_at"], 0)

    def test_settle_trade_records_loss_pnl(self):
        past = int(time.time()) - 120
        trade_id = self._insert_filled_buy(
            action="UP", price=0.48, matched_size=10.0,
            expiration_ts=past,
        )
        cost = 10.0 * 0.48
        settle_trade(self.conn, trade_id, settled_side="DOWN", pnl=-cost)
        row = dict(self.conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone())
        self.assertEqual(row["settled_side"], "DOWN")
        self.assertAlmostEqual(row["pnl"], -4.8, places=2)

    def test_settled_trade_excluded_from_unsettled(self):
        past = int(time.time()) - 120
        trade_id = self._insert_filled_buy(expiration_ts=past)
        settle_trade(self.conn, trade_id, settled_side="UP", pnl=5.0, settled_size=10.0, settled_cost_usd=4.8)
        self.assertEqual(len(get_unsettled_trades(self.conn)), 0)

    def test_get_unsettled_trades_nets_later_sell_shares(self):
        past = int(time.time()) - 120
        self._insert_filled_buy(price=0.50, matched_size=10.0, expiration_ts=past)
        self._insert_filled_sell(price=0.60, matched_size=4.0, expiration_ts=past)

        trades = get_unsettled_trades(self.conn)
        self.assertEqual(len(trades), 1)
        self.assertAlmostEqual(trades[0]["remaining_size"], 6.0, places=6)
        self.assertAlmostEqual(trades[0]["remaining_cost_usd"], 3.0, places=6)

    def test_settlement_stats_aggregation(self):
        past = int(time.time()) - 120
        t1 = self._insert_filled_buy(action="UP", price=0.48, matched_size=10.0, expiration_ts=past)
        t2 = self._insert_filled_buy(action="DOWN", price=0.50, matched_size=10.0, expiration_ts=past)
        settle_trade(self.conn, t1, settled_side="UP", pnl=5.2)
        settle_trade(self.conn, t2, settled_side="UP", pnl=-5.0)

        stats = get_settlement_stats(self.conn)
        self.assertEqual(stats["total"], 2)
        self.assertEqual(stats["wins"], 1)
        self.assertEqual(stats["losses"], 1)
        self.assertAlmostEqual(stats["total_pnl"], 0.2, places=2)
        self.assertAlmostEqual(stats["actual_win_rate"], 0.5, places=2)

    def test_settlement_tracker_keeps_retrying_after_many_failures(self):
        class FlakyGamma:
            def __init__(self):
                self.calls = 0

            async def get_resolved_truth(self, slug: str):
                self.calls += 1
                raise RuntimeError("temporary gamma outage")

        past = int(time.time()) - 300
        self._insert_filled_buy(expiration_ts=past)
        gamma = FlakyGamma()
        tracker = SettlementTracker(gamma, self.conn, poll_interval_secs=1.0)
        tracker._attempt_counts["btc-updown-15m-123"] = 50

        settled = asyncio.run(tracker.check_once())
        self.assertEqual(settled, 0)
        self.assertEqual(gamma.calls, 1)
        self.assertGreater(tracker._next_retry_at["btc-updown-15m-123"], time.time())


if __name__ == "__main__":
    unittest.main()
