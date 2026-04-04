from __future__ import annotations

import sqlite3
import time
import unittest

from src.output.db import get_connection, init_db, settle_trade, get_unsettled_trades, get_settlement_stats


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

    def test_get_unsettled_trades_returns_expired_unsettled_buys(self):
        past_expiration = int(time.time()) - 120
        self._insert_filled_buy(expiration_ts=past_expiration)
        trades = get_unsettled_trades(self.conn)
        self.assertEqual(len(trades), 1)

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
        settle_trade(self.conn, trade_id, settled_side="UP", pnl=10.0 * (1 - 0.48))
        row = dict(self.conn.execute("SELECT * FROM trades WHERE id=?", (trade_id,)).fetchone())
        self.assertEqual(row["settled_side"], "UP")
        self.assertAlmostEqual(row["pnl"], 5.2, places=2)
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
        settle_trade(self.conn, trade_id, settled_side="UP", pnl=5.0)
        self.assertEqual(len(get_unsettled_trades(self.conn)), 0)

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


if __name__ == "__main__":
    unittest.main()
