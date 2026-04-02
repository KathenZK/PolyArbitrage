from __future__ import annotations

import tempfile
import time
import unittest
from unittest.mock import patch
from pathlib import Path

from src.data.market_registry import CryptoMarket
from src.output.db import get_connection, init_db, insert_trade
from src.strategies.executor import Executor
from src.strategies.momentum import Direction, Signal


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
        best_ask=0.62,
        event_start=int(now) - 300,
        end_time=now + 300,
        liquidity=5000,
        spread=0.01,
        opening_price=100000,
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


def seed_trade(conn, *, matched_ratio: float, hours_ago: float):
    trade_id = insert_trade(
        conn,
        strategy="latency_arb",
        event_title="test",
        action="UP",
        side="Up",
        asset="BTC",
        market_id="m1",
        market_slug="btc-updown-15m-123",
        token_id="token-up",
        price=0.61,
        size=25.0,
        matched_size=25.0 * matched_ratio,
        cost_usd=15.0,
        matched_cost_usd=15.0 * matched_ratio,
        is_paper=False,
        status="filled" if matched_ratio >= 0.999 else "expired",
        order_id=f"seed-{matched_ratio}-{hours_ago}",
        win_prob=0.75,
        fill_prob=matched_ratio,
        fill_lower_bound=max(0.0, matched_ratio - 0.1),
        fill_confidence=0.5,
        fill_effective_samples=5.0,
        fill_source="seed",
        filled_ev_usd=1.0,
        expected_value_usd=0.5,
        taker_fee_avoided=0.1,
        expiration_ts=int(time.time()),
        secs_remaining_at_submit=300.0,
        liquidity_at_submit=5000.0,
        spread_at_submit=0.01,
        queue_ticks_at_submit=0.0,
        raw_data={"seed": True},
    )
    ts = time.time() - hours_ago * 3600
    conn.execute(
        "UPDATE trades SET timestamp=?, updated_at=?, resolved_at=? WHERE id=?",
        (ts, ts, ts, trade_id),
    )
    conn.commit()


class FillCalibrationTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.tmpdir.name) / "polyarb.db"
        self.conn = get_connection(self.db_path)
        init_db(self.conn)

    def tearDown(self):
        self.conn.close()
        self.tmpdir.cleanup()

    def test_recent_samples_have_more_weight_than_old_samples(self):
        seed_trade(self.conn, matched_ratio=1.0, hours_ago=1)
        seed_trade(self.conn, matched_ratio=0.0, hours_ago=72)

        fast_decay = Executor(
            dry_run=True,
            fill_min_samples=1,
            fill_decay_half_life_hours=6,
            fill_prior_strength=4,
            fill_confidence_scale=4,
        )
        slow_decay = Executor(
            dry_run=True,
            fill_min_samples=1,
            fill_decay_half_life_hours=240,
            fill_prior_strength=4,
            fill_confidence_scale=4,
        )
        fast_decay.attach_db(self.conn)
        slow_decay.attach_db(self.conn)

        fast_plan = fast_decay.evaluate_signal(build_signal())
        slow_plan = slow_decay.evaluate_signal(build_signal())
        self.assertIsNotNone(fast_plan)
        self.assertIsNotNone(slow_plan)
        self.assertGreater(fast_plan.expected_fill_ratio, slow_plan.expected_fill_ratio)
        self.assertLess(fast_plan.fill_ratio_lower_bound, fast_plan.expected_fill_ratio)
        self.assertGreater(fast_plan.fill_confidence, 0)
        self.assertEqual(fast_plan.fill_source, "decayed_history")

    def test_insufficient_fill_samples_stays_heuristic(self):
        seed_trade(self.conn, matched_ratio=1.0, hours_ago=1)
        seed_trade(self.conn, matched_ratio=0.0, hours_ago=72)

        executor = Executor(dry_run=True, fill_min_samples=3)
        executor.attach_db(self.conn)

        plan = executor.evaluate_signal(build_signal())
        self.assertIsNotNone(plan)
        self.assertEqual(plan.fill_source, "heuristic_insufficient_samples")

    def test_heuristic_lower_bound_is_more_conservative_than_mean(self):
        executor = Executor(dry_run=True)
        plan = executor.evaluate_signal(build_signal())
        self.assertIsNotNone(plan)
        self.assertLess(plan.fill_ratio_lower_bound, plan.expected_fill_ratio)
        self.assertEqual(plan.fill_source, "heuristic")

    def test_live_preflight_checks_proxy_wallet_balance_and_allowance(self):
        class FakeCLOB:
            def get_signer_address(self):
                return "0xSigner"

            def get_collateral_balance_allowance(self, signature_type=None):
                return {
                    "balance": "8560000",
                    "allowances": {
                        "0xspender": "20000000",
                    },
                }

        executor = Executor(dry_run=False, bet_size_usd=2)
        executor._clob = FakeCLOB()
        with patch.dict(
            "os.environ",
            {
                "POLYMARKET_SIGNATURE_TYPE": "1",
                "POLYMARKET_FUNDER": "0xFunder",
            },
            clear=False,
        ):
            report = executor.live_preflight()

        self.assertTrue(report.ok)
        self.assertEqual(report.signer_address, "0xSigner")
        self.assertEqual(report.funder_address, "0xFunder")
        self.assertAlmostEqual(report.collateral_balance, 8.56, places=6)
        self.assertAlmostEqual(report.max_allowance, 20.0, places=6)


if __name__ == "__main__":
    unittest.main()
