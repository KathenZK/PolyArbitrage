from __future__ import annotations

import unittest

from src.strategies.executor import Executor
from src.strategies.momentum import Direction, Signal
from src.strategies.replay import run_replay
from src.data.market_registry import CryptoMarket


def build_signal(win_prob: float = 0.70) -> Signal:
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
        event_start=0,
        end_time=10**10,
        opening_price=100000,
        liquidity=5000,
    )
    return Signal(
        asset="BTC",
        binance_symbol="btcusdt",
        direction=Direction.UP,
        current_price=101000,
        opening_price=100000,
        deviation_pct=0.01,
        win_prob=win_prob,
        market=market,
        timestamp=10**9,
    )


class ReplayTests(unittest.TestCase):
    def test_fill_adjusted_ev_blocks_low_fill_trades(self):
        signal = build_signal(win_prob=0.70)
        strict = Executor(
            dry_run=True,
            min_ev_usd=0.10,
            fill_rate_prior=0.05,
        )
        loose = Executor(
            dry_run=True,
            min_ev_usd=0.10,
            fill_rate_prior=0.35,
        )

        self.assertIsNone(strict.evaluate_signal(signal))
        plan = loose.evaluate_signal(signal)
        self.assertIsNotNone(plan)
        self.assertGreater(plan.submitted_ev, 0.10)

    def test_replay_summary_aggregates_trades(self):
        config = {
            "strategy": {
                "symbols": ["btcusdt"],
                "edge_threshold_pct": 0.003,
                "min_secs_remaining": 30,
                "min_secs_elapsed": 30,
                "annual_vol_btcusdt": 0.60,
                "signal_cooldown_sec": 120,
                "bet_size_usd": 15,
                "min_liquidity": 1000,
                "min_ev_usd": 0.10,
                "adverse_selection_haircut": 0.05,
                "maker_offset_ticks": 1,
                "fill_rate_prior": 0.35,
            }
        }
        rows = [
            {
                "timestamp": 1_700_000_000,
                "symbol": "btcusdt",
                "window_start": 1_699_999_900,
                "binance_price": 101000,
                "opening_price": 100000,
                "up_price": 0.62,
                "down_price": 0.38,
                "best_bid": 0.61,
                "best_ask": 0.63,
                "liquidity": 5000,
                "secs_remaining": 600,
                "secs_elapsed": 300,
                "final_price": 102000,
            },
            {
                "timestamp": 1_700_000_030,
                "symbol": "btcusdt",
                "window_start": 1_699_999_900,
                "binance_price": 101100,
                "opening_price": 100000,
                "up_price": 0.63,
                "down_price": 0.37,
                "best_bid": 0.62,
                "best_ask": 0.64,
                "liquidity": 5000,
                "secs_remaining": 570,
                "secs_elapsed": 330,
                "final_price": 102000,
            },
        ]

        summary = run_replay(rows, config)

        self.assertEqual(summary.rows, 2)
        self.assertEqual(summary.signals, 2)
        self.assertEqual(summary.trades, 1)
        self.assertGreater(summary.expected_submitted_ev, 0)
        self.assertGreater(summary.realized_submitted_pnl, 0)
        self.assertIn("BTC", summary.by_asset)

    def test_replay_uses_official_anchor_when_available(self):
        config = {
            "strategy": {
                "symbols": ["btcusdt"],
                "edge_threshold_pct": 0.003,
                "min_secs_remaining": 30,
                "min_secs_elapsed": 30,
                "annual_vol_btcusdt": 0.60,
                "signal_cooldown_sec": 120,
                "bet_size_usd": 15,
                "min_liquidity": 1000,
                "min_ev_usd": 0.10,
                "adverse_selection_haircut": 0.05,
                "maker_offset_ticks": 1,
                "fill_rate_prior": 0.35,
                "require_official_source": True,
                "official_max_age_sec": 120,
                "max_source_divergence_pct": 0.01,
            }
        }
        rows = [
            {
                "timestamp": 1_700_000_000,
                "symbol": "btcusdt",
                "window_start": 1_699_999_900,
                "binance_price": 100300,
                "opening_price": 100000,
                "official_opening_price": 99900,
                "official_current_price": 100250,
                "official_binance_ref_price": 100200,
                "official_price_updated_at": 1_699_999_980,
                "up_price": 0.58,
                "down_price": 0.42,
                "best_bid": 0.57,
                "best_ask": 0.59,
                "liquidity": 5000,
                "secs_remaining": 600,
                "secs_elapsed": 300,
                "final_price": 100500,
            },
        ]

        summary = run_replay(rows, config)

        self.assertEqual(summary.signals, 1)
        self.assertEqual(summary.trades, 1)
        self.assertGreater(summary.expected_submitted_ev, 0)


if __name__ == "__main__":
    unittest.main()
