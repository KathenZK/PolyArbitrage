"""Settlement tracker: checks resolved markets and records actual P&L.

After each 15-minute window ends, queries the Gamma API for the market
outcome (UP or DOWN won), then updates every filled BUY trade with:
  - settled_side: which side actually won
  - pnl: actual profit/loss in USD
  - settled_at: timestamp of settlement check

Binary payout math:
  - WIN:  pnl = matched_shares × (1.0 - entry_price)
  - LOSS: pnl = -matched_cost_usd
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
import time
from typing import Any

from src.data.polymarket_client import PolymarketGammaClient
from src.output.db import get_settlement_stats, get_unsettled_trades, settle_trade

logger = logging.getLogger(__name__)

SETTLE_DELAY_SECS = 90
MAX_SETTLE_ATTEMPTS = 10


class SettlementTracker:

    def __init__(
        self,
        gamma: PolymarketGammaClient,
        db: sqlite3.Connection,
        poll_interval_secs: float = 60.0,
    ):
        self._gamma = gamma
        self._db = db
        self._poll_interval = poll_interval_secs
        self._running = False
        self._attempt_counts: dict[str, int] = {}
        self.settled_count = 0
        self.win_count = 0
        self.loss_count = 0
        self.total_pnl = 0.0

    async def run(self):
        self._running = True
        while self._running:
            try:
                await self.check_once()
            except Exception as exc:
                logger.debug(f"Settlement check error: {exc}")
            await asyncio.sleep(self._poll_interval)

    def stop(self):
        self._running = False

    async def check_once(self) -> int:
        trades = get_unsettled_trades(self._db)
        if not trades:
            return 0

        by_slug: dict[str, list[dict[str, Any]]] = {}
        now = time.time()
        for trade in trades:
            slug = str(trade.get("market_slug", "") or "")
            expiration = int(trade.get("expiration_ts", 0) or 0)
            if not slug or expiration <= 0:
                continue
            window_end = expiration - 60
            if now < window_end + SETTLE_DELAY_SECS:
                continue
            attempts = self._attempt_counts.get(slug, 0)
            if attempts >= MAX_SETTLE_ATTEMPTS:
                continue
            by_slug.setdefault(slug, []).append(trade)

        settled = 0
        for slug, slug_trades in by_slug.items():
            try:
                truth = await asyncio.wait_for(
                    self._gamma.get_resolved_truth(slug),
                    timeout=15,
                )
            except Exception as exc:
                self._attempt_counts[slug] = self._attempt_counts.get(slug, 0) + 1
                logger.debug(f"Settlement truth fetch failed for {slug}: {exc}")
                continue

            if not truth.get("resolved_truth_available"):
                self._attempt_counts[slug] = self._attempt_counts.get(slug, 0) + 1
                continue

            winning_side = str(truth.get("resolved_settle_side", "") or "").upper()
            source = str(truth.get("resolved_truth_source", "") or "")
            if winning_side not in {"UP", "DOWN"}:
                self._attempt_counts[slug] = self._attempt_counts.get(slug, 0) + 1
                continue

            for trade in slug_trades:
                trade_id = int(trade["id"])
                direction = str(trade.get("action", "") or "").upper()
                matched_shares = float(trade.get("matched_size", 0) or 0)
                matched_cost = float(trade.get("matched_cost_usd", 0) or 0)
                entry_price = float(trade.get("price", 0) or 0)

                if direction == winning_side:
                    pnl = matched_shares * (1.0 - entry_price)
                else:
                    pnl = -matched_cost

                settle_trade(
                    self._db,
                    trade_id,
                    settled_side=winning_side,
                    pnl=pnl,
                    settlement_source=source,
                )
                settled += 1
                self.settled_count += 1
                self.total_pnl += pnl
                if pnl > 0:
                    self.win_count += 1
                elif pnl < 0:
                    self.loss_count += 1

                result_tag = "WIN" if pnl > 0 else "LOSS"
                asset = str(trade.get("asset", "") or "").upper()
                model_p = float(trade.get("win_prob", 0) or 0)
                logger.info(
                    f"[SETTLED] {asset} {direction} -> {winning_side} = {result_tag} "
                    f"pnl=${pnl:+.2f} (model_p={model_p:.1%}, "
                    f"cost=${matched_cost:.2f}, shares={matched_shares:.1f})"
                )

            self._attempt_counts.pop(slug, None)

        if settled > 0:
            stats = get_settlement_stats(self._db)
            total = stats["total"]
            wins = stats["wins"]
            rate = stats["actual_win_rate"]
            model_rate = stats["avg_model_win_prob"]
            cum_pnl = stats["total_pnl"]
            logger.info(
                f"Settlement stats: {wins}/{total} wins ({rate:.1%} actual vs "
                f"{model_rate:.1%} model), cumulative P&L: ${cum_pnl:+.2f}"
            )

        return settled

    @property
    def stats(self) -> dict[str, Any]:
        return get_settlement_stats(self._db)
