"""PolyArbitrage — main entry point and orchestrator."""

from __future__ import annotations

import asyncio
import logging
import os
import sys
import time
from pathlib import Path

import yaml
from dotenv import load_dotenv
from rich.console import Console
from rich.live import Live

load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.data.market_store import MarketStore
from src.data.polymarket_client import PolymarketGammaClient
from src.output.alerts import AlertSystem
from src.output.dashboard import build_layout
from src.output.db import get_connection, init_db
from src.strategies.base import Opportunity

console = Console()
logger = logging.getLogger("polyarbitrage")


def load_config() -> dict:
    cfg_path = Path(__file__).resolve().parent.parent / "config.yaml"
    with open(cfg_path) as f:
        return yaml.safe_load(f)


class Orchestrator:
    """Runs all enabled strategies on a timer loop and updates the dashboard."""

    def __init__(self, config: dict):
        self.config = config
        self.store = MarketStore()
        self.gamma = PolymarketGammaClient()
        self.alerts = AlertSystem(
            telegram_token=config.get("alerts", {}).get("telegram_bot_token", "")
                           or os.getenv("TELEGRAM_BOT_TOKEN", ""),
            telegram_chat_id=config.get("alerts", {}).get("telegram_chat_id", "")
                             or os.getenv("TELEGRAM_CHAT_ID", ""),
        )
        self.opportunities: list[Opportunity] = []
        self.scan_count = 0
        self.last_scan = 0.0
        self._strategies = []

    def _init_strategies(self):
        cfg = self.config.get("strategies", {})

        if cfg.get("negrisk", {}).get("enabled", True):
            from src.strategies.negrisk import NegRiskStrategy
            nr_cfg = cfg["negrisk"]
            self._strategies.append(NegRiskStrategy(
                gamma_client=self.gamma,
                store=self.store,
                min_deviation=nr_cfg.get("min_deviation", 0.03),
                min_daily_volume=nr_cfg.get("min_daily_volume", 50_000),
                max_settlement_days=nr_cfg.get("max_settlement_days", 30),
            ))

        if cfg.get("cross_platform", {}).get("enabled", True):
            try:
                from src.strategies.cross_platform import CrossPlatformStrategy
                cp_cfg = cfg["cross_platform"]
                kalshi_key_id = os.getenv("KALSHI_API_KEY_ID", "")
                kalshi_pk_path = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
                self._strategies.append(CrossPlatformStrategy(
                    gamma_client=self.gamma,
                    store=self.store,
                    kalshi_key_id=kalshi_key_id,
                    kalshi_pk_path=kalshi_pk_path,
                    min_gross_edge=cp_cfg.get("min_gross_edge", 0.0275),
                    kalshi_fee=cp_cfg.get("kalshi_fee", 0.0175),
                    poly_fee=cp_cfg.get("polymarket_fee", 0.0),
                    bridge_cost=cp_cfg.get("bridge_cost", 0.005),
                ))
            except Exception as e:
                logger.warning(f"Cross-platform strategy disabled: {e}")

        if cfg.get("sports_ev", {}).get("enabled", True):
            try:
                from src.strategies.sports_ev import SportsEVStrategy
                ev_cfg = cfg["sports_ev"]
                self._strategies.append(SportsEVStrategy(
                    gamma_client=self.gamma,
                    store=self.store,
                    min_edge=ev_cfg.get("min_edge", 0.04),
                    sports=ev_cfg.get("sports", ["nba"]),
                ))
            except Exception as e:
                logger.warning(f"Sports EV strategy disabled: {e}")

    async def run_scan(self):
        all_opps: list[Opportunity] = []
        for strategy in self._strategies:
            try:
                opps = await strategy.scan()
                all_opps.extend(opps)
                for opp in opps:
                    if opp.is_actionable:
                        await self.alerts.send(opp)
            except Exception as e:
                logger.error(f"Strategy {strategy.name} scan error: {e}")

        all_opps.sort(key=lambda o: o.edge_pct, reverse=True)
        self.opportunities = all_opps
        self.scan_count += 1
        self.last_scan = time.time()

    async def run_dashboard(self):
        """Main loop: scan on interval, update dashboard live."""
        self._init_strategies()

        conn = get_connection()
        init_db(conn)
        conn.close()

        console.print("[bold blue]PolyArbitrage Scanner starting...[/bold blue]")
        console.print(f"  Strategies: {[s.name for s in self._strategies]}")
        console.print(f"  Mode: {'PAPER' if self.config.get('risk', {}).get('dry_run', True) else 'LIVE'}")
        console.print()

        with Live(
            build_layout(self.opportunities, self.store, self.scan_count, self.last_scan),
            console=console,
            refresh_per_second=1,
            screen=True,
        ) as live:
            while True:
                try:
                    await self.run_scan()
                except Exception as e:
                    logger.error(f"Scan cycle error: {e}")

                live.update(
                    build_layout(self.opportunities, self.store, self.scan_count, self.last_scan)
                )

                interval = min(
                    self.config.get("strategies", {}).get("negrisk", {}).get("scan_interval_sec", 30),
                    self.config.get("strategies", {}).get("cross_platform", {}).get("poll_interval_sec", 10),
                    self.config.get("strategies", {}).get("sports_ev", {}).get("scan_interval_sec", 30),
                )
                await asyncio.sleep(interval)

    async def shutdown(self):
        await self.gamma.close()


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[logging.FileHandler("polyarbitrage.log"), logging.StreamHandler()],
    )

    config = load_config()
    orchestrator = Orchestrator(config)

    try:
        asyncio.run(orchestrator.run_dashboard())
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down...[/yellow]")
        asyncio.run(orchestrator.shutdown())


if __name__ == "__main__":
    main()
