"""PolyArbitrage — Binance-Polymarket latency arbitrage pipeline.

Architecture:
  Binance aggTrade WS  →  PriceComparator (vs opening price)  →  SignalGuard  →  Executor
  MarketRegistry (slug-based, window-aligned opening price with tick buffer)

Opening price tracking:
  Every Binance tick is buffered by the registry. When a new 15-min window
  starts, the registry picks the buffered tick closest to window start as
  the opening price reference — using the tick's own timestamp, not the
  local clock.
"""

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

from src.data.binance_stream import BinanceStream, Tick
from src.data.market_registry import MarketRegistry, current_window_start
from src.data.polymarket_client import PolymarketGammaClient
from src.output.alerts import DingTalkAlert
from src.output.db import get_connection, init_db
from src.output.dashboard import build_dashboard
from src.strategies.executor import Executor
from src.strategies.momentum import PriceComparator, Signal
from src.strategies.signal_guard import SignalGuard

console = Console()
logger = logging.getLogger("polyarbitrage")


def load_config() -> dict:
    cfg_path = Path(__file__).resolve().parent.parent / "config.yaml"
    with open(cfg_path) as f:
        return yaml.safe_load(f)


class Pipeline:
    """Wires Binance stream → PriceComparator → SignalGuard → Executor."""

    def __init__(self, config: dict):
        self.config = config
        strat = config.get("strategy", {})

        assets = [a.replace("usdt", "") for a in strat.get("symbols", ["btcusdt", "ethusdt", "solusdt"])]

        self.gamma = PolymarketGammaClient()
        self.registry = MarketRegistry(
            self.gamma,
            assets=assets,
            refresh_interval=strat.get("registry_refresh_sec", 15),
            min_liquidity=strat.get("min_liquidity", 1000),
        )
        annual_vols = {}
        for sym in strat.get("symbols", []):
            vol_key = f"annual_vol_{sym}"
            if vol_key in strat:
                annual_vols[sym] = strat[vol_key]

        self.comparator = PriceComparator(
            registry=self.registry,
            threshold_pct=strat.get("edge_threshold_pct", 0.003),
            min_secs_remaining=strat.get("min_secs_remaining", 30),
            min_secs_elapsed=strat.get("min_secs_elapsed", 30),
            annual_vols=annual_vols if annual_vols else None,
        )
        self.guard = SignalGuard(
            cooldown_secs=strat.get("signal_cooldown_sec", 120),
        )
        self.executor = Executor(
            bet_size_usd=strat.get("bet_size_usd", 15),
            dry_run=config.get("risk", {}).get("dry_run", True),
            min_liquidity=strat.get("min_liquidity", 1000),
            min_ev_usd=strat.get("min_ev_usd", 0.10),
            maker_offset_ticks=strat.get("maker_offset_ticks", 1),
            adverse_selection_haircut=strat.get("adverse_selection_haircut", 0.05),
            fill_rate_prior=strat.get("fill_rate_prior", 0.35),
            fill_min_samples=strat.get("fill_min_samples", 20),
            fill_lookback_hours=strat.get("fill_lookback_hours", 168),
        )

        self.registry.register_window_change_callback(self.guard.on_window_change)

        alert_cfg = config.get("alerts", {})
        self.alerts = DingTalkAlert(
            webhook_url=alert_cfg.get("dingtalk_webhook", "") or os.getenv("DINGTALK_WEBHOOK", ""),
            keyword=alert_cfg.get("dingtalk_keyword", "PolyGod"),
        )

        symbols = strat.get("symbols", ["btcusdt", "ethusdt", "solusdt"])
        self.stream = BinanceStream(symbols=symbols, on_tick=self._on_tick)

        self.signals: list[Signal] = []
        self.ticks_count = 0
        self.signals_count = 0
        self.guards_passed = 0
        self.last_prices: dict[str, float] = {}
        self.start_time = 0.0
        self._db_conn = None

    async def _on_tick(self, tick: Tick):
        self.ticks_count += 1
        self.last_prices[tick.symbol] = tick.price

        self.registry.buffer_tick(tick.symbol, tick.price, tick.timestamp)
        self.registry.record_opening_price(tick.symbol, tick.price, tick.timestamp)

        signal = self.comparator.check(tick.symbol, tick.price, tick.timestamp)
        if signal is None:
            return

        self.signals_count += 1
        self.signals.append(signal)
        if len(self.signals) > 200:
            self.signals = self.signals[-200:]

        if not self.guard.should_trade(signal):
            return

        self.guards_passed += 1
        asyncio.create_task(self._execute(signal))

    async def _execute(self, signal: Signal):
        try:
            result = await self.executor.execute(signal)
            if result:
                logger.info(
                    f"Trade #{result.order_id}: {result.direction} {signal.asset} "
                    f"buy {result.token_side} ${result.cost_usd:.2f}"
                )
                await self.alerts.send_trade(
                    symbol=signal.asset,
                    direction=result.direction,
                    price=result.price,
                    shares=result.shares,
                    cost=result.cost_usd,
                    momentum=signal.deviation_pct,
                    market_question=result.market.question,
                    is_paper=result.is_paper,
                    order_id=result.order_id,
                )
        except Exception as e:
            logger.error(f"Execution error: {e}")

    async def _reconcile_loop(self):
        while True:
            try:
                await self.executor.reconcile_pending_orders()
            except Exception as e:
                logger.error(f"Reconcile error: {e}")
            await asyncio.sleep(1.0)

    async def _check_geoblock(self):
        try:
            geo = await self.gamma.check_geoblock()
            blocked = geo.get("blocked", False)
            country = geo.get("country", "?")
            ip = geo.get("ip", "?")
            if blocked:
                console.print(f"[bold red]BLOCKED: IP {ip} in {country} — trading not allowed[/bold red]")
                console.print("Use a VPN to a non-blocked region, or switch to paper mode.")
                return False
            console.print(f"  Geo:     {country} ({ip}) — [green]OK[/green]")
            return True
        except Exception as e:
            logger.warning(f"Geoblock check failed: {e} (proceeding anyway)")
            console.print(f"  Geo:     check failed — proceeding")
            return True

    async def run(self):
        dry_run = self.config.get("risk", {}).get("dry_run", True)
        symbols = self.config.get("strategy", {}).get("symbols", ["btcusdt"])
        self.start_time = time.time()

        console.print("[bold blue]PolyArbitrage — Latency Arb Pipeline[/bold blue]")
        console.print(f"  Mode:    {'PAPER' if dry_run else '[bold red]LIVE[/bold red]'}")
        console.print(f"  Symbols: {[s.replace('usdt','').upper() for s in symbols]}")
        console.print(f"  Bet:     ${self.config.get('strategy', {}).get('bet_size_usd', 15)}/trade")

        if not dry_run:
            geo_ok = await self._check_geoblock()
            if not geo_ok:
                return
        else:
            console.print(f"  Geo:     skipped (paper mode)")

        console.print()

        self._db_conn = get_connection()
        init_db(self._db_conn)
        self.executor.attach_db(self._db_conn)

        registry_task = asyncio.create_task(self.registry.run())
        await self.registry.refresh()
        self.executor.bootstrap_pending_orders()
        await self.executor.reconcile_pending_orders(force=True)

        stream_task = asyncio.create_task(self.stream.run())
        reconcile_task = asyncio.create_task(self._reconcile_loop())

        await self.alerts.send_startup(
            mode="PAPER" if dry_run else "LIVE",
            symbols=[s.replace("usdt", "").upper() for s in symbols],
        )

        try:
            with Live(console=console, refresh_per_second=2, screen=True) as live:
                while True:
                    live.update(build_dashboard(self))
                    await asyncio.sleep(0.5)
        except asyncio.CancelledError:
            pass
        finally:
            registry_task.cancel()
            stream_task.cancel()
            reconcile_task.cancel()

    async def run_headless(self):
        self.start_time = time.time()
        logger.info("Pipeline started (headless)")

        self._db_conn = get_connection()
        init_db(self._db_conn)
        self.executor.attach_db(self._db_conn)

        registry_task = asyncio.create_task(self.registry.run())
        await self.registry.refresh()
        self.executor.bootstrap_pending_orders()
        await self.executor.reconcile_pending_orders(force=True)
        reconcile_task = asyncio.create_task(self._reconcile_loop())

        try:
            await self.stream.run()
        finally:
            registry_task.cancel()
            reconcile_task.cancel()

    async def shutdown(self):
        self.stream.stop()
        self.registry.stop()
        await self.gamma.close()
        if self._db_conn is not None:
            self._db_conn.close()
            self._db_conn = None


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        handlers=[logging.FileHandler("polyarbitrage.log"), logging.StreamHandler()],
    )

    config = load_config()
    pipeline = Pipeline(config)

    mode = sys.argv[1] if len(sys.argv) > 1 else "live"

    try:
        if mode == "headless":
            asyncio.run(pipeline.run_headless())
        else:
            asyncio.run(pipeline.run())
    except KeyboardInterrupt:
        console.print("\n[yellow]Shutting down...[/yellow]")
        asyncio.run(pipeline.shutdown())


if __name__ == "__main__":
    main()
