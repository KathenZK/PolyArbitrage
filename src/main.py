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
from src.data.market_registry import MarketRegistry
from src.data.polymarket_client import PolymarketGammaClient
from src.execution.redeemer import ProxyRedeemer
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
            official_refresh_interval=strat.get("official_refresh_sec", 60),
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
            require_official_source=strat.get("require_official_source", False),
            official_max_age_secs=strat.get("official_max_age_sec", 90),
            max_source_divergence_pct=strat.get("max_source_divergence_pct", 0.0025),
            source_gap_penalty_mult=strat.get("source_gap_penalty_mult", 8.0),
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
            fill_decay_half_life_hours=strat.get("fill_decay_half_life_hours", 24),
            fill_prior_strength=strat.get("fill_prior_strength", 12),
            fill_confidence_scale=strat.get("fill_confidence_scale", 8),
            fill_lower_bound_z=strat.get("fill_lower_bound_z", 1.0),
            max_live_orders_per_day=config.get("risk", {}).get("max_live_orders_per_day", 0),
            max_live_notional_usd_per_day=config.get("risk", {}).get("max_live_notional_usd_per_day", 0.0),
        )
        redeem_cfg = config.get("redeem", {})
        self.redeemer = ProxyRedeemer(
            self.gamma,
            enabled=redeem_cfg.get("enabled", True),
            poll_interval_secs=redeem_cfg.get("poll_interval_sec", 180),
            tracked_strategy_only=redeem_cfg.get("tracked_strategy_only", True),
            require_auth=redeem_cfg.get("require_auth", True),
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

    @property
    def db_conn(self):
        return self._db_conn

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

    async def _redeem_loop(self):
        while True:
            try:
                await self.redeemer.run_once()
            except Exception as e:
                logger.error(f"Redeem loop error: {e}")
            await asyncio.sleep(self.redeemer.poll_interval)

    async def _heartbeat_loop(self):
        interval = self.config.get("alerts", {}).get("heartbeat_interval_sec", 3600)
        if interval <= 0:
            return
        while True:
            await asyncio.sleep(interval)
            try:
                await self._send_heartbeat()
            except Exception as e:
                logger.warning(f"Heartbeat send failed: {e}")

    async def _send_heartbeat(self):
        dry_run = self.config.get("risk", {}).get("dry_run", True)
        await self.alerts.send_heartbeat(
            uptime_secs=time.time() - self.start_time if self.start_time > 0 else 0,
            ticks=self.ticks_count,
            signals=self.signals_count,
            guards_passed=self.guards_passed,
            trades_filled=self.executor.trade_count,
            trades_pending=self.executor.pending_count,
            skipped_liq=self.executor.skipped_low_liq,
            skipped_edge=self.executor.skipped_no_edge,
            skipped_ev=self.executor.skipped_low_ev,
            skipped_live=self.executor.skipped_live_limits,
            total_cost=self.executor.total_cost,
            markets_active=self.registry.market_count,
            mode="PAPER" if dry_run else "LIVE",
        )

    async def _check_geoblock(self):
        try:
            geo = await self.gamma.check_geoblock()
            blocked = geo.get("blocked", False)
            country = geo.get("country", "?")
            ip = geo.get("ip", "?")
            if blocked:
                console.print(f"[bold red]已封锁: IP {ip} 位于 {country} — 禁止交易[/bold red]")
                console.print("请使用VPN切换到未封锁地区，或切换到模拟模式。")
                return False
            console.print(f"  地区:    {country} ({ip}) — [green]正常[/green]")
            return True
        except Exception as e:
            logger.warning(f"Geoblock check failed: {e}")
            console.print("  地区:    检查失败 — [bold red]阻止实盘启动[/bold red]")
            return False

    async def _run_live_preflight(self) -> bool:
        require_live_arm = self.config.get("risk", {}).get("require_live_arm", True)
        if require_live_arm and os.getenv("LIVE_TRADING_ARMED", "").strip().upper() != "YES":
            console.print("[bold red]实盘交易开关未设置[/bold red]")
            console.print("请在 `.env` 中设置 `LIVE_TRADING_ARMED=YES`，然后再关闭 `dry_run`。")
            return False

        geo_ok = await self._check_geoblock()
        if not geo_ok:
            return False

        report = self.executor.live_preflight()
        if not report.ok:
            console.print("[bold red]实盘预检失败[/bold red]")
            for issue in report.issues:
                console.print(f"  - {issue}")
            return False

        console.print(f"  签名者:   {report.signer_address or '?'}")
        console.print(f"  出资者:   {report.funder_address or '?'}")
        console.print(f"  签名类型: {report.signature_type}")
        console.print(f"  USDC:    ${report.collateral_balance:,.6f}")
        allowance_text = "无限制" if report.max_allowance >= 1_000_000 else f"${report.max_allowance:,.6f}"
        console.print(f"  授权额度: {allowance_text}")
        for warning in report.warnings:
            console.print(f"  警告:    {warning}")

        redeem_report = await self.redeemer.preflight()
        if not redeem_report.ok:
            console.print("[bold red]赎回预检失败[/bold red]")
            for issue in redeem_report.issues:
                console.print(f"  - {issue}")
            return False

        if redeem_report.enabled:
            console.print("  赎回:    已激活")
            console.print(f"  所有者:   {redeem_report.owner or '?'}")
            console.print(f"  代理:    {redeem_report.derived_proxy or '?'}")
            if redeem_report.relay_address:
                console.print(f"  中继:    {redeem_report.relay_address} (nonce {redeem_report.relay_nonce or '0'})")
            for warning in redeem_report.warnings:
                console.print(f"  警告:    {warning}")
        else:
            console.print("  赎回:    配置中已禁用")
        return True

    async def run(self):
        dry_run = self.config.get("risk", {}).get("dry_run", True)
        symbols = self.config.get("strategy", {}).get("symbols", ["btcusdt"])
        self.start_time = time.time()

        console.print("[bold blue]PolyArbitrage — 延迟套利系统[/bold blue]")
        console.print(f"  模式:    {'模拟' if dry_run else '[bold red]实盘[/bold red]'}")
        console.print(f"  品种:    {[s.replace('usdt','').upper() for s in symbols]}")
        console.print(f"  下注:    ${self.config.get('strategy', {}).get('bet_size_usd', 15)}/笔")

        if not dry_run:
            if not await self._run_live_preflight():
                return
        else:
            console.print(f"  地区:    已跳过（模拟模式）")

        console.print()

        self._db_conn = get_connection()
        init_db(self._db_conn)
        self.executor.attach_db(self._db_conn)
        self.redeemer.attach_db(self._db_conn)

        registry_task = asyncio.create_task(self.registry.run())
        await self.registry.refresh()
        self.executor.bootstrap_pending_orders()
        self.executor.bootstrap_wallet_orders()
        await self.executor.reconcile_pending_orders(force=True)
        self.redeemer.attach_clob(self.executor._clob)

        stream_task = asyncio.create_task(self.stream.run())
        reconcile_task = asyncio.create_task(self._reconcile_loop())
        redeem_task = asyncio.create_task(self._redeem_loop()) if not dry_run else None
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

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
            if redeem_task is not None:
                redeem_task.cancel()
            heartbeat_task.cancel()

    async def run_headless(self):
        dry_run = self.config.get("risk", {}).get("dry_run", True)
        symbols = self.config.get("strategy", {}).get("symbols", ["btcusdt"])
        self.start_time = time.time()

        if not dry_run:
            if not await self._run_live_preflight():
                logger.error("Live preflight failed")
                return
        logger.info("Pipeline started (headless)")

        self._db_conn = get_connection()
        init_db(self._db_conn)
        self.executor.attach_db(self._db_conn)
        self.redeemer.attach_db(self._db_conn)

        registry_task = asyncio.create_task(self.registry.run())
        await self.registry.refresh()
        self.executor.bootstrap_pending_orders()
        self.executor.bootstrap_wallet_orders()
        await self.executor.reconcile_pending_orders(force=True)
        self.redeemer.attach_clob(self.executor._clob)
        reconcile_task = asyncio.create_task(self._reconcile_loop())
        redeem_task = asyncio.create_task(self._redeem_loop()) if not dry_run else None
        heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        await self.alerts.send_startup(
            mode="PAPER" if dry_run else "LIVE",
            symbols=[s.replace("usdt", "").upper() for s in symbols],
        )

        try:
            await self.stream.run()
        finally:
            registry_task.cancel()
            reconcile_task.cancel()
            if redeem_task is not None:
                redeem_task.cancel()
            heartbeat_task.cancel()
            if self._db_conn is not None:
                self._db_conn.close()
                self._db_conn = None

    async def shutdown(self):
        self.stream.stop()
        self.registry.stop()
        await self.alerts.close()
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
        console.print("\n[yellow]正在关闭...[/yellow]")
        asyncio.run(pipeline.shutdown())


if __name__ == "__main__":
    main()
