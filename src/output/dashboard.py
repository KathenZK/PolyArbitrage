"""Rich TUI dashboard for real-time monitoring of the latency arb pipeline."""

from __future__ import annotations

import time
from datetime import datetime
from typing import TYPE_CHECKING

from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.output.db import get_connection, get_trade_summary

if TYPE_CHECKING:
    from src.main import Pipeline


def _trunc(s: str, n: int = 50) -> str:
    return s[: n - 2] + ".." if len(s) > n else s


def build_prices_panel(pipeline: Pipeline) -> Panel:
    text = Text()
    for symbol in sorted(pipeline.last_prices):
        price = pipeline.last_prices[symbol]
        label = symbol.upper().replace("USDT", "")

        market = pipeline.registry.get_market(symbol)
        open_str = ""
        dev_str = ""
        if market and market.has_opening_price:
            op = market.opening_price
            dev = (price - op) / op
            open_str = f"  open ${op:>10,.2f}"
            style = "green" if dev >= 0 else "red"
            dev_str = f"  [{style}]{dev:+.2%}[/{style}]"

        text.append(f"  {label:<4}", style="bold cyan")
        text.append(f"${price:>10,.2f}")
        if open_str:
            text.append(open_str, style="dim")
        if dev_str:
            text.append_text(Text.from_markup(dev_str))
        text.append("\n")

    if not pipeline.last_prices:
        text.append("  Connecting to Binance...", style="dim")
    return Panel(text, title="Prices (Binance vs Open)", border_style="cyan")


def build_signals_table(pipeline: Pipeline) -> Table:
    table = Table(title="Signals", expand=True, border_style="yellow")
    table.add_column("Time", width=8)
    table.add_column("Sym", width=4)
    table.add_column("Dir", width=5)
    table.add_column("Now", width=11, justify="right")
    table.add_column("Open", width=11, justify="right")
    table.add_column("Dev", width=7, justify="right")

    for sig in reversed(pipeline.signals[-12:]):
        ts = datetime.fromtimestamp(sig.timestamp).strftime("%H:%M:%S")
        is_up = sig.direction.value == "UP"
        style = "green" if is_up else "red"
        table.add_row(
            ts,
            sig.asset,
            Text(sig.direction.value, style=f"bold {style}"),
            f"${sig.current_price:,.2f}",
            f"${sig.opening_price:,.2f}",
            Text(f"{sig.deviation_pct:+.2%}", style=style),
        )
    if not pipeline.signals:
        table.add_row("", "", Text("waiting...", style="dim"), "", "", "")
    return table


def build_trades_table(pipeline: Pipeline) -> Table:
    table = Table(title="Trades", expand=True, border_style="green")
    table.add_column("Time", width=8)
    table.add_column("Sym", width=4)
    table.add_column("Side", width=5)
    table.add_column("Price", width=7, justify="right")
    table.add_column("Shares", width=7, justify="right")
    table.add_column("Cost", width=9, justify="right")
    table.add_column("Dev", width=7, justify="right")
    table.add_column("", width=6)

    for t in reversed(pipeline.executor.recent_trades[-12:]):
        ts = datetime.fromtimestamp(t.timestamp).strftime("%H:%M:%S")
        is_up = t.direction == "UP"
        style = "green" if is_up else "red"
        mode = Text("PAPER", style="yellow") if t.is_paper else Text("LIVE", style="bold red")
        table.add_row(
            ts,
            t.signal.asset,
            Text(t.token_side, style=f"bold {style}"),
            f"${t.price:.3f}",
            f"{t.shares:.1f}",
            f"${t.cost_usd:.2f}",
            Text(f"{t.signal.deviation_pct:+.2%}", style=style),
            mode,
        )
    if not pipeline.executor.recent_trades:
        table.add_row("", "", "", "", "", "", Text("no trades", style="dim"), "")
    return table


def build_markets_table(pipeline: Pipeline) -> Table:
    table = Table(title="15-Min Markets", expand=True, border_style="magenta")
    table.add_column("Sym", width=4)
    table.add_column("Up", width=6, justify="right")
    table.add_column("Down", width=6, justify="right")
    table.add_column("Open", width=11, justify="right")
    table.add_column("Left", width=7, justify="right")
    table.add_column("Liq", width=9, justify="right")

    for m in pipeline.registry.all_markets:
        secs = m.secs_remaining
        if secs < 5:
            continue
        sym = m.asset.upper()
        mins, sec = divmod(int(secs), 60)
        remaining = f"{mins}m{sec:02d}s"
        time_style = "bold red" if secs < 60 else "yellow" if secs < 180 else "dim"
        open_str = f"${m.opening_price:,.2f}" if m.has_opening_price else "waiting..."
        open_style = "white" if m.has_opening_price else "dim"
        table.add_row(
            sym,
            f"${m.up_price:.3f}",
            f"${m.down_price:.3f}",
            Text(open_str, style=open_style),
            Text(remaining, style=time_style),
            f"${m.liquidity:,.0f}",
        )
    if not pipeline.registry.all_markets:
        table.add_row("", "", "", "", Text("scanning...", style="dim"), "")
    return table


def build_status_panel(pipeline: Pipeline) -> Panel:
    uptime = time.time() - pipeline.start_time if pipeline.start_time > 0 else 0
    h, rem = divmod(int(uptime), 3600)
    m, s = divmod(rem, 60)

    text = Text()
    text.append(f" {datetime.now().strftime('%H:%M:%S')}", style="bold")
    text.append(f"  up {h:02d}:{m:02d}:{s:02d}\n")
    text.append(f" Ticks:    {pipeline.ticks_count:>10,}\n")
    text.append(f" Signals:  {pipeline.signals_count:>10}\n")
    text.append(f" Guarded:  {pipeline.guard.suppressed_count:>10}\n")
    text.append(f" Passed:   {pipeline.guards_passed:>10}\n")
    text.append(f" Trades:   {pipeline.executor.trade_count:>10}\n")
    text.append(f" Spent:    ${pipeline.executor.total_cost:>9,.2f}\n")
    text.append(" Mode:     ", style="bold")
    if pipeline.config.get("risk", {}).get("dry_run", True):
        text.append("PAPER", style="bold yellow")
    else:
        text.append("LIVE", style="bold red")

    binance_ok = pipeline.stream.connected
    poly_ok = pipeline.registry.market_count > 0
    text.append(f"\n Binance:  ", style="bold")
    text.append("connected" if binance_ok else "connecting...", style="green" if binance_ok else "red")
    text.append(f"\n Markets:  ", style="bold")
    text.append(
        f"{pipeline.registry.market_count} active" if poly_ok else "scanning...",
        style="green" if poly_ok else "yellow",
    )

    return Panel(text, title="Status", border_style="blue")


def build_pnl_panel() -> Panel:
    try:
        conn = get_connection()
        summary = get_trade_summary(conn)
        conn.close()
    except Exception:
        return Panel(" DB not ready", title="P&L", border_style="red")

    total = summary.get("total_trades", 0)
    wins = summary.get("wins", 0)
    pnl = summary.get("total_pnl", 0.0) or 0.0
    win_rate = (wins / total * 100) if total > 0 else 0

    text = Text()
    text.append(f" P&L:     ", style="bold")
    text.append(f"${pnl:+,.2f}\n", style="green" if pnl >= 0 else "red")
    text.append(f" Resolved: {total}  Wins: {wins}\n")
    text.append(f" Win Rate: {win_rate:.1f}%\n")

    return Panel(text, title="P&L (Resolved)", border_style="green")


def build_dashboard(pipeline: Pipeline) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top", size=12),
        Layout(name="middle"),
        Layout(name="bottom", size=14),
    )

    layout["header"].update(
        Panel(
            Text("PolyArbitrage — Latency Arb", style="bold white on blue", justify="center"),
            border_style="blue",
        )
    )

    layout["top"].split_row(
        Layout(name="prices", ratio=2),
        Layout(name="markets", ratio=3),
    )
    layout["prices"].update(build_prices_panel(pipeline))
    layout["markets"].update(build_markets_table(pipeline))

    layout["middle"].split_row(
        Layout(name="signals", ratio=2),
        Layout(name="trades", ratio=3),
    )
    layout["signals"].update(build_signals_table(pipeline))
    layout["trades"].update(build_trades_table(pipeline))

    layout["bottom"].split_row(
        Layout(build_status_panel(pipeline), ratio=1),
        Layout(build_pnl_panel(), ratio=1),
    )

    return layout
