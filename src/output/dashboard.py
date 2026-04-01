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


def _truncate(s: str, max_len: int = 50) -> str:
    return s[: max_len - 2] + ".." if len(s) > max_len else s


def build_prices_panel(pipeline: Pipeline) -> Panel:
    text = Text()
    for symbol in sorted(pipeline.last_prices):
        price = pipeline.last_prices[symbol]
        label = symbol.upper().replace("USDT", "")
        text.append(f"  {label:<5}", style="bold cyan")
        text.append(f"${price:>10,.2f}\n", style="white")
    if not pipeline.last_prices:
        text.append("  Connecting to Binance...", style="dim")
    return Panel(text, title="Prices (Binance)", border_style="cyan")


def build_signals_table(pipeline: Pipeline) -> Table:
    table = Table(title="Signals", expand=True, border_style="yellow")
    table.add_column("Time", width=8)
    table.add_column("Sym", width=5)
    table.add_column("Dir", width=5)
    table.add_column("Price", width=11, justify="right")
    table.add_column("Move", width=8, justify="right")

    for sig in reversed(pipeline.signals[-12:]):
        ts = datetime.fromtimestamp(sig.timestamp).strftime("%H:%M:%S")
        is_up = sig.direction.value == "UP"
        style = "green" if is_up else "red"
        sym = sig.symbol.upper().replace("USDT", "")
        table.add_row(
            ts,
            sym,
            Text(sig.direction.value, style=f"bold {style}"),
            f"${sig.price:,.2f}",
            Text(f"{sig.momentum_pct:+.2%}", style=style),
        )
    if not pipeline.signals:
        table.add_row("", "", Text("waiting...", style="dim"), "", "")
    return table


def build_trades_table(pipeline: Pipeline) -> Table:
    table = Table(title="Trades", expand=True, border_style="green")
    table.add_column("Time", width=8)
    table.add_column("Sym", width=5)
    table.add_column("Dir", width=5)
    table.add_column("Price", width=7, justify="right")
    table.add_column("Shares", width=7, justify="right")
    table.add_column("Cost", width=9, justify="right")
    table.add_column("Market", width=38)
    table.add_column("", width=6)

    for trade in reversed(pipeline.executor.recent_trades[-12:]):
        ts = datetime.fromtimestamp(trade.timestamp).strftime("%H:%M:%S")
        is_up = trade.direction == "UP"
        style = "green" if is_up else "red"
        mode = Text("PAPER", style="yellow") if trade.is_paper else Text("LIVE", style="bold red")
        sym = trade.signal.symbol.upper().replace("USDT", "")
        table.add_row(
            ts,
            sym,
            Text(trade.direction, style=f"bold {style}"),
            f"${trade.price:.3f}",
            f"{trade.shares:.1f}",
            f"${trade.cost_usd:.2f}",
            _truncate(trade.market.question, 38),
            mode,
        )
    if not pipeline.executor.recent_trades:
        table.add_row("", "", "", "", "", "", Text("no trades yet", style="dim"), "")
    return table


def build_markets_table(pipeline: Pipeline) -> Table:
    table = Table(title="Active Markets", expand=True, border_style="magenta")
    table.add_column("Sym", width=5)
    table.add_column("Dir", width=5)
    table.add_column("YES", width=7, justify="right")
    table.add_column("NO", width=7, justify="right")
    table.add_column("Left", width=7, justify="right")
    table.add_column("Question", width=45)

    markets = sorted(pipeline.registry.all_markets, key=lambda m: m.secs_remaining)
    for m in markets:
        secs = m.secs_remaining
        if secs < 5:
            continue
        sym = m.symbol.upper().replace("USDT", "")
        dir_style = "green" if m.direction == "UP" else "red"
        mins, sec = divmod(int(secs), 60)
        remaining = f"{mins}m{sec:02d}s"
        time_style = "bold red" if secs < 60 else "yellow" if secs < 180 else "dim"
        table.add_row(
            sym,
            Text(m.direction, style=dir_style),
            f"${m.yes_price:.3f}",
            f"${m.no_price:.3f}",
            Text(remaining, style=time_style),
            _truncate(m.question, 45),
        )
    if not markets:
        table.add_row("", "", "", "", "", Text("scanning...", style="dim"))
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
        Layout(name="top", size=14),
        Layout(name="middle"),
        Layout(name="bottom", size=14),
    )

    layout["header"].update(
        Panel(
            Text(
                "PolyArbitrage — Latency Arb Pipeline",
                style="bold white on blue",
                justify="center",
            ),
            border_style="blue",
        )
    )

    layout["top"].split_row(
        Layout(name="prices", ratio=1),
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
