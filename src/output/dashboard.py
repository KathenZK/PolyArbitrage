"""Rich TUI dashboard for real-time monitoring of the latency arb pipeline."""

from __future__ import annotations

import time
from datetime import datetime
from typing import TYPE_CHECKING

from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

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
        if market and (market.has_official_opening_price or market.has_opening_price):
            op = market.official_opening_price if market.has_official_opening_price else market.opening_price
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
    return Panel(text, title="Prices (Binance vs Anchor)", border_style="cyan")


def build_signals_table(pipeline: Pipeline) -> Table:
    table = Table(title="Signals", expand=True, border_style="yellow")
    table.add_column("Time", width=8)
    table.add_column("Sym", width=4)
    table.add_column("Src", width=5)
    table.add_column("Dir", width=5)
    table.add_column("Dev", width=7, justify="right")
    table.add_column("p", width=6, justify="right")

    for sig in reversed(pipeline.signals[-12:]):
        ts = datetime.fromtimestamp(sig.timestamp).strftime("%H:%M:%S")
        is_up = sig.direction.value == "UP"
        style = "green" if is_up else "red"
        table.add_row(
            ts,
            sig.asset,
            "DUAL" if sig.price_source == "dual_calibrated" else "BIN",
            Text(sig.direction.value, style=f"bold {style}"),
            Text(f"{sig.deviation_pct:+.2%}", style=style),
            f"{sig.win_prob:.1%}",
        )
    if not pipeline.signals:
        table.add_row("", "", "", Text("waiting...", style="dim"), "", "")
    return table


def build_trades_table(pipeline: Pipeline) -> Table:
    table = Table(title="Trades", expand=True, border_style="green")
    table.add_column("Time", width=8)
    table.add_column("Sym", width=4)
    table.add_column("Side", width=5)
    table.add_column("Stat", width=8)
    table.add_column("Fill", width=7, justify="right")
    table.add_column("f*", width=5, justify="right")
    table.add_column("q", width=5, justify="right")
    table.add_column("p", width=5, justify="right")
    table.add_column("EV", width=6, justify="right")
    table.add_column("$", width=6, justify="right")
    table.add_column("", width=6)

    for t in reversed(pipeline.executor.recent_trades[-12:]):
        ts = datetime.fromtimestamp(t.timestamp).strftime("%H:%M:%S")
        is_up = t.direction == "UP"
        style = "green" if is_up else "red"
        mode = Text("PAPER", style="yellow") if t.is_paper else Text("LIVE", style="bold red")
        ev_style = "green" if t.submitted_ev > 0 else "red"
        status_style = "green" if t.display_status == "filled" else "yellow" if t.display_status in {"pending", "partial"} else "red"
        table.add_row(
            ts,
            t.asset,
            Text(t.token_side, style=f"bold {style}"),
            Text(t.display_status, style=status_style),
            f"{t.matched_ratio:.0%}",
            f"{t.fill_ratio_lower_bound:.0%}",
            f"{t.price:.2f}",
            f"{t.win_prob:.0%}",
            Text(f"${t.submitted_ev:.2f}", style=ev_style),
            f"${t.matched_cost_usd:.0f}",
            mode,
        )
    if not pipeline.executor.recent_trades:
        table.add_row("", "", "", "", "", "", "", "", "", Text("--", style="dim"), "")
    return table


def build_markets_table(pipeline: Pipeline) -> Table:
    table = Table(title="15-Min Markets", expand=True, border_style="magenta")
    table.add_column("Sym", width=4)
    table.add_column("Up", width=6, justify="right")
    table.add_column("Down", width=6, justify="right")
    table.add_column("Beat", width=11, justify="right")
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
        anchor = m.official_opening_price if m.has_official_opening_price else m.opening_price
        has_anchor = anchor > 0
        open_str = f"${anchor:,.2f}" if has_anchor else "waiting..."
        open_style = "white" if has_anchor else "dim"
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
    text.append(f" SkipLiq:  {pipeline.executor.skipped_low_liq:>10}\n")
    text.append(f" SkipEdge: {pipeline.executor.skipped_no_edge:>10}\n")
    text.append(f" SkipEV:   {pipeline.executor.skipped_low_ev:>10}\n")
    text.append(f" SkipLive: {pipeline.executor.skipped_live_limits:>10}\n")
    text.append(f" Pending:  {pipeline.executor.pending_count:>10}\n")
    text.append(f" Filled:   {pipeline.executor.trade_count:>10}\n")
    text.append(f" Filled$:  ${pipeline.executor.total_cost:>9,.2f}\n")
    text.append(f" Commit$:  ${pipeline.executor.total_committed:>9,.2f}\n")
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


def build_ev_panel(pipeline: Pipeline) -> Panel:
    trades = pipeline.executor.recent_trades
    filled = [t for t in trades if t.matched_shares > 0 or t.status.value == "filled"]
    total_submitted_ev = sum(t.submitted_ev for t in trades)
    total_matched_ev = sum(t.realized_ev for t in filled)
    total_fee_saved = sum(t.taker_fee_avoided * t.matched_ratio for t in filled)
    matched_weight = sum(t.matched_ratio for t in filled)
    avg_p = sum(t.win_prob * t.matched_ratio for t in filled) / matched_weight if matched_weight > 0 else 0
    avg_fill_prob = sum(t.fill_ratio_lower_bound for t in trades) / len(trades) if trades else 0

    text = Text()
    text.append(f" Trades:     {len(trades)}\n")
    text.append(f" Filled:     {len(filled)}\n")
    text.append(" Sub EV:    ", style="bold")
    text.append(f"${total_submitted_ev:+,.2f}\n", style="green" if total_submitted_ev >= 0 else "red")
    text.append(" Match EV:  ", style="bold")
    text.append(f"${total_matched_ev:+,.2f}\n", style="green" if total_matched_ev >= 0 else "red")
    text.append(f" Fee saved: ${total_fee_saved:,.2f}\n")
    text.append(f" Avg p:     {avg_p:.1%}\n") if filled else None
    text.append(f" Avg f*:    {avg_fill_prob:.1%}\n") if trades else None

    return Panel(text, title="Expected Value", border_style="green")


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
        Layout(build_ev_panel(pipeline), ratio=1),
    )

    return layout
