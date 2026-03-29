"""Rich TUI dashboard for live monitoring of arbitrage opportunities."""

from __future__ import annotations

import time
from datetime import datetime, timezone

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.data.market_store import MarketStore
from src.output.db import get_connection, get_open_trades, get_trade_summary
from src.strategies.base import Action, Opportunity

console = Console()


def _format_edge(edge: float) -> Text:
    if edge >= 5.0:
        return Text(f"{edge:.2f}%", style="bold green")
    elif edge >= 3.0:
        return Text(f"{edge:.2f}%", style="green")
    elif edge >= 1.0:
        return Text(f"{edge:.2f}%", style="yellow")
    else:
        return Text(f"{edge:.2f}%", style="dim")


def _format_action(action: Action) -> Text:
    styles = {
        Action.BUY_ALL_YES: ("BUY ALL YES", "bold cyan"),
        Action.BUY_ALL_NO: ("BUY ALL NO", "bold magenta"),
        Action.BUY_YES: ("BUY YES", "cyan"),
        Action.BUY_NO: ("BUY NO", "magenta"),
        Action.CROSS_ARB: ("CROSS ARB", "bold yellow"),
        Action.SKIP: ("SKIP", "dim"),
    }
    label, style = styles.get(action, (str(action), "white"))
    return Text(label, style=style)


def _truncate(s: str, max_len: int = 45) -> str:
    return s[:max_len - 2] + ".." if len(s) > max_len else s


def build_opportunities_table(opportunities: list[Opportunity]) -> Table:
    table = Table(title="Live Opportunities", expand=True, border_style="blue")
    table.add_column("#", width=3, justify="right")
    table.add_column("Strategy", width=14)
    table.add_column("Event", width=42)
    table.add_column("Action", width=14)
    table.add_column("Edge", width=8, justify="right")
    table.add_column("YES Sum", width=8, justify="right")
    table.add_column("Outcomes", width=8, justify="right")
    table.add_column("Volume", width=12, justify="right")
    table.add_column("Settles", width=12)

    for i, opp in enumerate(opportunities[:20], 1):
        details = opp.details
        yes_sum = details.get("yes_sum", "")
        yes_sum_str = f"${yes_sum}" if yes_sum else ""
        vol = details.get("total_volume", 0)
        vol_str = f"${vol:,.0f}" if vol else ""
        outcomes = details.get("outcome_count", "")

        settle = ""
        if opp.settlement_date:
            try:
                dt = datetime.fromisoformat(opp.settlement_date.replace("Z", "+00:00"))
                days = (dt - datetime.now(timezone.utc)).days
                settle = f"{days}d" if days >= 0 else "expired"
            except (ValueError, TypeError):
                settle = opp.settlement_date[:10]

        table.add_row(
            str(i),
            opp.strategy[:14],
            _truncate(opp.event_title),
            _format_action(opp.action),
            _format_edge(opp.edge_pct),
            yes_sum_str,
            str(outcomes),
            vol_str,
            settle,
        )

    if not opportunities:
        table.add_row("", "", Text("No opportunities detected", style="dim"), "", "", "", "", "", "")

    return table


def build_negrisk_detail_table(store: MarketStore) -> Table:
    """Show all NegRisk events and their YES sums."""
    events = store.get_negrisk_events()
    events.sort(key=lambda e: abs(e.deviation), reverse=True)

    table = Table(title="NegRisk Events (by deviation)", expand=True, border_style="cyan")
    table.add_column("Event", width=45)
    table.add_column("Outcomes", width=8, justify="right")
    table.add_column("YES Sum", width=10, justify="right")
    table.add_column("Deviation", width=10, justify="right")
    table.add_column("Volume", width=12, justify="right")

    for e in events[:15]:
        dev = e.deviation
        if abs(dev) >= 0.03:
            dev_style = "bold green" if dev > 0 else "bold red"
        elif abs(dev) >= 0.01:
            dev_style = "yellow"
        else:
            dev_style = "dim"

        table.add_row(
            _truncate(e.title),
            str(e.outcome_count),
            f"${e.yes_price_sum:.4f}",
            Text(f"{dev:+.4f}", style=dev_style),
            f"${e.total_volume:,.0f}",
        )

    return table


def build_pnl_panel() -> Panel:
    try:
        conn = get_connection()
        summary = get_trade_summary(conn)
        open_trades = get_open_trades(conn)
        conn.close()
    except Exception:
        return Panel("DB not initialized", title="P&L", border_style="red")

    total = summary.get("total_trades", 0)
    wins = summary.get("wins", 0)
    pnl = summary.get("total_pnl", 0.0) or 0.0
    win_rate = (wins / total * 100) if total > 0 else 0

    pnl_style = "green" if pnl >= 0 else "red"
    text = Text()
    text.append(f"Total P&L: ", style="bold")
    text.append(f"${pnl:+,.2f}\n", style=pnl_style)
    text.append(f"Trades: {total}  Wins: {wins}  Win Rate: {win_rate:.1f}%\n")
    text.append(f"Open positions: {len(open_trades)}\n")

    return Panel(text, title="Portfolio", border_style="green")


def build_status_panel(scan_count: int, last_scan: float) -> Panel:
    now = time.time()
    elapsed = now - last_scan if last_scan > 0 else 0
    ts = datetime.now().strftime("%H:%M:%S")

    text = Text()
    text.append(f"Time: {ts}\n", style="bold")
    text.append(f"Scans: {scan_count}\n")
    text.append(f"Last scan: {elapsed:.1f}s ago\n")
    text.append("Mode: ", style="bold")
    text.append("PAPER TRADING", style="bold yellow")

    return Panel(text, title="Status", border_style="yellow")


def build_layout(
    opportunities: list[Opportunity],
    store: MarketStore,
    scan_count: int = 0,
    last_scan: float = 0.0,
) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="body"),
        Layout(name="footer", size=8),
    )

    layout["header"].update(
        Panel(
            Text("PolyArbitrage Scanner", style="bold white on blue", justify="center"),
            border_style="blue",
        )
    )

    layout["body"].split_row(
        Layout(name="opportunities", ratio=3),
        Layout(name="negrisk", ratio=2),
    )
    layout["opportunities"].update(build_opportunities_table(opportunities))
    layout["negrisk"].update(build_negrisk_detail_table(store))

    layout["footer"].split_row(
        Layout(build_pnl_panel(), ratio=2),
        Layout(build_status_panel(scan_count, last_scan), ratio=1),
    )

    return layout
