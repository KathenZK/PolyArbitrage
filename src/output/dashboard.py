"""Rich TUI 仪表盘，用于实时监控延迟套利流水线。"""

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

_DIR_ZH = {"UP": "涨", "DOWN": "跌"}
_SIDE_ZH = {"Up": "涨", "Down": "跌"}
_STATUS_ZH = {
    "pending": "挂单中",
    "partial": "部分成交",
    "filled": "已成交",
    "rejected": "已拒绝",
    "expired": "已过期",
}


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
            open_str = f"  开盘 ${op:>10,.2f}"
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
        text.append("  正在连接币安...", style="dim")
    return Panel(text, title="价格 (币安 vs 锚定价)", border_style="cyan")


def build_signals_table(pipeline: Pipeline) -> Table:
    table = Table(title="信号", expand=True, border_style="yellow")
    table.add_column("时间", width=8)
    table.add_column("品种", width=4)
    table.add_column("来源", width=5)
    table.add_column("方向", width=5)
    table.add_column("偏差", width=7, justify="right")
    table.add_column("胜率", width=6, justify="right")

    for sig in reversed(pipeline.signals[-12:]):
        ts = datetime.fromtimestamp(sig.timestamp).strftime("%H:%M:%S")
        is_up = sig.direction.value == "UP"
        style = "green" if is_up else "red"
        table.add_row(
            ts,
            sig.asset,
            "双源" if sig.price_source == "dual_calibrated" else "币安",
            Text(_DIR_ZH.get(sig.direction.value, sig.direction.value), style=f"bold {style}"),
            Text(f"{sig.deviation_pct:+.2%}", style=style),
            f"{sig.win_prob:.1%}",
        )
    if not pipeline.signals:
        table.add_row("", "", "", Text("等待中...", style="dim"), "", "")
    return table


def build_trades_table(pipeline: Pipeline) -> Table:
    table = Table(title="交易", expand=True, border_style="green")
    table.add_column("时间", width=8)
    table.add_column("品种", width=4)
    table.add_column("方向", width=5)
    table.add_column("状态", width=8)
    table.add_column("成交", width=7, justify="right")
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
        mode = Text("模拟", style="yellow") if t.is_paper else Text("实盘", style="bold red")
        ev_style = "green" if t.submitted_ev > 0 else "red"
        status_zh = _STATUS_ZH.get(t.display_status, t.display_status)
        status_style = "green" if t.display_status == "filled" else "yellow" if t.display_status in {"pending", "partial"} else "red"
        table.add_row(
            ts,
            t.asset,
            Text(_SIDE_ZH.get(t.token_side, t.token_side), style=f"bold {style}"),
            Text(status_zh, style=status_style),
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
    table = Table(title="15分钟市场", expand=True, border_style="magenta")
    table.add_column("品种", width=4)
    table.add_column("看涨", width=6, justify="right")
    table.add_column("看跌", width=6, justify="right")
    table.add_column("锚定价", width=11, justify="right")
    table.add_column("剩余", width=8, justify="right")
    table.add_column("流动性", width=9, justify="right")

    for m in pipeline.registry.all_markets:
        secs = m.secs_remaining
        if secs < 5:
            continue
        sym = m.asset.upper()
        mins, sec = divmod(int(secs), 60)
        remaining = f"{mins}分{sec:02d}秒"
        time_style = "bold red" if secs < 60 else "yellow" if secs < 180 else "dim"
        anchor = m.official_opening_price if m.has_official_opening_price else m.opening_price
        has_anchor = anchor > 0
        open_str = f"${anchor:,.2f}" if has_anchor else "等待中..."
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
        table.add_row("", "", "", "", Text("扫描中...", style="dim"), "")
    return table


def build_status_panel(pipeline: Pipeline) -> Panel:
    uptime = time.time() - pipeline.start_time if pipeline.start_time > 0 else 0
    h, rem = divmod(int(uptime), 3600)
    m, s = divmod(rem, 60)

    text = Text()
    text.append(f" {datetime.now().strftime('%H:%M:%S')}", style="bold")
    text.append(f"  运行 {h:02d}:{m:02d}:{s:02d}\n")
    text.append(f" 行情数:   {pipeline.ticks_count:>10,}\n")
    text.append(f" 信号数:   {pipeline.signals_count:>10}\n")
    text.append(f" 已拦截:   {pipeline.guard.suppressed_count:>10}\n")
    text.append(f" 已通过:   {pipeline.guards_passed:>10}\n")
    text.append(f" 低流动:   {pipeline.executor.skipped_low_liq:>10}\n")
    text.append(f" 无优势:   {pipeline.executor.skipped_no_edge:>10}\n")
    text.append(f" 低期望:   {pipeline.executor.skipped_low_ev:>10}\n")
    text.append(f" 超限额:   {pipeline.executor.skipped_live_limits:>10}\n")
    text.append(f" 挂单中:   {pipeline.executor.pending_count:>10}\n")
    text.append(f" 已成交:   {pipeline.executor.trade_count:>10}\n")
    text.append(f" 成交额:   ${pipeline.executor.total_cost:>9,.2f}\n")
    text.append(f" 占用额:   ${pipeline.executor.total_committed:>9,.2f}\n")
    redeem_status = pipeline.redeemer.status()
    text.append(" 赎回:     ", style="bold")
    if redeem_status.armed:
        text.append("已激活", style="green")
    else:
        text.append("关闭", style="yellow")
    text.append("\n")
    text.append(" 模式:     ", style="bold")
    if pipeline.config.get("risk", {}).get("dry_run", True):
        text.append("模拟", style="bold yellow")
    else:
        text.append("实盘", style="bold red")

    binance_ok = pipeline.stream.connected
    poly_ok = pipeline.registry.market_count > 0
    text.append(f"\n 币安:     ", style="bold")
    text.append("已连接" if binance_ok else "连接中...", style="green" if binance_ok else "red")
    text.append(f"\n 市场:     ", style="bold")
    text.append(
        f"{pipeline.registry.market_count} 个活跃" if poly_ok else "扫描中...",
        style="green" if poly_ok else "yellow",
    )

    return Panel(text, title="状态", border_style="blue")


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
    text.append(f" 交易数:   {len(trades)}\n")
    text.append(f" 已成交:   {len(filled)}\n")
    text.append(" 提交EV:   ", style="bold")
    text.append(f"${total_submitted_ev:+,.2f}\n", style="green" if total_submitted_ev >= 0 else "red")
    text.append(" 匹配EV:   ", style="bold")
    text.append(f"${total_matched_ev:+,.2f}\n", style="green" if total_matched_ev >= 0 else "red")
    text.append(f" 省手续费:  ${total_fee_saved:,.2f}\n")
    text.append(f" 平均胜率:  {avg_p:.1%}\n") if filled else None
    text.append(f" 平均f*:   {avg_fill_prob:.1%}\n") if trades else None

    return Panel(text, title="期望价值", border_style="green")


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
            Text("PolyArbitrage — 延迟套利", style="bold white on blue", justify="center"),
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
