"""Rich TUI 仪表盘，用于实时监控双源校准交易流水线。"""

from __future__ import annotations

import time
from datetime import datetime
from typing import TYPE_CHECKING, Any

from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from src.output.db import get_fill_rate_stats, get_live_daily_usage

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
_ORDER_SIDE_ZH = {"BUY": "买", "SELL": "卖"}

_db_cache: dict[str, tuple[float, Any]] = {}
_DB_CACHE_TTL = 5.0


def _cached_query(key: str, conn, fn, **kwargs) -> Any:
    now = time.time()
    cached = _db_cache.get(key)
    if cached and now - cached[0] < _DB_CACHE_TTL:
        return cached[1]
    try:
        result = fn(conn, **kwargs)
    except Exception:
        return cached[1] if cached else None
    _db_cache[key] = (now, result)
    return result


def _trunc(s: str, n: int = 50) -> str:
    return s[: n - 2] + ".." if len(s) > n else s


def _market_source_text(market) -> Text:
    if market.has_official_calibration and market.official_calibration_age < 90:
        return Text("双源", style="green")
    if market.has_official_opening_price and market.has_opening_price:
        return Text("锚定降级", style="yellow")
    if market.has_opening_price:
        return Text("币安", style="dim")
    return Text("--", style="dim")


def _signal_source_label(price_source: str) -> str:
    if price_source == "dual_calibrated":
        return "双源"
    if price_source == "official_anchor_fast_return":
        return "锚定降级"
    return "币安"


# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------

def build_header(pipeline: Pipeline) -> Panel:
    strat = pipeline.config.get("strategy", {})
    dry_run = pipeline.config.get("risk", {}).get("dry_run", True)

    uptime = time.time() - pipeline.start_time if pipeline.start_time > 0 else 0
    h, rem = divmod(int(uptime), 3600)
    m, s = divmod(rem, 60)

    symbols = "/".join(
        sym.replace("usdt", "").upper() for sym in strat.get("symbols", [])
    )
    bet = strat.get("bet_size_usd", 15)
    threshold = strat.get("edge_threshold_pct", 0.003)
    min_ev = strat.get("min_ev_usd", 0.10)

    t = Text(justify="center")
    t.append("PolyArbitrage — 双源校准策略", style="bold white")
    t.append(" | ", style="dim")
    t.append("模拟" if dry_run else "实盘", style="bold yellow" if dry_run else "bold red")
    t.append(" | ", style="dim")
    t.append(symbols, style="cyan")
    t.append(" | ", style="dim")
    t.append(f"${bet}/笔", style="white")
    t.append(f" 阈值{threshold:.1%}", style="white")
    t.append(f" EV≥${min_ev:.2f}", style="white")
    t.append(" | ", style="dim")
    t.append(f"{h:02d}:{m:02d}:{s:02d}", style="green")

    return Panel(t, border_style="blue")


# ---------------------------------------------------------------------------
# Prices
# ---------------------------------------------------------------------------

def build_prices_panel(pipeline: Pipeline) -> Panel:
    text = Text()
    for symbol in sorted(pipeline.last_prices):
        price = pipeline.last_prices[symbol]
        label = symbol.upper().replace("USDT", "")

        market = pipeline.registry.get_market(symbol)
        open_str = ""
        dev_str = ""
        source_tag = ""
        source_style = "dim"

        if market:
            source_label = _market_source_text(market).plain
            if source_label == "双源":
                source_tag = " (双源)"
                source_style = "green"
            elif source_label == "锚定降级":
                source_tag = " (锚定降级)"
                source_style = "yellow"
            elif source_label == "币安":
                source_tag = " (币安)"
                source_style = "yellow"
            else:
                source_tag = " (---)"

            if market.has_official_opening_price or market.has_opening_price:
                op = (
                    market.official_opening_price
                    if market.has_official_opening_price
                    else market.opening_price
                )
                dev = (price - op) / op
                open_str = f"  开盘${op:>10,.2f}"
                style = "green" if dev >= 0 else "red"
                dev_str = f" [{style}]{dev:+.2%}[/{style}]"

        text.append(f"  {label:<4}", style="bold cyan")
        text.append(f"${price:>10,.2f}")
        if open_str:
            text.append(open_str, style="dim")
        if dev_str:
            text.append_text(Text.from_markup(dev_str))
        if source_tag:
            text.append(source_tag, style=source_style)
        text.append("\n")

    if not pipeline.last_prices:
        text.append("  正在连接币安...", style="dim")
    return Panel(text, title="价格 (币安 vs 锚定价)", border_style="cyan")


# ---------------------------------------------------------------------------
# Markets
# ---------------------------------------------------------------------------

def build_markets_table(pipeline: Pipeline) -> Table:
    table = Table(title="15分钟市场", expand=True, border_style="magenta")
    table.add_column("品种", width=4)
    table.add_column("看涨", width=6, justify="right")
    table.add_column("看跌", width=6, justify="right")
    table.add_column("价差", width=5, justify="right")
    table.add_column("锚定价", width=11, justify="right")
    table.add_column("源", width=10, justify="center")
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

        anchor = (
            m.official_opening_price
            if m.has_official_opening_price
            else m.opening_price
        )
        has_anchor = anchor > 0
        open_str = f"${anchor:,.2f}" if has_anchor else "等待中..."
        open_style = "white" if has_anchor else "dim"

        spread = m.spread if m.spread > 0 else max(0.0, m.best_ask - m.best_bid)
        if spread > 0:
            spread_str = f"{spread:.2f}"
            spread_style = (
                "green" if spread <= 0.02 else "yellow" if spread <= 0.04 else "red"
            )
        else:
            spread_str = "--"
            spread_style = "dim"

        src = _market_source_text(m)

        table.add_row(
            sym,
            f"${m.up_price:.3f}",
            f"${m.down_price:.3f}",
            Text(spread_str, style=spread_style),
            Text(open_str, style=open_style),
            src,
            Text(remaining, style=time_style),
            f"${m.liquidity:,.0f}",
        )
    if not pipeline.registry.all_markets:
        table.add_row("", "", "", "", "", "", Text("扫描中...", style="dim"), "")
    return table


# ---------------------------------------------------------------------------
# Signals
# ---------------------------------------------------------------------------

def build_signals_table(pipeline: Pipeline) -> Table:
    table = Table(title="信号", expand=True, border_style="yellow")
    table.add_column("时间", width=8)
    table.add_column("品种", width=4)
    table.add_column("来源", width=10)
    table.add_column("方向", width=5)
    table.add_column("偏差", width=7, justify="right")
    table.add_column("胜率", width=6, justify="right")
    table.add_column("双源差", width=7, justify="right")

    for sig in reversed(pipeline.signals[-12:]):
        ts = datetime.fromtimestamp(sig.timestamp).strftime("%H:%M:%S")
        is_up = sig.direction.value == "UP"
        style = "green" if is_up else "red"

        gap = sig.source_gap_pct
        if gap > 0:
            gap_style = (
                "green" if gap < 0.001 else "yellow" if gap < 0.0025 else "red"
            )
            gap_text = Text(f"{gap:.2%}", style=gap_style)
        else:
            gap_text = Text("--", style="dim")

        table.add_row(
            ts,
            sig.asset,
            _signal_source_label(sig.price_source),
            Text(
                _DIR_ZH.get(sig.direction.value, sig.direction.value),
                style=f"bold {style}",
            ),
            Text(f"{sig.deviation_pct:+.2%}", style=style),
            f"{sig.win_prob:.1%}",
            gap_text,
        )
    if not pipeline.signals:
        table.add_row("", "", "", Text("等待中...", style="dim"), "", "", "")
    return table


# ---------------------------------------------------------------------------
# Trades
# ---------------------------------------------------------------------------

def build_trades_table(pipeline: Pipeline) -> Table:
    table = Table(title="最近交易", expand=True, border_style="green")
    table.add_column("时间", width=8)
    table.add_column("标的", width=4)
    table.add_column("买哪边", width=6)
    table.add_column("状态", width=10)
    table.add_column("实际成交", width=8, justify="right")
    table.add_column("保守成交", width=8, justify="right")
    table.add_column("挂单价", width=6, justify="right")
    table.add_column("模型胜率", width=8, justify="right")
    table.add_column("模型期望", width=8, justify="right")
    table.add_column("成交额", width=7, justify="right")
    table.add_column("模式", width=5)

    now = time.time()
    for t in reversed(pipeline.executor.recent_trades[-12:]):
        ts = datetime.fromtimestamp(t.timestamp).strftime("%H:%M:%S")
        is_up = t.direction == "UP"
        dir_style = "green" if is_up else "red"
        mode = (
            Text("模拟", style="yellow")
            if t.is_paper
            else Text("实盘", style="bold red")
        )

        status_zh = _STATUS_ZH.get(t.display_status, t.display_status)
        if t.display_status in {"pending", "partial"} and t.expiration_ts > 0:
            ttl = max(0, t.expiration_ts - now)
            rm, rs = divmod(int(ttl), 60)
            status_zh = f"{status_zh} {rm}:{rs:02d}"
        elif t.display_status in {"rejected", "expired"} and t.last_error:
            status_zh = f"{status_zh}!"
        status_style = (
            "green"
            if t.display_status == "filled"
            else "yellow"
            if t.display_status in {"pending", "partial"}
            else "red"
        )

        if t.display_status == "filled" or t.matched_shares > 0:
            ev_val = t.realized_ev
        else:
            ev_val = t.submitted_ev
        ev_style = "green" if ev_val > 0 else "red"

        if t.display_status in {"pending", "partial"} and t.matched_cost_usd < 0.01:
            cost_text = Text(f"${t.cost_usd:.0f}", style="dim")
        else:
            cost_text = Text(f"${t.matched_cost_usd:.0f}")

        table.add_row(
            ts,
            t.asset,
            Text(
                f"{_ORDER_SIDE_ZH.get(t.order_side, t.order_side)}{_SIDE_ZH.get(t.token_side, t.token_side)}",
                style=f"bold {dir_style}",
            ),
            Text(status_zh, style=status_style),
            f"{t.matched_ratio:.0%}",
            f"{t.fill_ratio_lower_bound:.0%}",
            f"{t.price:.2f}",
            f"{t.win_prob:.0%}",
            Text(f"${ev_val:.2f}", style=ev_style),
            cost_text,
            mode,
        )
    if not pipeline.executor.recent_trades:
        table.add_row(
            "", "", "", "", "", "", "", "", "", Text("--", style="dim"), ""
        )
    return table


# ---------------------------------------------------------------------------
# Status
# ---------------------------------------------------------------------------

def build_status_panel(pipeline: Pipeline) -> Panel:
    ex = pipeline.executor
    uptime = time.time() - pipeline.start_time if pipeline.start_time > 0 else 0
    h, rem = divmod(int(uptime), 3600)
    m, s = divmod(rem, 60)

    t = Text()
    t.append(f" {datetime.now().strftime('%H:%M:%S')}", style="bold")
    t.append(f"  运行 {h:02d}:{m:02d}:{s:02d}\n")

    t.append(f" 行情: {pipeline.ticks_count:>8,}")
    t.append(f"  信号: {pipeline.signals_count}\n")
    t.append(f" 通过: {pipeline.guards_passed:>8}")
    t.append(f"  拦截: {pipeline.guard.suppressed_count}\n")
    t.append(f" 低流动: {ex.skipped_low_liq:>6}")
    t.append(f"  无优势: {ex.skipped_no_edge}\n")
    t.append(f" 低期望: {ex.skipped_low_ev:>6}")
    t.append(f"  超限额: {ex.skipped_live_limits}\n")
    t.append(f" 挂单: {ex.pending_count:>8}")
    t.append(f"  成交: {ex.trade_count}\n")
    t.append(f" 成交额: ${ex.total_cost:>7,.2f}")
    t.append(f"  占用: ${ex.total_committed:,.2f}\n")

    t.append(" ─── 连接/风控 ───\n", style="dim")

    binance_ok = pipeline.stream.connected
    poly_ok = pipeline.registry.market_count > 0
    t.append(" 币安: ")
    t.append(
        "已连接" if binance_ok else "连接中...",
        style="green" if binance_ok else "red",
    )
    t.append("  市场: ")
    t.append(
        f"{pipeline.registry.market_count}个" if poly_ok else "扫描中...",
        style="green" if poly_ok else "yellow",
    )
    t.append("\n")

    dry_run = pipeline.config.get("risk", {}).get("dry_run", True)
    t.append(" 模式: ")
    t.append("模拟" if dry_run else "实盘", style="bold yellow" if dry_run else "bold red")
    redeem_status = pipeline.redeemer.status()
    t.append("  赎回: ")
    if dry_run:
        t.append("模拟停用", style="dim")
    else:
        t.append(
            "已激活" if redeem_status.armed else "关闭",
            style="green" if redeem_status.armed else "yellow",
        )
    t.append("\n")

    t.append(" ─── 今日限额 ───\n", style="dim")

    daily_orders = 0
    daily_notional = 0.0
    if pipeline.db_conn is not None:
        usage = _cached_query("daily_usage", pipeline.db_conn, get_live_daily_usage)
        if usage:
            daily_orders = int(usage["orders"])
            daily_notional = float(usage["submitted_notional"])

    max_orders = ex.max_daily_orders
    max_notional = ex.max_daily_notional

    orders_str = f"{daily_orders}/{max_orders}" if max_orders > 0 else f"{daily_orders}/∞"
    notional_str = (
        f"${daily_notional:.0f}/${max_notional:.0f}"
        if max_notional > 0
        else f"${daily_notional:.0f}/∞"
    )
    orders_style = (
        "red" if max_orders > 0 and daily_orders >= max_orders else "green"
    )
    notional_style = (
        "red" if max_notional > 0 and daily_notional >= max_notional else "green"
    )

    t.append(" 日单: ")
    t.append(orders_str, style=orders_style)
    t.append("  日额: ")
    t.append(notional_str, style=notional_style)

    return Panel(t, title="运行状态", border_style="blue")


# ---------------------------------------------------------------------------
# Stats
# ---------------------------------------------------------------------------

def build_stats_panel(pipeline: Pipeline) -> Panel:
    trades = pipeline.executor.recent_trades
    filled = [
        tr for tr in trades if tr.matched_shares > 0 or tr.status.value == "filled"
    ]
    total_submitted_ev = sum(tr.submitted_ev for tr in trades)
    total_matched_ev = sum(tr.realized_ev for tr in filled)
    total_fee_saved = sum(tr.taker_fee_avoided * tr.matched_ratio for tr in filled)

    t = Text()

    t.append(f" 交易: {len(trades)}")
    t.append(f"  已成交: {len(filled)}\n")
    t.append(" 提交EV: ")
    t.append(
        f"${total_submitted_ev:+,.2f}",
        style="green" if total_submitted_ev >= 0 else "red",
    )
    t.append("  成交EV: ")
    t.append(
        f"${total_matched_ev:+,.2f}\n",
        style="green" if total_matched_ev >= 0 else "red",
    )
    t.append(f" 省手续费: ${total_fee_saved:,.2f}\n")

    t.append(" ─── 成交模型 ───\n", style="dim")

    matched_weight = sum(tr.matched_ratio for tr in filled)
    avg_p = (
        sum(tr.win_prob * tr.matched_ratio for tr in filled) / matched_weight
        if matched_weight > 0
        else 0
    )
    avg_fill = (
        sum(tr.fill_ratio_lower_bound for tr in trades) / len(trades) if trades else 0
    )
    avg_conf = sum(tr.fill_confidence for tr in trades) / len(trades) if trades else 0
    avg_samples = (
        sum(tr.fill_effective_samples for tr in trades) / len(trades) if trades else 0
    )

    t.append(f" 平均胜率: {avg_p:.1%}" if filled else " 平均胜率: --")
    t.append(f"  平均保守下界: {avg_fill:.1%}\n" if trades else "  平均保守下界: --\n")

    if trades:
        t.append(f" 置信度: {avg_conf:.1%}")
        t.append(f"  有效样本: {avg_samples:.0f}\n")
    else:
        t.append(" 置信度: --  有效样本: --\n")

    fill_stats = None
    if pipeline.db_conn is not None:
        fill_stats = _cached_query("fill_stats", pipeline.db_conn, get_fill_rate_stats)

    if fill_stats and fill_stats["samples"] > 0:
        t.append(f" 历史成交率: {fill_stats['avg_fill_ratio']:.0%}")
        t.append(f" ({fill_stats['samples']}样本/7天)\n", style="dim")
    else:
        t.append(" 历史成交率: -- (无数据)\n", style="dim")

    return Panel(t, title="统计", border_style="green")


# ---------------------------------------------------------------------------
# Funding
# ---------------------------------------------------------------------------

def build_funding_panel(pipeline: Pipeline) -> Panel:
    ex = pipeline.executor
    positions = ex.open_positions()

    unsettled_cost = ex.open_position_cost_basis
    buy_matched = ex.total_cost
    sell_recovered = ex.total_sell_recovered
    pending_committed = ex.total_committed
    net_invested = ex.net_cash_invested

    marked_value = 0.0
    markable_cost = 0.0
    markable_value = 0.0
    carried_cost = 0.0
    markable_positions = 0
    carried_positions = 0
    for pos in positions:
        market = pipeline.registry.get_market(pos.binance_symbol)
        if market is None or (pos.market_slug and market.slug != pos.market_slug):
            carried_cost += pos.available_shares * pos.avg_entry_price
            marked_value += pos.available_shares * pos.avg_entry_price
            carried_positions += 1
            continue
        token_price = market.up_price if pos.direction == "UP" else market.down_price
        if token_price <= 0:
            token_price = pos.avg_entry_price
            carried_cost += pos.available_shares * pos.avg_entry_price
            marked_value += pos.available_shares * pos.avg_entry_price
            carried_positions += 1
            continue
        cost_basis = pos.available_shares * pos.avg_entry_price
        current_value = pos.available_shares * token_price
        markable_cost += cost_basis
        markable_value += current_value
        marked_value += current_value
        markable_positions += 1

    unrealized = marked_value - unsettled_cost
    open_markets = len({pos.market_slug for pos in positions if pos.market_slug})
    active_unrealized = markable_value - markable_cost

    t = Text()
    t.append(" 已买入成交: ")
    t.append(f"${buy_matched:,.2f}", style="cyan")
    t.append("  已卖出回收: ")
    t.append(f"${sell_recovered:,.2f}\n", style="green" if sell_recovered > 0 else "dim")

    t.append(" 未结算成本: ")
    t.append(f"${unsettled_cost:,.2f}", style="yellow" if unsettled_cost > 0 else "dim")
    t.append("  未结算估值: ")
    t.append(
        f"${marked_value:,.2f}\n",
        style="green" if marked_value >= unsettled_cost else "red" if marked_value > 0 else "dim",
    )

    t.append(" 挂单占用: ")
    t.append(f"${pending_committed:,.2f}", style="yellow" if pending_committed > 0 else "dim")
    t.append("  净投入: ")
    t.append(f"${net_invested:,.2f}\n", style="white")

    t.append(" ─── 未结算头寸 ───\n", style="dim")
    t.append(f" 头寸数: {len(positions)}")
    t.append(f"  待结算市场: {open_markets}\n")
    t.append(" 可市价估值: ")
    t.append(f"{markable_positions}", style="cyan" if markable_positions > 0 else "dim")
    t.append("  按成本记账: ")
    t.append(f"{carried_positions}\n", style="yellow" if carried_positions > 0 else "dim")

    if markable_positions > 0:
        t.append(" 当前浮盈亏: ")
        t.append(
            f"${active_unrealized:+,.2f}",
            style="green" if active_unrealized >= 0 else "red",
        )
        if carried_positions > 0:
            t.append("  历史待结算: ")
            t.append(f"${carried_cost:,.2f}", style="yellow")
    else:
        t.append(" 当前浮盈亏: ")
        t.append("--", style="dim")
        if carried_positions > 0:
            t.append("  历史待结算: ")
            t.append(f"${carried_cost:,.2f}", style="yellow")
    t.append("\n")

    if carried_positions > 0:
        t.append(" 注: 非当前活跃窗口的仓位先按成本记账", style="dim")
    elif positions:
        t.append(
            f" 总浮盈亏: ${unrealized:+,.2f}",
            style="green" if unrealized >= 0 else "red",
        )
    else:
        t.append(" 当前无未结算仓位", style="dim")

    return Panel(t, title="资金", border_style="yellow")


# ---------------------------------------------------------------------------
# Dashboard layout
# ---------------------------------------------------------------------------

def build_dashboard(pipeline: Pipeline) -> Layout:
    layout = Layout()
    layout.split_column(
        Layout(name="header", size=3),
        Layout(name="top", size=10),
        Layout(name="middle"),
        Layout(name="bottom", size=16),
    )

    layout["header"].update(build_header(pipeline))

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
        Layout(build_status_panel(pipeline), ratio=5),
        Layout(build_funding_panel(pipeline), ratio=4),
        Layout(build_stats_panel(pipeline), ratio=4),
    )

    return layout
