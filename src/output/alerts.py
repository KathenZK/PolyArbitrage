"""钉钉 Webhook 通知"""

from __future__ import annotations

import logging
import os
from datetime import datetime

import aiohttp

logger = logging.getLogger(__name__)


class DingTalkAlert:
    def __init__(self, webhook_url: str = "", keyword: str = "PolyGod"):
        self._url = webhook_url or os.getenv("DINGTALK_WEBHOOK", "")
        self._keyword = keyword
        self._session: aiohttp.ClientSession | None = None

    @property
    def enabled(self) -> bool:
        return bool(self._url)

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
            self._session = None

    async def send_text(self, content: str):
        if not self._url:
            return
        body = {
            "msgtype": "text",
            "text": {"content": f"{self._keyword} {content}"},
        }
        await self._post(body)

    async def send_markdown(self, title: str, text: str):
        if not self._url:
            return
        if self._keyword not in text:
            text = f"**{self._keyword}**\n\n{text}"
        body = {
            "msgtype": "markdown",
            "markdown": {"title": title, "text": text},
        }
        await self._post(body)

    async def send_trade(
        self,
        symbol: str,
        direction: str,
        price: float,
        shares: float,
        cost: float,
        momentum: float,
        market_question: str,
        is_paper: bool,
        order_id: str,
    ):
        mode = "模拟" if is_paper else "实盘"
        ts = datetime.now().strftime("%H:%M:%S")
        arrow = "📈" if direction == "UP" else "📉"
        side = "看涨" if direction == "UP" else "看跌"
        text = (
            f"**{self._keyword}** {arrow} **下单通知**\n\n"
            f"> **{side} {symbol}** @ ${price:.3f}\n\n"
            f"- 方向：{side}（买入 {'Up' if direction == 'UP' else 'Down'} token）\n"
            f"- 价格：${price:.3f}\n"
            f"- 数量：{shares:.1f} 股\n"
            f"- 金额：${cost:.2f}\n"
            f"- 偏离开盘价：{momentum:+.2%}\n"
            f"- 模式：**{mode}**\n"
            f"- 时间：{ts}\n"
            f"- 订单号：{order_id}\n"
        )
        await self.send_markdown(f"{mode} {side} {symbol}", text)

    async def send_signal(self, symbol: str, direction: str, momentum: float, price: float):
        ts = datetime.now().strftime("%H:%M:%S")
        arrow = "📈" if direction == "UP" else "📉"
        side = "看涨" if direction == "UP" else "看跌"
        await self.send_text(
            f"{arrow} 信号：{side} {symbol} "
            f"偏离 {momentum:+.2%} 当前价 ${price:,.2f} [{ts}]"
        )

    async def send_startup(self, mode: str, symbols: list[str]):
        sym_str = "、".join(symbols)
        mode_cn = "模拟交易" if mode == "PAPER" else "实盘交易"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = (
            f"**{self._keyword}** 🚀 **策略已启动**\n\n"
            f"- 模式：**{mode_cn}**\n"
            f"- 监控币种：{sym_str}\n"
            f"- 启动时间：{ts}\n"
        )
        await self.send_markdown(f"策略启动 {mode_cn}", text)

    async def send_heartbeat(
        self,
        *,
        uptime_secs: float,
        ticks: int,
        signals: int,
        guards_passed: int,
        trades_filled: int,
        trades_pending: int,
        skipped_liq: int,
        skipped_edge: int,
        skipped_ev: int,
        skipped_live: int,
        total_cost: float,
        markets_active: int,
        mode: str,
    ):
        h, rem = divmod(int(uptime_secs), 3600)
        m, s = divmod(rem, 60)
        mode_cn = "模拟" if mode == "PAPER" else "实盘"
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        text = (
            f"**{self._keyword}** 💓 **状态报告**\n\n"
            f"- 时间：{ts}\n"
            f"- 运行：{h}h{m:02d}m{s:02d}s\n"
            f"- 模式：**{mode_cn}**\n"
            f"- 市场：{markets_active} 个活跃\n"
            f"- Tick：{ticks:,}\n"
            f"- 信号：{signals}\n"
            f"- 通过Guard：{guards_passed}\n"
            f"- 跳过（流动性）：{skipped_liq}\n"
            f"- 跳过（无edge）：{skipped_edge}\n"
            f"- 跳过（EV低）：{skipped_ev}\n"
            f"- 跳过（日限额）：{skipped_live}\n"
            f"- 已成交：{trades_filled}\n"
            f"- 挂单中：{trades_pending}\n"
            f"- 累计金额：${total_cost:,.2f}\n"
        )
        await self.send_markdown(f"状态报告 {mode_cn}", text)

    async def _post(self, body: dict):
        try:
            session = await self._ensure_session()
            async with session.post(
                self._url,
                json=body,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                result = await resp.json()
                if result.get("errcode") != 0:
                    logger.warning(f"DingTalk error: {result}")
        except Exception as e:
            logger.warning(f"DingTalk send failed: {e}")
