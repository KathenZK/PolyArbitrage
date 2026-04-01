"""Market registry: slug-based discovery of Polymarket 15-min crypto markets.

Uses the deterministic slug pattern {asset}-updown-15m-{window_start} to
directly fetch active markets. Records the Binance price at window start
as the opening price reference for signal generation.

Window alignment: 900-second (15-min) boundaries in Unix time.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass, field

import aiohttp

from src.data.polymarket_client import PolymarketGammaClient

logger = logging.getLogger(__name__)

WINDOW_SECS = 900

ASSETS: dict[str, str] = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}

BINANCE_TO_ASSET: dict[str, str] = {v: k for k, v in ASSETS.items()}


def current_window_start() -> int:
    return (int(time.time()) // WINDOW_SECS) * WINDOW_SECS


@dataclass
class CryptoMarket:
    market_id: str
    question: str
    slug: str
    asset: str
    binance_symbol: str
    up_token_id: str
    down_token_id: str
    up_price: float
    down_price: float
    event_start: int
    end_time: float
    opening_price: float = 0.0
    volume: float = 0.0
    liquidity: float = 0.0
    fees_enabled: bool = True
    fee_rate: float = 0.072

    @property
    def secs_remaining(self) -> float:
        return max(0, self.end_time - time.time())

    @property
    def secs_elapsed(self) -> float:
        return max(0, time.time() - self.event_start)

    @property
    def has_opening_price(self) -> bool:
        return self.opening_price > 0


class MarketRegistry:
    """Discovers active 15-min crypto markets via slug pattern and tracks opening prices."""

    def __init__(
        self,
        gamma: PolymarketGammaClient,
        assets: list[str] | None = None,
        refresh_interval: float = 15,
    ):
        self._gamma = gamma
        self._assets = assets or ["btc", "eth", "sol"]
        self._refresh_interval = refresh_interval
        self._markets: dict[str, CryptoMarket] = {}
        self._running = False
        self._last_window_start = 0

    @property
    def markets(self) -> dict[str, CryptoMarket]:
        return dict(self._markets)

    @property
    def all_markets(self) -> list[CryptoMarket]:
        return list(self._markets.values())

    @property
    def market_count(self) -> int:
        return len(self._markets)

    def get_market(self, binance_symbol: str) -> CryptoMarket | None:
        return self._markets.get(binance_symbol)

    def record_opening_price(self, binance_symbol: str, price: float):
        """Called on the first tick after a new window starts."""
        market = self._markets.get(binance_symbol)
        if market and not market.has_opening_price:
            market.opening_price = price
            sym = market.asset.upper()
            logger.info(f"Opening price: {sym} = ${price:,.2f} (window {market.event_start})")

    async def refresh(self):
        ws = current_window_start()
        new_window = ws != self._last_window_start

        for asset_key in self._assets:
            binance_sym = ASSETS.get(asset_key, f"{asset_key}usdt")
            slug = f"{asset_key}-updown-15m-{ws}"

            try:
                event_data = await asyncio.wait_for(
                    self._fetch_event_by_slug(slug), timeout=8
                )
                if not event_data:
                    continue

                market = self._parse_event(event_data, asset_key, binance_sym, ws)
                if not market:
                    continue

                old = self._markets.get(binance_sym)
                if old and old.event_start == ws and old.has_opening_price:
                    market.opening_price = old.opening_price

                self._markets[binance_sym] = market

            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {slug}")
            except Exception as e:
                logger.warning(f"Error fetching {slug}: {e}")

        self._last_window_start = ws

        active = [m for m in self._markets.values() if m.secs_remaining > 10]
        if active:
            parts = []
            for m in active:
                sym = m.asset.upper()
                op = f"${m.opening_price:,.2f}" if m.has_opening_price else "?"
                parts.append(f"{sym}(open={op})")
            logger.info(f"Registry: {', '.join(parts)}")

        if new_window:
            for m in self._markets.values():
                if m.event_start == ws and not m.has_opening_price:
                    logger.info(f"New window {ws}: waiting for {m.asset.upper()} opening price")

    async def _fetch_event_by_slug(self, slug: str) -> dict | None:
        session = await self._gamma._ensure_session()
        url = f"https://gamma-api.polymarket.com/events?slug={slug}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=8)) as r:
            events = await r.json()
        return events[0] if events else None

    def _parse_event(
        self, event: dict, asset: str, binance_sym: str, window_start: int
    ) -> CryptoMarket | None:
        markets = event.get("markets", [])
        if not markets:
            return None
        m = markets[0]

        if not m.get("active") or m.get("closed"):
            return None

        outcomes_raw = m.get("outcomes", [])
        if isinstance(outcomes_raw, str):
            try:
                outcomes = json.loads(outcomes_raw)
            except json.JSONDecodeError:
                return None
        else:
            outcomes = outcomes_raw
        if len(outcomes) < 2 or outcomes[0] != "Up":
            return None

        tokens_raw = m.get("clobTokenIds", "[]")
        if isinstance(tokens_raw, str):
            try:
                tokens = json.loads(tokens_raw)
            except json.JSONDecodeError:
                return None
        else:
            tokens = tokens_raw
        if len(tokens) < 2:
            return None

        prices_raw = m.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices = [float(p) for p in json.loads(prices_raw)]
            except (json.JSONDecodeError, ValueError):
                prices = [0.5, 0.5]
        else:
            prices = [float(p) for p in prices_raw]

        end_date_str = m.get("endDate", "")
        try:
            from datetime import datetime, timezone

            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            end_ts = end_dt.timestamp()
        except (ValueError, TypeError):
            end_ts = window_start + WINDOW_SECS

        fee_sched = m.get("feeSchedule", {})

        return CryptoMarket(
            market_id=str(m.get("id", "")),
            question=m.get("question", ""),
            slug=event.get("slug", ""),
            asset=asset,
            binance_symbol=binance_sym,
            up_token_id=tokens[0],
            down_token_id=tokens[1],
            up_price=prices[0] if prices else 0.5,
            down_price=prices[1] if len(prices) > 1 else 0.5,
            event_start=window_start,
            end_time=end_ts,
            volume=float(m.get("volume", 0) or 0),
            liquidity=float(m.get("liquidity", 0) or 0),
            fees_enabled=bool(m.get("feesEnabled", True)),
            fee_rate=fee_sched.get("rate", 0.072) if fee_sched else 0.072,
        )

    async def run(self):
        self._running = True
        while self._running:
            await self.refresh()
            await asyncio.sleep(self._refresh_interval)

    def stop(self):
        self._running = False
