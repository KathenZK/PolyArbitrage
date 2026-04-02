"""Market registry: discover Polymarket crypto windows and keep price anchors.

Two reference layers are tracked per market:
  - fast source: Binance ticks, used for sub-second movement
  - official source: Polymarket event metadata derived from Chainlink, used as
    the settlement anchor (`priceToBeat`) and current official price snapshot

Opening price tracking:
  - buffers recent Binance ticks per symbol (last ``TICK_BUFFER_SECS`` seconds)
  - on window change, reuses the tick closest to the window start
  - pre-fetches the next window's Gamma payload near the boundary
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from dataclasses import dataclass
from datetime import datetime

import aiohttp

from src.data.polymarket_client import PolymarketGammaClient

logger = logging.getLogger(__name__)

WINDOW_SECS = 900
MAX_OPENING_PRICE_DELAY = 15
TICK_BUFFER_SECS = 30


def current_window_start() -> int:
    return (int(time.time()) // WINDOW_SECS) * WINDOW_SECS


def next_window_start() -> int:
    return current_window_start() + WINDOW_SECS


ASSETS: dict[str, str] = {
    "btc": "btcusdt",
    "eth": "ethusdt",
    "sol": "solusdt",
    "xrp": "xrpusdt",
}

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
    best_bid: float
    best_ask: float
    event_start: int
    end_time: float
    opening_price: float = 0.0
    volume: float = 0.0
    liquidity: float = 0.0
    spread: float = 0.0
    resolution_source: str = ""
    description: str = ""
    fees_enabled: bool = True
    fee_rate: float = 0.072
    order_min_size: int = 5
    official_opening_price: float = 0.0
    official_current_price: float = 0.0
    official_binance_ref_price: float = 0.0
    official_price_updated_at: float = 0.0
    official_binance_ref_ts: float = 0.0

    @property
    def secs_remaining(self) -> float:
        return max(0, self.end_time - time.time())

    @property
    def secs_elapsed(self) -> float:
        return max(0, time.time() - self.event_start)

    @property
    def has_opening_price(self) -> bool:
        return self.opening_price > 0

    @property
    def has_official_opening_price(self) -> bool:
        return self.official_opening_price > 0

    @property
    def has_official_current_price(self) -> bool:
        return self.official_current_price > 0

    @property
    def has_official_calibration(self) -> bool:
        return (
            self.official_opening_price > 0
            and self.official_current_price > 0
            and self.official_binance_ref_price > 0
        )

    @property
    def official_price_age(self) -> float:
        if self.official_price_updated_at <= 0:
            return float("inf")
        return max(0.0, time.time() - self.official_price_updated_at)

    @property
    def official_calibration_age(self) -> float:
        ts = self.official_binance_ref_ts if self.official_binance_ref_ts > 0 else self.official_price_updated_at
        if ts <= 0:
            return float("inf")
        return max(0.0, time.time() - ts)

    def taker_fee(self, shares: float, price: float) -> float:
        if not self.fees_enabled:
            return 0.0
        return shares * self.fee_rate * price * (1 - price)


class MarketRegistry:
    """Discovers active 15-min crypto markets and tracks opening prices."""

    def __init__(
        self,
        gamma: PolymarketGammaClient,
        assets: list[str] | None = None,
        refresh_interval: float = 15,
        official_refresh_interval: float = 60,
        min_liquidity: float = 1000,
    ):
        self._gamma = gamma
        self._assets = assets or ["btc", "eth", "sol"]
        self._refresh_interval = refresh_interval
        self._official_refresh_interval = official_refresh_interval
        self._min_liquidity = min_liquidity
        self._markets: dict[str, CryptoMarket] = {}
        self._running = False
        self._current_window: int = 0
        self._tick_buffer: dict[str, deque[tuple[float, float]]] = {}
        self._on_window_change: list = []
        self._prefetched_events: dict[str, dict] = {}

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

    def register_window_change_callback(self, cb):
        self._on_window_change.append(cb)

    def buffer_tick(self, binance_symbol: str, price: float, tick_ts: float):
        """Buffer a Binance tick for opening price recovery. Called on every tick."""
        if binance_symbol not in self._tick_buffer:
            self._tick_buffer[binance_symbol] = deque()
        buf = self._tick_buffer[binance_symbol]
        buf.append((tick_ts, price))
        cutoff = tick_ts - TICK_BUFFER_SECS
        while buf and buf[0][0] < cutoff:
            buf.popleft()

    def record_opening_price(self, binance_symbol: str, price: float, tick_ts: float):
        """Try to set the opening price from a live tick or the buffer."""
        market = self._markets.get(binance_symbol)
        if not market or market.has_opening_price:
            return

        age = tick_ts - market.event_start if tick_ts > 0 else market.secs_elapsed
        if age > MAX_OPENING_PRICE_DELAY:
            return

        best_price, best_ts = self._find_best_opening_tick(binance_symbol, market.event_start)
        if best_price > 0:
            market.opening_price = best_price
            delta = best_ts - market.event_start
            sym = market.asset.upper()
            logger.info(f"Opening price: {sym} = ${best_price:,.2f} (tick {delta:+.1f}s from window start)")
            return

        if age <= 5:
            market.opening_price = price
            sym = market.asset.upper()
            logger.info(f"Opening price: {sym} = ${price:,.2f} (live tick {age:.1f}s into window)")

    def _find_best_opening_tick(self, binance_symbol: str, event_start: int) -> tuple[float, float]:
        """Find the buffered tick closest to the window start, allowing 1s early skew."""
        buf = self._tick_buffer.get(binance_symbol)
        if not buf:
            return 0.0, 0.0

        best_price = 0.0
        best_ts = 0.0
        best_delta = float("inf")

        for ts, price in buf:
            delta = ts - event_start
            if -1.0 <= delta <= 5.0 and abs(delta) < best_delta:
                best_delta = abs(delta)
                best_price = price
                best_ts = ts

        return best_price, best_ts

    def _latest_buffered_price(self, binance_symbol: str) -> float:
        buf = self._tick_buffer.get(binance_symbol)
        if not buf:
            return 0.0
        return float(buf[-1][1] or 0.0)

    async def _refresh_official_metadata(self, market: CryptoMarket):
        try:
            metadata = await asyncio.wait_for(
                self._gamma.get_event_page_metadata(market.slug),
                timeout=12,
            )
        except Exception as exc:
            logger.debug(f"Official metadata fetch failed for {market.slug}: {exc}")
            return

        opening = float(metadata.get("official_opening_price", 0) or 0)
        current = float(metadata.get("official_current_price", 0) or 0)
        if opening > 0:
            market.official_opening_price = opening
        if current > 0:
            market.official_current_price = current
        if opening > 0 or current > 0:
            market.official_price_updated_at = float(metadata.get("fetched_at", time.time()) or time.time())

        latest_binance = self._latest_buffered_price(market.binance_symbol)
        if latest_binance > 0:
            market.official_binance_ref_price = latest_binance
            market.official_binance_ref_ts = time.time()

    async def refresh(self):
        ws = current_window_start()
        new_window = ws != self._current_window

        if new_window:
            for cb in self._on_window_change:
                try:
                    cb()
                except Exception:
                    pass

        for asset_key in self._assets:
            binance_sym = ASSETS.get(asset_key, f"{asset_key}usdt")
            slug = f"{asset_key}-updown-15m-{ws}"

            try:
                event_data = self._prefetched_events.pop(slug, None)
                if event_data is None:
                    event_data = await asyncio.wait_for(
                        self._fetch_event_by_slug(slug), timeout=8
                    )
                if not event_data:
                    continue

                market = self._parse_event(event_data, asset_key, binance_sym, ws)
                if not market:
                    continue

                old = self._markets.get(binance_sym)
                if old and old.event_start == ws:
                    if old.has_opening_price:
                        market.opening_price = old.opening_price
                    market.official_opening_price = old.official_opening_price
                    market.official_current_price = old.official_current_price
                    market.official_binance_ref_price = old.official_binance_ref_price
                    market.official_price_updated_at = old.official_price_updated_at
                    market.official_binance_ref_ts = old.official_binance_ref_ts

                self._markets[binance_sym] = market

                if new_window and not market.has_opening_price:
                    best_p, best_ts = self._find_best_opening_tick(binance_sym, ws)
                    if best_p > 0:
                        market.opening_price = best_p
                        delta = best_ts - ws
                        logger.info(
                            f"Opening price (from buffer): {asset_key.upper()} = "
                            f"${best_p:,.2f} ({delta:+.1f}s from window start)"
                        )

            except asyncio.TimeoutError:
                logger.warning(f"Timeout fetching {slug}")
            except Exception as e:
                logger.warning(f"Error fetching {slug}: {e}")

        self._current_window = ws

        stale = [
            m for m in self._markets.values()
            if m.secs_remaining > 10
            and (
                m.official_price_age >= self._official_refresh_interval
                or not m.has_official_opening_price
                or not m.has_official_current_price
            )
        ]
        if stale:
            await asyncio.gather(
                *(self._refresh_official_metadata(m) for m in stale),
                return_exceptions=True,
            )

        active = [m for m in self._markets.values() if m.secs_remaining > 10]
        if active:
            parts = []
            for m in active:
                sym = m.asset.upper()
                fast_open = f"${m.opening_price:,.2f}" if m.has_opening_price else "?"
                official_open = f"${m.official_opening_price:,.2f}" if m.has_official_opening_price else "?"
                parts.append(f"{sym}(bin={fast_open}, off={official_open})")
            logger.info(f"Registry: {', '.join(parts)}")

    async def _pre_fetch_next_window(self):
        """Fetch market data for the NEXT window before it starts."""
        nws = next_window_start()
        for asset_key in self._assets:
            slug = f"{asset_key}-updown-15m-{nws}"
            try:
                event_data = await asyncio.wait_for(self._fetch_event_by_slug(slug), timeout=5)
                if event_data:
                    self._prefetched_events[slug] = event_data
            except Exception:
                pass

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
            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            end_ts = end_dt.timestamp()
        except (ValueError, TypeError):
            end_ts = window_start + WINDOW_SECS

        fee_sched = m.get("feeSchedule", {})

        return CryptoMarket(
            market_id=str(m.get("id", "")),
            question=m.get("question", ""),
            description=m.get("description", "") or event.get("description", ""),
            slug=event.get("slug", ""),
            asset=asset,
            binance_symbol=binance_sym,
            up_token_id=tokens[0],
            down_token_id=tokens[1],
            up_price=prices[0] if prices else 0.5,
            down_price=prices[1] if len(prices) > 1 else 0.5,
            best_bid=float(m.get("bestBid", 0) or 0),
            best_ask=float(m.get("bestAsk", 0) or 0),
            event_start=window_start,
            end_time=end_ts,
            volume=float(m.get("volume", 0) or 0),
            liquidity=float(m.get("liquidity", 0) or 0),
            spread=float(m.get("spread", 0) or 0),
            resolution_source=m.get("resolutionSource", "") or event.get("resolutionSource", ""),
            fees_enabled=bool(m.get("feesEnabled", True)),
            fee_rate=fee_sched.get("rate", 0.072) if fee_sched else 0.072,
            order_min_size=int(m.get("orderMinSize", 5) or 5),
        )

    async def run(self):
        """Refresh loop aligned to window boundaries.

        Near window end (< 20s left): pre-fetch next window's markets,
        then sleep until boundary, then immediately refresh to pick up
        the new window and record opening prices from the tick buffer.
        """
        self._running = True
        while self._running:
            now = time.time()
            ws = current_window_start()
            secs_into_window = now - ws
            secs_to_next = WINDOW_SECS - secs_into_window

            await self.refresh()

            if secs_to_next <= 20:
                await self._pre_fetch_next_window()
                await asyncio.sleep(max(0.5, secs_to_next - 1))
                await self.refresh()
                await asyncio.sleep(2)
            else:
                sleep_time = min(self._refresh_interval, secs_to_next - 15)
                await asyncio.sleep(max(1, sleep_time))

    def stop(self):
        self._running = False
