"""Market registry: discovers and tracks active Polymarket 15-min crypto markets.

Polls the Gamma API for active binary crypto markets (BTC/ETH/SOL UP/DOWN)
with short time horizons. Uses multiple discovery strategies:
  1. Tag-based search (fast, if Polymarket tags these markets)
  2. Keyword search across recent active markets (broader fallback)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone

from src.data.polymarket_client import PolymarketGammaClient

logger = logging.getLogger(__name__)

SYMBOL_KEYWORDS: dict[str, list[str]] = {
    "btcusdt": ["bitcoin", "btc"],
    "ethusdt": ["ethereum", "eth"],
    "solusdt": ["solana", "sol"],
    "xrpusdt": ["xrp"],
}

_REVERSE: dict[str, str] = {}
for _sym, _kws in SYMBOL_KEYWORDS.items():
    for _kw in _kws:
        _REVERSE[_kw] = _sym

UP_PATTERNS = ["higher", "above", "up ", " up?", "increase", "rise"]
DOWN_PATTERNS = ["lower", "below", "down ", " down?", "decrease", "fall"]

CRYPTO_TAGS = ["crypto", "bitcoin", "btc", "ethereum", "solana"]
MAX_MARKET_HORIZON_SECS = 1800
MAX_PAGES = 5


@dataclass
class CryptoMarket:
    market_id: str
    question: str
    symbol: str
    direction: str
    yes_token_id: str
    no_token_id: str
    yes_price: float
    no_price: float
    end_date: str
    end_timestamp: float
    volume: float
    liquidity: float

    @property
    def secs_remaining(self) -> float:
        return max(0, self.end_timestamp - time.time())

    @property
    def mid_price(self) -> float:
        return self.yes_price if self.yes_price > 0 else 0.5


class MarketRegistry:
    """Discovers active short-duration crypto markets on Polymarket."""

    def __init__(self, gamma: PolymarketGammaClient, refresh_interval: float = 30):
        self._gamma = gamma
        self._refresh_interval = refresh_interval
        self._markets: dict[str, list[CryptoMarket]] = {}
        self._running = False
        self._last_refresh = 0.0
        self._total_scanned = 0

    @property
    def all_markets(self) -> list[CryptoMarket]:
        result = []
        for ms in self._markets.values():
            result.extend(ms)
        return result

    @property
    def market_count(self) -> int:
        return sum(len(v) for v in self._markets.values())

    @property
    def symbols_active(self) -> list[str]:
        return [k for k, v in self._markets.items() if v]

    def get_active_market(
        self, symbol: str, direction: str, min_secs: float = 30
    ) -> CryptoMarket | None:
        candidates = self._markets.get(symbol, [])
        now = time.time()
        best = None
        for m in candidates:
            remaining = m.end_timestamp - now
            if remaining < min_secs:
                continue
            if m.direction.upper() != direction.upper():
                continue
            if best is None or remaining > (best.end_timestamp - now):
                best = m
        return best

    async def refresh(self):
        try:
            raw_markets = await self._fetch_markets()
            self._total_scanned = len(raw_markets)

            new_markets: dict[str, list[CryptoMarket]] = {}
            for raw in raw_markets:
                if not raw.get("active"):
                    continue
                cm = self._parse_crypto_market(raw)
                if cm is None:
                    continue
                if cm.secs_remaining < 10:
                    continue
                new_markets.setdefault(cm.symbol, []).append(cm)

            self._markets = new_markets
            self._last_refresh = time.time()

            total = sum(len(v) for v in self._markets.values())
            if total > 0:
                symbols = list(self._markets.keys())
                logger.info(
                    f"Registry: {total} crypto markets "
                    f"({', '.join(s.replace('usdt','').upper() for s in symbols)})"
                )
            else:
                logger.debug(f"Registry: 0 crypto markets (scanned {self._total_scanned})")

        except Exception as e:
            logger.error(f"Registry refresh error: {e}")

    async def _fetch_markets(self) -> list[dict]:
        """Multi-strategy market discovery with timeout protection."""
        all_raw: list[dict] = []
        seen_ids: set[str] = set()

        for tag in CRYPTO_TAGS:
            try:
                batch = await asyncio.wait_for(
                    self._gamma.get_markets(tag=tag, limit=100), timeout=10
                )
                for m in batch:
                    mid = m.get("id", "")
                    if mid and mid not in seen_ids:
                        seen_ids.add(mid)
                        all_raw.append(m)
            except asyncio.TimeoutError:
                logger.debug(f"Tag search '{tag}' timed out")
            except Exception as e:
                logger.debug(f"Tag search '{tag}' error: {e}")

        if len(all_raw) < 10:
            offset = 0
            for _ in range(MAX_PAGES):
                try:
                    batch = await asyncio.wait_for(
                        self._gamma.get_markets(limit=100, offset=offset), timeout=10
                    )
                    if not batch:
                        break
                    for m in batch:
                        mid = m.get("id", "")
                        if mid and mid not in seen_ids:
                            seen_ids.add(mid)
                            all_raw.append(m)
                    if len(batch) < 100:
                        break
                    offset += 100
                except asyncio.TimeoutError:
                    logger.debug(f"Market fetch offset={offset} timed out")
                    break
                except Exception as e:
                    logger.debug(f"Market fetch offset={offset} error: {e}")
                    break

        return all_raw

    async def run(self):
        self._running = True
        while self._running:
            await self.refresh()
            await asyncio.sleep(self._refresh_interval)

    def stop(self):
        self._running = False

    def _parse_crypto_market(self, raw: dict) -> CryptoMarket | None:
        question = (raw.get("question") or "").lower()

        symbol = None
        for keyword, binance_sym in _REVERSE.items():
            if keyword in question:
                symbol = binance_sym
                break
        if symbol is None:
            return None

        direction = None
        for p in UP_PATTERNS:
            if p in question:
                direction = "UP"
                break
        if not direction:
            for p in DOWN_PATTERNS:
                if p in question:
                    direction = "DOWN"
                    break
        if direction is None:
            return None

        end_date_str = raw.get("endDate", "")
        if not end_date_str:
            return None

        try:
            end_dt = datetime.fromisoformat(end_date_str.replace("Z", "+00:00"))
            end_ts = end_dt.timestamp()
            now = time.time()
            if end_ts - now > MAX_MARKET_HORIZON_SECS or end_ts < now:
                return None
        except (ValueError, TypeError):
            return None

        prices_raw = raw.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            try:
                prices = [float(p) for p in json.loads(prices_raw)]
            except (json.JSONDecodeError, ValueError):
                return None
        elif isinstance(prices_raw, list):
            prices = [float(p) for p in prices_raw]
        else:
            return None

        if len(prices) < 2:
            return None

        tokens_raw = raw.get("clobTokenIds", "[]")
        if isinstance(tokens_raw, str):
            try:
                tokens = json.loads(tokens_raw)
            except json.JSONDecodeError:
                tokens = ["", ""]
        elif isinstance(tokens_raw, list):
            tokens = tokens_raw
        else:
            tokens = ["", ""]

        return CryptoMarket(
            market_id=raw.get("id", ""),
            question=raw.get("question", ""),
            symbol=symbol,
            direction=direction,
            yes_token_id=tokens[0] if tokens else "",
            no_token_id=tokens[1] if len(tokens) > 1 else "",
            yes_price=prices[0],
            no_price=prices[1],
            end_date=end_date_str,
            end_timestamp=end_ts,
            volume=float(raw.get("volume", 0) or 0),
            liquidity=float(raw.get("liquidity", 0) or 0),
        )
