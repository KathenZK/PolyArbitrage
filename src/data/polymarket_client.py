"""Polymarket API client: Gamma (REST) for market discovery, WebSocket for real-time prices."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any, Callable

import aiohttp
import websockets

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
WS_MARKET_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


@dataclass
class PolymarketMarket:
    id: str
    question: str
    condition_id: str
    token_ids: list[str]
    outcome_prices: list[float]
    volume: float
    liquidity: float
    neg_risk: bool
    event_id: str
    event_title: str
    end_date: str
    active: bool
    slug: str = ""
    tags: list[str] = field(default_factory=list)


class PolymarketGammaClient:
    """REST client for the Gamma API — market and event discovery (no auth)."""

    def __init__(self, session: aiohttp.ClientSession | None = None):
        self._own_session = session is None
        self._session = session

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
            self._own_session = True
        return self._session

    async def close(self):
        if self._own_session and self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, path: str, params: dict | None = None) -> Any:
        session = await self._ensure_session()
        url = f"{GAMMA_BASE}{path}"
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            r.raise_for_status()
            return await r.json()

    async def get_events(
        self,
        active: bool = True,
        closed: bool = False,
        neg_risk: bool | None = None,
        tag: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        params: dict[str, Any] = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }
        if neg_risk is not None:
            params["neg_risk"] = str(neg_risk).lower()
        if tag:
            params["tag"] = tag
        return await self._get("/events", params)

    async def get_all_negrisk_events(self) -> list[dict]:
        """Paginate through all active NegRisk events."""
        all_events: list[dict] = []
        offset = 0
        while True:
            batch = await self.get_events(neg_risk=True, limit=100, offset=offset)
            if not batch:
                break
            all_events.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
        return all_events

    async def get_markets(
        self,
        active: bool = True,
        closed: bool = False,
        tag: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        params: dict[str, Any] = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }
        if tag:
            params["tag"] = tag
        return await self._get("/markets", params)

    async def get_sports_markets(self, sport: str = "nba") -> list[dict]:
        """Fetch sports markets by tag, paginating through all results."""
        all_markets: list[dict] = []
        offset = 0
        while True:
            batch = await self.get_markets(tag=sport, limit=100, offset=offset)
            if not batch:
                break
            all_markets.extend(batch)
            if len(batch) < 100:
                break
            offset += 100
        return all_markets

    @staticmethod
    def parse_market(raw: dict, event: dict | None = None) -> PolymarketMarket:
        prices_raw = raw.get("outcomePrices", "[]")
        if isinstance(prices_raw, str):
            prices = [float(p) for p in json.loads(prices_raw)]
        elif isinstance(prices_raw, list):
            prices = [float(p) for p in prices_raw]
        else:
            prices = [0.0, 0.0]

        tokens_raw = raw.get("clobTokenIds", "[]")
        if isinstance(tokens_raw, str):
            tokens = json.loads(tokens_raw)
        elif isinstance(tokens_raw, list):
            tokens = tokens_raw
        else:
            tokens = []

        return PolymarketMarket(
            id=raw.get("id", ""),
            question=raw.get("question", ""),
            condition_id=raw.get("conditionId", ""),
            token_ids=tokens,
            outcome_prices=prices,
            volume=float(raw.get("volume", 0) or 0),
            liquidity=float(raw.get("liquidity", 0) or 0),
            neg_risk=bool(raw.get("negRisk", False)),
            event_id=event.get("id", "") if event else raw.get("eventId", ""),
            event_title=event.get("title", "") if event else "",
            end_date=raw.get("endDate", ""),
            active=bool(raw.get("active", True)),
            slug=raw.get("slug", ""),
        )


class PolymarketWebSocket:
    """WebSocket client for real-time market price updates (no auth)."""

    def __init__(self, on_price_update: Callable[[str, list[float]], None] | None = None):
        self._ws: Any = None
        self._subscribed_tokens: set[str] = set()
        self._on_price_update = on_price_update
        self._running = False

    async def connect(self):
        self._ws = await websockets.connect(WS_MARKET_URL, ping_interval=10, ping_timeout=5)
        self._running = True
        logger.info("WebSocket connected to Polymarket market channel")

    async def subscribe(self, token_ids: list[str]):
        if not self._ws:
            return
        new_tokens = [t for t in token_ids if t not in self._subscribed_tokens]
        if not new_tokens:
            return
        msg = {"assets_ids": new_tokens, "type": "market", "custom_feature_enabled": True}
        await self._ws.send(json.dumps(msg))
        self._subscribed_tokens.update(new_tokens)
        logger.info(f"Subscribed to {len(new_tokens)} tokens (total: {len(self._subscribed_tokens)})")

    async def listen(self):
        if not self._ws:
            return
        try:
            async for raw_msg in self._ws:
                if not self._running:
                    break
                try:
                    msgs = json.loads(raw_msg)
                    if not isinstance(msgs, list):
                        msgs = [msgs]
                    for msg in msgs:
                        self._handle_message(msg)
                except json.JSONDecodeError:
                    continue
        except websockets.ConnectionClosed:
            logger.warning("WebSocket connection closed")
        except Exception as e:
            logger.error(f"WebSocket error: {e}")

    def _handle_message(self, msg: dict):
        event_type = msg.get("event_type", "")
        if event_type in ("price_change", "book", "last_trade_price"):
            asset_id = msg.get("asset_id", "")
            price = msg.get("price")
            if asset_id and price is not None and self._on_price_update:
                self._on_price_update(asset_id, [float(price)])

    async def close(self):
        self._running = False
        if self._ws:
            await self._ws.close()


class PolymarketCLOBClient:
    """Thin wrapper around py-clob-client for live trading (requires auth)."""

    def __init__(self, private_key: str, chain_id: int = 137):
        self._key = private_key
        self._chain_id = chain_id
        self._client = None

    def _ensure_client(self):
        if self._client is None:
            from py_clob_client.client import ClobClient
            self._client = ClobClient(
                host="https://clob.polymarket.com",
                chain_id=self._chain_id,
                key=self._key,
            )
            self._client.set_api_creds(self._client.create_or_derive_api_creds())
        return self._client

    def get_orderbook(self, token_id: str) -> dict:
        client = self._ensure_client()
        return client.get_order_book(token_id)

    def place_market_order(self, token_id: str, side: str, size: float, neg_risk: bool = False):
        client = self._ensure_client()
        from py_clob_client.order_builder.constants import BUY, SELL
        order_side = BUY if side.upper() == "BUY" else SELL
        order = client.create_market_order(
            token_id=token_id,
            amount=size,
            side=order_side,
            neg_risk=neg_risk,
        )
        return client.post_order(order)
