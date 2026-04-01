"""Polymarket API clients: Gamma (REST) for market discovery, CLOB for trading.

CLOB client uses the official py-clob-client SDK with:
  - Post-Only orders (guarantees maker status, 0% fee)
  - GTD expiration (auto-cancel at window end)
  - Proper options (tick_size, neg_risk)
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"


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

    async def get_events(
        self,
        active: bool = True,
        closed: bool = False,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict]:
        params: dict[str, Any] = {
            "active": str(active).lower(),
            "closed": str(closed).lower(),
            "limit": limit,
            "offset": offset,
        }
        return await self._get("/events", params)

    async def check_geoblock(self) -> dict:
        """Check if the current IP is blocked from trading."""
        session = await self._ensure_session()
        async with session.get(
            "https://polymarket.com/api/geoblock",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return await r.json()


class PolymarketCLOBClient:
    """Wrapper around py-clob-client with post-only + GTD support."""

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

    def get_orderbook(self, token_id: str) -> Any:
        client = self._ensure_client()
        return client.get_order_book(token_id)

    @staticmethod
    def _read_level_value(level: Any, field: str) -> float:
        if isinstance(level, dict):
            value = level.get(field, 0)
        else:
            value = getattr(level, field, 0)
        return float(value or 0)

    @staticmethod
    def _read_book_levels(book: Any, side: str) -> list[Any]:
        if isinstance(book, dict):
            levels = book.get(side, [])
        else:
            levels = getattr(book, side, [])
        return list(levels or [])

    def get_best_bid(self, token_id: str) -> float:
        """Return the highest resting bid for post-only BUY pricing."""
        book = self.get_orderbook(token_id)
        bids = self._read_book_levels(book, "bids")
        if bids:
            return self._read_level_value(bids[0], "price")
        return 0.0

    def get_best_ask(self, token_id: str) -> float:
        book = self.get_orderbook(token_id)
        asks = self._read_book_levels(book, "asks")
        if asks:
            return self._read_level_value(asks[0], "price")
        return 0.0

    def get_book_depth(self, token_id: str, levels: int = 3) -> tuple[float, float]:
        """Returns (bid_depth_usd, ask_depth_usd) for top N levels."""
        book = self.get_orderbook(token_id)
        bid_depth = sum(
            self._read_level_value(level, "price") * self._read_level_value(level, "size")
            for level in self._read_book_levels(book, "bids")[:levels]
        )
        ask_depth = sum(
            self._read_level_value(level, "price") * self._read_level_value(level, "size")
            for level in self._read_book_levels(book, "asks")[:levels]
        )
        return bid_depth, ask_depth

    def get_order(self, order_id: str) -> Any:
        client = self._ensure_client()
        return client.get_order(order_id)

    def get_open_orders(self, order_id: str | None = None) -> list[Any]:
        client = self._ensure_client()
        if order_id:
            from py_clob_client.clob_types import OpenOrderParams

            return client.get_orders(OpenOrderParams(id=order_id))
        return client.get_orders()

    def cancel_order(self, order_id: str) -> Any:
        client = self._ensure_client()
        return client.cancel(order_id)

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        tick_size: str = "0.01",
        neg_risk: bool = False,
        expiration: int = 0,
        post_only: bool = True,
    ) -> dict:
        """Place a limit order. Defaults to post-only GTC (guaranteed maker).

        If expiration > 0, uses GTD (auto-cancel at that unix timestamp).
        post_only=True rejects if the order would cross the spread.
        """
        client = self._ensure_client()
        from py_clob_client.clob_types import OrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        order_side = BUY if side.upper() == "BUY" else SELL

        order_args = OrderArgs(
            token_id=token_id,
            price=price,
            size=size,
            side=order_side,
            expiration=expiration if expiration > 0 else 0,
        )
        options = {"tick_size": tick_size, "neg_risk": neg_risk}

        order_type = OrderType.GTD if expiration > 0 else OrderType.GTC

        signed = client.create_order(order_args, options=options)
        return client.post_order(signed, order_type=order_type, post_only=post_only)

    def place_market_order(
        self,
        token_id: str,
        side: str,
        amount: float,
        worst_price: float = 0,
        tick_size: str = "0.01",
        neg_risk: bool = False,
    ) -> dict:
        """Place a FOK market order (fill entirely or cancel)."""
        client = self._ensure_client()
        from py_clob_client.clob_types import OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        order_side = BUY if side.upper() == "BUY" else SELL

        order = client.create_market_order(
            token_id=token_id,
            amount=amount,
            side=order_side,
            price=worst_price if worst_price > 0 else None,
            options={"tick_size": tick_size, "neg_risk": neg_risk},
        )
        return client.post_order(order, order_type=OrderType.FOK)
