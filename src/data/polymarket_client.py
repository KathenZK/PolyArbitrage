"""Polymarket API clients: Gamma (REST) for market discovery, CLOB for trading."""

from __future__ import annotations

import json
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


class PolymarketCLOBClient:
    """Wrapper around py-clob-client for live trading (requires private key)."""

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

    def get_price(self, token_id: str, side: str = "buy") -> float:
        client = self._ensure_client()
        return float(client.get_price(token_id, side))

    def place_market_order(self, token_id: str, side: str, size: float):
        client = self._ensure_client()
        from py_clob_client.order_builder.constants import BUY, SELL

        order_side = BUY if side.upper() == "BUY" else SELL
        order = client.create_market_order(
            token_id=token_id,
            amount=size,
            side=order_side,
        )
        return client.post_order(order)

    def place_limit_order(self, token_id: str, side: str, price: float, size: float):
        client = self._ensure_client()
        from py_clob_client.order_builder.constants import BUY, SELL

        order_side = BUY if side.upper() == "BUY" else SELL
        signed = client.create_order(
            token_id=token_id,
            price=price,
            size=size,
            side=order_side,
        )
        return client.post_order(signed)
