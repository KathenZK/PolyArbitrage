"""Kalshi REST API client for market data and trading."""

from __future__ import annotations

import hashlib
import hmac
import logging
import time
from base64 import b64encode, b64decode
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

KALSHI_BASE = "https://api.elections.kalshi.com/trade-api/v2"
KALSHI_DEMO_BASE = "https://demo-api.kalshi.co/trade-api/v2"


@dataclass
class KalshiMarket:
    ticker: str
    event_ticker: str
    title: str
    subtitle: str
    yes_bid: float
    yes_ask: float
    no_bid: float
    no_ask: float
    volume: int
    open_interest: int
    status: str
    close_time: str
    result: str = ""
    category: str = ""

    @property
    def yes_mid(self) -> float:
        if self.yes_bid > 0 and self.yes_ask > 0:
            return (self.yes_bid + self.yes_ask) / 2
        return self.yes_ask or self.yes_bid

    @property
    def no_mid(self) -> float:
        if self.no_bid > 0 and self.no_ask > 0:
            return (self.no_bid + self.no_ask) / 2
        return self.no_ask or self.no_bid


class KalshiClient:
    """Async REST client for Kalshi's trade API."""

    def __init__(
        self,
        api_key_id: str = "",
        private_key_path: str = "",
        use_demo: bool = False,
    ):
        self._key_id = api_key_id
        self._pk_path = private_key_path
        self._base = KALSHI_DEMO_BASE if use_demo else KALSHI_BASE
        self._session: aiohttp.ClientSession | None = None
        self._private_key = None
        if private_key_path and Path(private_key_path).exists():
            self._private_key = Path(private_key_path).read_text()

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    def _sign_request(self, method: str, path: str) -> dict[str, str]:
        """Generate RSA-PSS auth headers. Returns empty dict if no credentials."""
        if not self._key_id or not self._private_key:
            return {}

        timestamp = str(int(time.time() * 1000))
        message = f"{timestamp}{method.upper()}{path}"

        try:
            from cryptography.hazmat.primitives import hashes, serialization
            from cryptography.hazmat.primitives.asymmetric import padding

            key = serialization.load_pem_private_key(self._private_key.encode(), password=None)
            signature = key.sign(
                message.encode(),
                padding.PSS(
                    mgf=padding.MGF1(hashes.SHA256()),
                    salt_length=padding.PSS.MAX_LENGTH,
                ),
                hashes.SHA256(),
            )
            return {
                "KALSHI-ACCESS-KEY": self._key_id,
                "KALSHI-ACCESS-TIMESTAMP": timestamp,
                "KALSHI-ACCESS-SIGNATURE": b64encode(signature).decode(),
            }
        except ImportError:
            logger.warning("cryptography package not installed; Kalshi auth disabled")
            return {}
        except Exception as e:
            logger.warning(f"Kalshi signing error: {e}")
            return {}

    async def _get(self, path: str, params: dict | None = None) -> Any:
        session = await self._ensure_session()
        url = f"{self._base}{path}"
        headers = self._sign_request("GET", path)
        async with session.get(
            url, params=params, headers=headers, timeout=aiohttp.ClientTimeout(total=15)
        ) as r:
            r.raise_for_status()
            return await r.json()

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_events(self, status: str = "open", limit: int = 100, cursor: str = "") -> dict:
        params: dict[str, Any] = {"status": status, "limit": limit}
        if cursor:
            params["cursor"] = cursor
        return await self._get("/events", params)

    async def get_markets(
        self,
        event_ticker: str = "",
        status: str = "open",
        limit: int = 100,
        cursor: str = "",
    ) -> dict:
        params: dict[str, Any] = {"status": status, "limit": limit}
        if event_ticker:
            params["event_ticker"] = event_ticker
        if cursor:
            params["cursor"] = cursor
        return await self._get("/markets", params)

    async def get_all_open_markets(self) -> list[KalshiMarket]:
        """Paginate through all open markets."""
        all_markets: list[KalshiMarket] = []
        cursor = ""
        while True:
            data = await self.get_markets(status="open", limit=100, cursor=cursor)
            markets_raw = data.get("markets", [])
            for m in markets_raw:
                all_markets.append(self._parse_market(m))
            cursor = data.get("cursor", "")
            if not cursor or not markets_raw:
                break
        return all_markets

    async def get_orderbook(self, ticker: str) -> dict:
        return await self._get(f"/markets/{ticker}/orderbook")

    @staticmethod
    def _parse_market(raw: dict) -> KalshiMarket:
        return KalshiMarket(
            ticker=raw.get("ticker", ""),
            event_ticker=raw.get("event_ticker", ""),
            title=raw.get("title", ""),
            subtitle=raw.get("subtitle", ""),
            yes_bid=raw.get("yes_bid", 0) / 100 if raw.get("yes_bid") else 0.0,
            yes_ask=raw.get("yes_ask", 0) / 100 if raw.get("yes_ask") else 0.0,
            no_bid=raw.get("no_bid", 0) / 100 if raw.get("no_bid") else 0.0,
            no_ask=raw.get("no_ask", 0) / 100 if raw.get("no_ask") else 0.0,
            volume=raw.get("volume", 0) or 0,
            open_interest=raw.get("open_interest", 0) or 0,
            status=raw.get("status", ""),
            close_time=raw.get("close_time", ""),
            result=raw.get("result", ""),
            category=raw.get("category", ""),
        )
