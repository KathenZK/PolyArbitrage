"""Polymarket API clients: Gamma (REST) for market discovery, CLOB for trading.

CLOB client uses the official py-clob-client SDK with:
  - official L1 -> L2 auth flow (private key + API creds)
  - configurable signature type / funder for EOA vs proxy wallets
  - Post-Only orders (guarantees maker status, 0% fee)
  - GTD expiration (auto-cancel at window end)
  - Proper options (tick_size, neg_risk)
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

GAMMA_BASE = "https://gamma-api.polymarket.com"
EVENT_PAGE_BASE = "https://polymarket.com/event"


@dataclass
class TokenBookSnapshot:
    token_id: str
    best_bid: float
    best_ask: float
    spread: float
    tick_size: float


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

    async def get_event_by_slug(self, slug: str) -> dict | None:
        session = await self._ensure_session()
        async with session.get(
            f"{GAMMA_BASE}/events",
            params={"slug": slug},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            r.raise_for_status()
            events = await r.json()
        return events[0] if events else None

    async def get_positions(
        self,
        user: str,
        *,
        redeemable: bool | None = None,
        limit: int = 500,
        offset: int = 0,
    ) -> list[dict]:
        params: dict[str, Any] = {
            "user": user,
            "limit": limit,
            "offset": offset,
        }
        if redeemable is not None:
            params["redeemable"] = str(redeemable).lower()
        session = await self._ensure_session()
        async with session.get(
            "https://data-api.polymarket.com/positions",
            params=params,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            r.raise_for_status()
            return await r.json()

    async def check_geoblock(self) -> dict:
        """Check if the current IP is blocked from trading."""
        session = await self._ensure_session()
        async with session.get(
            "https://polymarket.com/api/geoblock",
            timeout=aiohttp.ClientTimeout(total=10),
        ) as r:
            return await r.json()

    @staticmethod
    def _parse_event_page_metadata(slug: str, html: str) -> dict[str, float]:
        start_tag = '<script id="__NEXT_DATA__" type="application/json">'
        start_idx = html.find(start_tag)
        if start_idx < 0:
            return {}
        json_start = start_idx + len(start_tag)
        end_idx = html.find("</script>", json_start)
        if end_idx < 0:
            return {}

        try:
            payload = json.loads(html[json_start:end_idx])
        except (json.JSONDecodeError, ValueError):
            return {}

        data_list = (
            payload
            .get("props", {})
            .get("pageProps", {})
            .get("data", [])
        )
        if not isinstance(data_list, list):
            return {}

        for entry in data_list:
            if not isinstance(entry, dict) or entry.get("slug") != slug:
                continue
            meta = entry.get("eventMetadata")
            if not isinstance(meta, dict):
                continue
            try:
                final_price = float(meta["finalPrice"])
                price_to_beat = float(meta["priceToBeat"])
            except (KeyError, TypeError, ValueError):
                continue
            return {
                "official_current_price": final_price,
                "official_opening_price": price_to_beat,
                "fetched_at": time.time(),
            }
        return {}

    @staticmethod
    def _parse_list_field(raw: Any) -> list[Any]:
        if raw is None:
            return []
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                return []
            return parsed if isinstance(parsed, list) else []
        if isinstance(raw, list):
            return raw
        return []

    @classmethod
    def _parse_outcome_prices(cls, raw: Any) -> list[float]:
        prices: list[float] = []
        for item in cls._parse_list_field(raw):
            try:
                prices.append(float(item))
            except (TypeError, ValueError):
                continue
        return prices

    @staticmethod
    def _normalize_settle_side(label: str) -> str:
        normalized = str(label or "").strip().upper()
        if normalized in {"UP", "DOWN"}:
            return normalized
        return normalized

    @classmethod
    def _extract_resolved_truth(
        cls,
        slug: str,
        event: dict,
        metadata: dict[str, float] | None = None,
    ) -> dict[str, Any]:
        markets = event.get("markets", []) if isinstance(event, dict) else []
        market = markets[0] if markets else {}
        outcomes = cls._parse_list_field(market.get("outcomes"))
        outcome_prices = cls._parse_outcome_prices(market.get("outcomePrices"))
        closed = bool(market.get("closed", False))
        active = bool(market.get("active", False))

        resolved_side = ""
        truth_source = ""
        if len(outcomes) >= 2 and len(outcome_prices) >= 2:
            up_price = float(outcome_prices[0])
            down_price = float(outcome_prices[1])
            if up_price >= 0.999 and down_price <= 0.001:
                resolved_side = cls._normalize_settle_side(str(outcomes[0]))
                truth_source = "gamma_outcome_prices"
            elif down_price >= 0.999 and up_price <= 0.001:
                resolved_side = cls._normalize_settle_side(str(outcomes[1]))
                truth_source = "gamma_outcome_prices"

        metadata = metadata or {}
        official_opening = float(metadata.get("official_opening_price", 0) or 0)
        official_final = float(metadata.get("official_current_price", 0) or 0)
        if not resolved_side and official_opening > 0 and official_final > 0:
            resolved_side = "UP" if official_final >= official_opening else "DOWN"
            truth_source = "event_page_metadata"

        return {
            "market_slug": slug,
            "condition_id": str(market.get("conditionId", "") or ""),
            "market_closed": closed,
            "market_active": active,
            "resolved_truth_available": bool(resolved_side),
            "resolved_truth_source": truth_source,
            "resolved_settle_side": resolved_side,
            "resolved_up_price": outcome_prices[0] if outcome_prices else 0.0,
            "resolved_down_price": outcome_prices[1] if len(outcome_prices) > 1 else 0.0,
            "resolved_official_opening_price": official_opening,
            "resolved_official_final_price": official_final,
            "resolved_checked_at": time.time(),
        }

    async def get_event_page_metadata(self, slug: str) -> dict[str, float]:
        """Fetch the official event page metadata with retry.

        Polymarket's event page embeds the current Chainlink-derived "finalPrice"
        and the window's "priceToBeat". Retries once on failure to improve
        reliability since this is HTML scraping of __NEXT_DATA__.
        """
        session = await self._ensure_session()
        url = f"{EVENT_PAGE_BASE}/{slug}"
        last_exc: Exception | None = None
        for attempt in range(2):
            try:
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=8)
                ) as r:
                    r.raise_for_status()
                    html = await r.text()
                result = self._parse_event_page_metadata(slug, html)
                if result:
                    return result
            except Exception as exc:
                last_exc = exc
            if attempt == 0:
                await asyncio.sleep(0.5)
        if last_exc:
            logger.debug(f"Event page metadata failed for {slug}: {last_exc}")
        return {}

    async def get_resolved_truth(self, slug: str) -> dict[str, Any]:
        event = await self.get_event_by_slug(slug)
        if not event:
            return {
                "market_slug": slug,
                "resolved_truth_available": False,
                "resolved_truth_source": "",
                "resolved_checked_at": time.time(),
            }

        metadata: dict[str, float] = {}
        try:
            metadata = await self.get_event_page_metadata(slug)
        except Exception:
            metadata = {}
        return self._extract_resolved_truth(slug, event, metadata)


class PolymarketCLOBClient:
    """Wrapper around py-clob-client with post-only + GTD support."""

    def __init__(
        self,
        private_key: str = "",
        chain_id: int = 137,
        *,
        signature_type: int = 0,
        funder: str = "",
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
    ):
        self._key = private_key
        self._chain_id = chain_id
        self._signature_type = signature_type
        self._funder = funder
        self._api_key = api_key
        self._api_secret = api_secret
        self._api_passphrase = api_passphrase
        self._client = None

    def _ensure_client(self):
        if self._client is None:
            try:
                from py_clob_client.client import ClobClient
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "py-clob-client is not installed. Run `pip install -r requirements.txt` "
                    "before enabling live Polymarket trading."
                ) from exc

            if self._key:
                client_kwargs = {
                    "host": "https://clob.polymarket.com",
                    "chain_id": self._chain_id,
                    "key": self._key,
                    "signature_type": self._signature_type,
                }
                if self._funder:
                    client_kwargs["funder"] = self._funder

                self._client = ClobClient(**client_kwargs)
                if self._api_key and self._api_secret and self._api_passphrase:
                    self._client.set_api_creds(
                        {
                            "key": self._api_key,
                            "secret": self._api_secret,
                            "passphrase": self._api_passphrase,
                        }
                    )
                else:
                    self._client.set_api_creds(self._client.create_or_derive_api_creds())
            else:
                self._client = ClobClient("https://clob.polymarket.com")
        return self._client

    def get_orderbook(self, token_id: str) -> Any:
        client = self._ensure_client()
        return client.get_order_book(token_id)

    def get_signer_address(self) -> str:
        client = self._ensure_client()
        return str(client.get_address())

    def get_collateral_balance_allowance(self, signature_type: int | None = None) -> Any:
        client = self._ensure_client()
        from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

        params = BalanceAllowanceParams(
            asset_type=AssetType.COLLATERAL,
            signature_type=self._signature_type if signature_type is None else signature_type,
        )
        return client.get_balance_allowance(params)

    def get_collateral_address(self) -> str:
        client = self._ensure_client()
        return str(client.get_collateral_address())

    def get_market(self, condition_id: str) -> Any:
        client = self._ensure_client()
        return client.get_market(condition_id)

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

    def get_tick_size(self, token_id: str) -> float:
        client = self._ensure_client()
        try:
            return float(client.get_tick_size(token_id) or 0.01)
        except (TypeError, ValueError):
            return 0.01

    def get_book_snapshot(self, token_id: str) -> TokenBookSnapshot:
        book = self.get_orderbook(token_id)
        bids = self._read_book_levels(book, "bids")
        asks = self._read_book_levels(book, "asks")
        best_bid = self._read_level_value(bids[0], "price") if bids else 0.0
        best_ask = self._read_level_value(asks[0], "price") if asks else 0.0
        if best_bid > 0 and best_ask > 0:
            spread = max(0.0, best_ask - best_bid)
        else:
            spread = 0.0

        tick_size = 0.01
        if isinstance(book, dict):
            try:
                tick_size = float(book.get("tick_size", 0.01) or 0.01)
            except (TypeError, ValueError):
                tick_size = 0.01
        else:
            try:
                tick_size = float(getattr(book, "tick_size", 0.01) or 0.01)
            except (TypeError, ValueError):
                tick_size = 0.01

        if tick_size <= 0:
            tick_size = self.get_tick_size(token_id)

        return TokenBookSnapshot(
            token_id=token_id,
            best_bid=best_bid,
            best_ask=best_ask,
            spread=spread,
            tick_size=tick_size if tick_size > 0 else 0.01,
        )

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

    def post_heartbeat(self, heartbeat_id: str | None = None) -> Any:
        client = self._ensure_client()
        return client.post_heartbeat(heartbeat_id)

    def place_limit_order(
        self,
        token_id: str,
        side: str,
        price: float,
        size: float,
        tick_size: str | None = None,
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
        options: dict[str, Any] = {"neg_risk": neg_risk}
        if tick_size:
            options["tick_size"] = tick_size

        order_type = OrderType.GTD if expiration > 0 else OrderType.GTC

        signed = client.create_order(order_args, options=options)
        return client.post_order(signed, order_type=order_type, post_only=post_only)

    def place_market_order(
        self,
        token_id: str,
        side: str,
        amount: float,
        worst_price: float = 0,
        tick_size: str | None = None,
        neg_risk: bool = False,
    ) -> dict:
        """Place a FOK market order (fill entirely or cancel)."""
        client = self._ensure_client()
        from py_clob_client.clob_types import OrderType
        from py_clob_client.order_builder.constants import BUY, SELL

        order_side = BUY if side.upper() == "BUY" else SELL

        options: dict[str, Any] = {"neg_risk": neg_risk}
        if tick_size:
            options["tick_size"] = tick_size

        order = client.create_market_order(
            token_id=token_id,
            amount=amount,
            side=order_side,
            price=worst_price if worst_price > 0 else None,
            options=options,
        )
        return client.post_order(order, order_type=OrderType.FOK)
