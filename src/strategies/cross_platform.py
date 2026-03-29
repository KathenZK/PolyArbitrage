"""Strategy 1: Cross-platform arbitrage between Polymarket and Kalshi.

Finds same events on both platforms, computes the cheapest YES + cheapest NO
across platforms, and detects when the sum is less than $1.00 (net of fees).
"""

from __future__ import annotations

import json
import logging

from src.data.kalshi_client import KalshiClient
from src.data.market_store import CrossPlatformPair, MarketStore
from src.data.polymarket_client import PolymarketGammaClient
from src.matching.event_matcher import EventMatcher
from src.matching.resolution_checker import ResolutionChecker
from src.strategies.base import Action, BaseStrategy, Opportunity

logger = logging.getLogger(__name__)


class CrossPlatformStrategy(BaseStrategy):
    def __init__(
        self,
        gamma_client: PolymarketGammaClient,
        store: MarketStore,
        kalshi_key_id: str = "",
        kalshi_pk_path: str = "",
        min_gross_edge: float = 0.0275,
        kalshi_fee: float = 0.0175,
        poly_fee: float = 0.0,
        bridge_cost: float = 0.005,
    ):
        self._gamma = gamma_client
        self._store = store
        self._kalshi = KalshiClient(api_key_id=kalshi_key_id, private_key_path=kalshi_pk_path)
        self._matcher = EventMatcher(min_confidence=0.65)
        self._resolution_checker = ResolutionChecker()
        self._min_gross_edge = min_gross_edge
        self._kalshi_fee = kalshi_fee
        self._poly_fee = poly_fee
        self._bridge_cost = bridge_cost

    @property
    def name(self) -> str:
        return "Cross-Platform"

    def calculate_edge(
        self,
        poly_yes: float = 0,
        poly_no: float = 0,
        kalshi_yes: float = 0,
        kalshi_no: float = 0,
        **kwargs,
    ) -> float:
        cheapest_yes = min(poly_yes, kalshi_yes)
        cheapest_no = min(poly_no, kalshi_no)
        gross_edge = 1.0 - (cheapest_yes + cheapest_no)
        return gross_edge

    async def scan(self) -> list[Opportunity]:
        poly_raw = await self._gamma.get_markets(limit=100)
        poly_markets = [m for m in poly_raw if m.get("active")]

        try:
            kalshi_markets_obj = await self._kalshi.get_all_open_markets()
            kalshi_raw = [
                {
                    "ticker": km.ticker,
                    "event_ticker": km.event_ticker,
                    "title": km.title,
                    "subtitle": km.subtitle,
                    "yes_bid": km.yes_bid,
                    "yes_ask": km.yes_ask,
                    "no_bid": km.no_bid,
                    "no_ask": km.no_ask,
                    "volume": km.volume,
                    "close_time": km.close_time,
                    "status": km.status,
                }
                for km in kalshi_markets_obj
            ]
        except Exception as e:
            logger.warning(f"Kalshi API error (using cached data): {e}")
            kalshi_raw = [
                {"ticker": k, **v} for k, v in self._store.get_kalshi_markets().items()
            ]

        matches = self._matcher.match(poly_markets, kalshi_raw)
        opportunities: list[Opportunity] = []

        for match in matches:
            pm = next((m for m in poly_markets if m.get("id") == match.poly_id), None)
            km = next((m for m in kalshi_raw if m.get("ticker") == match.kalshi_ticker), None)
            if not pm or not km:
                continue

            prices_raw = pm.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                try:
                    prices = [float(p) for p in json.loads(prices_raw)]
                except (json.JSONDecodeError, ValueError):
                    continue
            else:
                prices = [float(p) for p in prices_raw]

            if len(prices) < 2:
                continue

            poly_yes, poly_no = prices[0], prices[1]
            kalshi_yes = km.get("yes_ask", 0) or km.get("yes_bid", 0)
            kalshi_no = km.get("no_ask", 0) or km.get("no_bid", 0)

            if not all([poly_yes, poly_no, kalshi_yes, kalshi_no]):
                continue

            cheapest_yes = min(poly_yes, kalshi_yes)
            cheapest_no = min(poly_no, kalshi_no)
            gross_edge = 1.0 - (cheapest_yes + cheapest_no)

            if gross_edge < self._min_gross_edge:
                continue

            total_fees = self._kalshi_fee + self._poly_fee + self._bridge_cost
            net_edge = gross_edge - total_fees

            if net_edge <= 0:
                continue

            res_check = self._resolution_checker.check(
                pm.get("question", ""), km.get("title", "")
            )

            buy_yes_on = "Kalshi" if kalshi_yes <= poly_yes else "Polymarket"
            buy_no_on = "Polymarket" if poly_no <= kalshi_no else "Kalshi"

            pair = CrossPlatformPair(
                poly_market_id=pm.get("id", ""),
                kalshi_ticker=km.get("ticker", ""),
                event_description=pm.get("question", ""),
                poly_yes_price=poly_yes,
                poly_no_price=poly_no,
                kalshi_yes_price=kalshi_yes,
                kalshi_no_price=kalshi_no,
                cheapest_yes=cheapest_yes,
                cheapest_no=cheapest_no,
                gross_edge=gross_edge,
                net_edge=net_edge,
                match_confidence=match.confidence,
                resolution_match=res_check.is_safe,
                poly_end_date=pm.get("endDate", ""),
                kalshi_end_date=km.get("close_time", ""),
            )
            self._store.update_cross_pair(pair)

            opportunities.append(Opportunity(
                strategy=self.name,
                event_title=pm.get("question", ""),
                action=Action.CROSS_ARB,
                edge_pct=round(net_edge * 100, 2),
                settlement_date=pm.get("endDate", ""),
                details={
                    "poly_yes": poly_yes,
                    "poly_no": poly_no,
                    "kalshi_yes": kalshi_yes,
                    "kalshi_no": kalshi_no,
                    "cheapest_yes": cheapest_yes,
                    "cheapest_no": cheapest_no,
                    "gross_edge_pct": round(gross_edge * 100, 2),
                    "net_edge_pct": round(net_edge * 100, 2),
                    "total_fees_pct": round(total_fees * 100, 2),
                    "buy_yes_on": buy_yes_on,
                    "buy_no_on": buy_no_on,
                    "match_confidence": match.confidence,
                    "resolution_risk": res_check.risk_level,
                    "resolution_warnings": res_check.warnings,
                },
                market_ids=[pm.get("id", ""), km.get("ticker", "")],
                confidence=match.confidence,
            ))

        opportunities.sort(key=lambda o: o.edge_pct, reverse=True)
        return opportunities
