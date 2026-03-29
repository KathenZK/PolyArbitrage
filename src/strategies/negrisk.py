"""Strategy 2: NegRisk multi-outcome rebalancing scanner.

Scans all Polymarket NegRisk events, computes the sum of all YES prices,
and detects deviations from 1.0 that represent arbitrage opportunities.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone

from src.data.market_store import MarketStore, NegRiskEvent, NegRiskOutcome
from src.data.polymarket_client import PolymarketGammaClient
from src.strategies.base import Action, BaseStrategy, Opportunity

logger = logging.getLogger(__name__)


class NegRiskStrategy(BaseStrategy):
    def __init__(
        self,
        gamma_client: PolymarketGammaClient,
        store: MarketStore,
        min_deviation: float = 0.03,
        min_daily_volume: float = 50_000,
        max_settlement_days: int = 30,
    ):
        self._gamma = gamma_client
        self._store = store
        self._min_deviation = min_deviation
        self._min_daily_volume = min_daily_volume
        self._max_settlement_days = max_settlement_days

    @property
    def name(self) -> str:
        return "NegRisk Rebalancing"

    def calculate_edge(self, yes_sum: float = 0.0, **kwargs) -> float:
        return abs(1.0 - yes_sum)

    async def scan(self) -> list[Opportunity]:
        """Fetch all NegRisk events and detect price-sum deviations."""
        events = await self._gamma.get_all_negrisk_events()
        opportunities: list[Opportunity] = []

        for event_raw in events:
            try:
                neg_event = self._parse_event(event_raw)
                if neg_event is None:
                    continue
                self._store.update_negrisk_event(neg_event)

                opp = self._evaluate(neg_event)
                if opp:
                    opportunities.append(opp)
            except Exception as e:
                logger.debug(f"Error parsing event {event_raw.get('id', '?')}: {e}")

        opportunities.sort(key=lambda o: o.edge_pct, reverse=True)
        return opportunities

    def _parse_event(self, raw: dict) -> NegRiskEvent | None:
        markets_raw = raw.get("markets", [])
        if not markets_raw or len(markets_raw) < 2:
            return None

        outcomes: list[NegRiskOutcome] = []
        total_volume = 0.0
        total_liquidity = 0.0

        for m in markets_raw:
            if not m.get("active", True):
                continue

            prices_raw = m.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                try:
                    prices = [float(p) for p in json.loads(prices_raw)]
                except (json.JSONDecodeError, ValueError):
                    continue
            elif isinstance(prices_raw, list):
                prices = [float(p) for p in prices_raw]
            else:
                continue

            if len(prices) < 2:
                continue

            tokens_raw = m.get("clobTokenIds", "[]")
            if isinstance(tokens_raw, str):
                try:
                    tokens = json.loads(tokens_raw)
                except json.JSONDecodeError:
                    tokens = ["", ""]
            elif isinstance(tokens_raw, list):
                tokens = tokens_raw
            else:
                tokens = ["", ""]

            vol = float(m.get("volume", 0) or 0)
            liq = float(m.get("liquidity", 0) or 0)
            total_volume += vol
            total_liquidity += liq

            outcomes.append(NegRiskOutcome(
                market_id=m.get("id", ""),
                question=m.get("question", ""),
                yes_price=prices[0],
                no_price=prices[1] if len(prices) > 1 else 1.0 - prices[0],
                yes_token_id=tokens[0] if tokens else "",
                no_token_id=tokens[1] if len(tokens) > 1 else "",
                volume=vol,
                liquidity=liq,
            ))

        if len(outcomes) < 2:
            return None

        return NegRiskEvent(
            event_id=raw.get("id", ""),
            title=raw.get("title", ""),
            outcomes=outcomes,
            total_volume=total_volume,
            total_liquidity=total_liquidity,
            end_date=raw.get("endDate", ""),
        )

    def _evaluate(self, event: NegRiskEvent) -> Opportunity | None:
        if event.total_volume < self._min_daily_volume:
            return None

        if event.end_date:
            try:
                end = datetime.fromisoformat(event.end_date.replace("Z", "+00:00"))
                now = datetime.now(timezone.utc)
                days_to_settle = (end - now).days
                if days_to_settle > self._max_settlement_days:
                    return None
            except (ValueError, TypeError):
                pass

        deviation = event.deviation
        edge = abs(deviation)
        if edge < self._min_deviation:
            return None

        if deviation > 0:
            action = Action.BUY_ALL_YES
            cost = event.yes_price_sum
        else:
            action = Action.BUY_ALL_NO
            cost = sum(o.no_price for o in event.outcomes)

        profit_pct = edge / cost * 100 if cost > 0 else 0

        outcome_details = [
            {"question": o.question, "yes": f"${o.yes_price:.3f}", "liq": f"${o.liquidity:,.0f}"}
            for o in event.outcomes
        ]

        return Opportunity(
            strategy=self.name,
            event_title=event.title,
            action=action,
            edge_pct=round(edge * 100, 2),
            settlement_date=event.end_date,
            estimated_profit_usd=0.0,
            details={
                "yes_sum": round(event.yes_price_sum, 4),
                "deviation": round(deviation, 4),
                "cost": round(cost, 4),
                "profit_pct": round(profit_pct, 2),
                "outcome_count": event.outcome_count,
                "total_volume": round(event.total_volume, 0),
                "total_liquidity": round(event.total_liquidity, 0),
                "outcomes": outcome_details,
            },
            market_ids=[o.market_id for o in event.outcomes],
        )
