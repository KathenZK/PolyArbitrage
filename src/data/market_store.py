"""In-memory cache of market state from all platforms."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from threading import Lock

logger = logging.getLogger(__name__)


@dataclass
class NegRiskEvent:
    """A multi-outcome NegRisk event with its constituent markets."""
    event_id: str
    title: str
    outcomes: list[NegRiskOutcome]
    total_volume: float = 0.0
    total_liquidity: float = 0.0
    end_date: str = ""
    updated_at: float = 0.0

    @property
    def yes_price_sum(self) -> float:
        return sum(o.yes_price for o in self.outcomes)

    @property
    def deviation(self) -> float:
        return 1.0 - self.yes_price_sum

    @property
    def outcome_count(self) -> int:
        return len(self.outcomes)


@dataclass
class NegRiskOutcome:
    market_id: str
    question: str
    yes_price: float
    no_price: float
    yes_token_id: str
    no_token_id: str
    volume: float = 0.0
    liquidity: float = 0.0


@dataclass
class SportMatch:
    """A sports game with market data and model predictions."""
    event_id: str
    market_id: str
    home_team: str
    away_team: str
    market_yes_price: float
    market_no_price: float
    model_probability: float | None = None
    edge: float | None = None
    sport: str = "nba"
    game_date: str = ""
    yes_token_id: str = ""
    no_token_id: str = ""
    volume: float = 0.0


@dataclass
class CrossPlatformPair:
    """Matched event across Polymarket and Kalshi."""
    poly_market_id: str
    kalshi_ticker: str
    event_description: str
    poly_yes_price: float
    poly_no_price: float
    kalshi_yes_price: float
    kalshi_no_price: float
    cheapest_yes: float = 0.0
    cheapest_no: float = 0.0
    gross_edge: float = 0.0
    net_edge: float = 0.0
    match_confidence: float = 0.0
    resolution_match: bool = True
    poly_end_date: str = ""
    kalshi_end_date: str = ""


class MarketStore:
    """Thread-safe in-memory market state cache."""

    def __init__(self):
        self._lock = Lock()
        self._negrisk_events: dict[str, NegRiskEvent] = {}
        self._sport_matches: dict[str, SportMatch] = {}
        self._cross_pairs: dict[str, CrossPlatformPair] = {}
        self._poly_markets: dict[str, dict] = {}
        self._kalshi_markets: dict[str, dict] = {}
        self._last_poly_update: float = 0.0
        self._last_kalshi_update: float = 0.0

    def update_negrisk_event(self, event: NegRiskEvent):
        with self._lock:
            event.updated_at = time.time()
            self._negrisk_events[event.event_id] = event

    def get_negrisk_events(self) -> list[NegRiskEvent]:
        with self._lock:
            return list(self._negrisk_events.values())

    def update_sport_match(self, match: SportMatch):
        with self._lock:
            self._sport_matches[match.market_id] = match

    def get_sport_matches(self) -> list[SportMatch]:
        with self._lock:
            return list(self._sport_matches.values())

    def update_cross_pair(self, pair: CrossPlatformPair):
        with self._lock:
            key = f"{pair.poly_market_id}:{pair.kalshi_ticker}"
            self._cross_pairs[key] = pair

    def get_cross_pairs(self) -> list[CrossPlatformPair]:
        with self._lock:
            return list(self._cross_pairs.values())

    def set_poly_markets(self, markets: dict[str, dict]):
        with self._lock:
            self._poly_markets = markets
            self._last_poly_update = time.time()

    def get_poly_markets(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._poly_markets)

    def set_kalshi_markets(self, markets: dict[str, dict]):
        with self._lock:
            self._kalshi_markets = markets
            self._last_kalshi_update = time.time()

    def get_kalshi_markets(self) -> dict[str, dict]:
        with self._lock:
            return dict(self._kalshi_markets)

    def update_price(self, token_id: str, price: float):
        """Update a single token's price from WebSocket."""
        with self._lock:
            for event in self._negrisk_events.values():
                for outcome in event.outcomes:
                    if outcome.yes_token_id == token_id:
                        outcome.yes_price = price
                        event.updated_at = time.time()
                        return
                    elif outcome.no_token_id == token_id:
                        outcome.no_price = price
                        event.updated_at = time.time()
                        return
