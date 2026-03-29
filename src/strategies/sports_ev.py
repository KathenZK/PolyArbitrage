"""Strategy 3: Sports EV betting — uses Elo + 6-factor model to find
mispriced NBA/NCAA game-day markets on Polymarket.
"""

from __future__ import annotations

import logging
import re

from src.data.espn_client import ESPNClient
from src.data.market_store import MarketStore, SportMatch
from src.data.polymarket_client import PolymarketGammaClient
from src.models.elo import EloSystem
from src.models.six_factor import GameContext, SixFactorModel
from src.strategies.base import Action, BaseStrategy, Opportunity

logger = logging.getLogger(__name__)


class SportsEVStrategy(BaseStrategy):
    def __init__(
        self,
        gamma_client: PolymarketGammaClient,
        store: MarketStore,
        min_edge: float = 0.04,
        sports: list[str] | None = None,
    ):
        self._gamma = gamma_client
        self._store = store
        self._espn = ESPNClient()
        self._elo = EloSystem()
        self._elo.load()
        self._six_factor = SixFactorModel()
        self._min_edge = min_edge
        self._sports = sports or ["nba"]

    @property
    def name(self) -> str:
        return "Sports EV"

    def calculate_edge(self, model_prob: float = 0, market_price: float = 0, **kwargs) -> float:
        return model_prob - market_price

    async def scan(self) -> list[Opportunity]:
        opportunities: list[Opportunity] = []

        for sport in self._sports:
            try:
                opps = await self._scan_sport(sport)
                opportunities.extend(opps)
            except Exception as e:
                logger.error(f"Error scanning {sport}: {e}")

        opportunities.sort(key=lambda o: o.edge_pct, reverse=True)
        return opportunities

    async def _scan_sport(self, sport: str) -> list[Opportunity]:
        games = await self._espn.get_scoreboard(sport)
        pre_games = [g for g in games if g.status == "pre"]

        if not pre_games:
            return []

        poly_markets = await self._gamma.get_sports_markets(sport)

        opportunities: list[Opportunity] = []

        for game in pre_games:
            matched = self._match_game_to_market(game, poly_markets)
            if not matched:
                continue

            market_raw = matched
            prices_raw = market_raw.get("outcomePrices", "[]")
            if isinstance(prices_raw, str):
                import json
                try:
                    prices = [float(p) for p in json.loads(prices_raw)]
                except Exception:
                    continue
            else:
                prices = [float(p) for p in prices_raw]

            if len(prices) < 2:
                continue

            market_yes = prices[0]
            market_no = prices[1]

            elo_prob = self._elo.predict_win_probability(
                game.home_team.display_name,
                game.away_team.display_name,
                team_a_is_home=True,
            )

            context = GameContext(
                team_a_is_home=True,
                team_a_last5=self._estimate_recent_form(game.home_team),
                team_b_last5=self._estimate_recent_form(game.away_team),
            )
            six_factor_prob = self._six_factor.predict(context)

            model_prob = 0.6 * elo_prob + 0.4 * six_factor_prob

            edge = model_prob - market_yes
            abs_edge = abs(edge)

            if abs_edge < self._min_edge:
                continue

            if edge > 0:
                action = Action.BUY_YES
                side_label = f"BUY {game.home_team.abbreviation} YES"
            else:
                action = Action.BUY_NO
                side_label = f"BUY {game.home_team.abbreviation} NO"

            kelly = abs_edge / (1.0 - market_yes if edge > 0 else market_yes)
            half_kelly = min(kelly * 0.5, 0.02)

            match_obj = SportMatch(
                event_id=game.game_id,
                market_id=market_raw.get("id", ""),
                home_team=game.home_team.display_name,
                away_team=game.away_team.display_name,
                market_yes_price=market_yes,
                market_no_price=market_no,
                model_probability=model_prob,
                edge=edge,
                sport=sport,
                game_date=game.start_time,
                volume=float(market_raw.get("volume", 0) or 0),
            )
            self._store.update_sport_match(match_obj)

            opportunities.append(Opportunity(
                strategy=self.name,
                event_title=f"{game.away_team.abbreviation} @ {game.home_team.abbreviation}",
                action=action,
                edge_pct=round(abs_edge * 100, 2),
                settlement_date=game.start_time,
                details={
                    "home": game.home_team.display_name,
                    "away": game.away_team.display_name,
                    "market_yes": market_yes,
                    "market_no": market_no,
                    "elo_prob": round(elo_prob, 4),
                    "six_factor_prob": round(six_factor_prob, 4),
                    "model_prob": round(model_prob, 4),
                    "edge": round(edge, 4),
                    "side": side_label,
                    "half_kelly": round(half_kelly, 4),
                    "sport": sport,
                    "home_record": game.home_team.record,
                    "away_record": game.away_team.record,
                },
                market_ids=[market_raw.get("id", "")],
            ))

        return opportunities

    def _match_game_to_market(self, game, poly_markets: list[dict]) -> dict | None:
        """Fuzzy-match an ESPN game to a Polymarket market."""
        home = game.home_team.display_name.lower()
        away = game.away_team.display_name.lower()
        home_abbr = game.home_team.abbreviation.lower()
        away_abbr = game.away_team.abbreviation.lower()
        home_name = game.home_team.name.lower()
        away_name = game.away_team.name.lower()

        for m in poly_markets:
            q = m.get("question", "").lower()
            if not q:
                continue

            home_match = any(t in q for t in [home, home_abbr, home_name])
            away_match = any(t in q for t in [away, away_abbr, away_name])

            is_moneyline = any(kw in q for kw in ["win", "beat", "defeat", "vs", "vs."])

            if home_match and away_match and is_moneyline:
                return m

        return None

    @staticmethod
    def _estimate_recent_form(team) -> list[int]:
        """Estimate recent form from season record (simplified)."""
        if team.wins + team.losses == 0:
            return [1, 0, 1, 0, 1]
        win_rate = team.wins / (team.wins + team.losses)
        import random
        random.seed(hash(team.display_name))
        return [1 if random.random() < win_rate else 0 for _ in range(5)]
