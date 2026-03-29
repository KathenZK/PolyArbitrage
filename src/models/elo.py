"""Elo rating system for NBA/NCAA game prediction.

Based on FiveThirtyEight methodology:
- Margin-of-victory adjusted K-factor
- Home court advantage (+100 Elo)
- Season reset: 75% carry-forward + 25% regression to 1505
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

DEFAULT_ELO = 1500
HOME_ADVANTAGE = 100
SEASON_CARRYOVER = 0.75
SEASON_MEAN = 1505

ELO_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "elo_ratings.json"


class EloSystem:
    def __init__(self):
        self.ratings: dict[str, float] = {}

    def get_elo(self, team: str) -> float:
        return self.ratings.get(team, DEFAULT_ELO)

    def predict_win_probability(
        self,
        team_a: str,
        team_b: str,
        team_a_is_home: bool = True,
    ) -> float:
        """Predict team_a's win probability against team_b."""
        elo_a = self.get_elo(team_a)
        elo_b = self.get_elo(team_b)

        if team_a_is_home:
            elo_a += HOME_ADVANTAGE
        else:
            elo_b += HOME_ADVANTAGE

        return 1.0 / (1.0 + 10.0 ** ((elo_b - elo_a) / 400.0))

    def update_after_game(
        self,
        winner: str,
        loser: str,
        margin_of_victory: int,
        winner_is_home: bool = True,
    ):
        """Update Elo ratings after a completed game."""
        winner_elo = self.get_elo(winner)
        loser_elo = self.get_elo(loser)

        if winner_is_home:
            winner_elo_adj = winner_elo + HOME_ADVANTAGE
            loser_elo_adj = loser_elo
        else:
            winner_elo_adj = winner_elo
            loser_elo_adj = loser_elo + HOME_ADVANTAGE

        elo_diff = winner_elo_adj - loser_elo_adj
        k = self._calc_k(margin_of_victory, elo_diff)

        e_winner = 1.0 / (1.0 + 10.0 ** ((loser_elo_adj - winner_elo_adj) / 400.0))

        self.ratings[winner] = winner_elo + k * (1.0 - e_winner)
        self.ratings[loser] = loser_elo + k * (0.0 - (1.0 - e_winner))

    @staticmethod
    def _calc_k(margin_of_victory: int, elo_diff_winner: float) -> float:
        """FiveThirtyEight K-factor: scales with margin of victory."""
        mov = abs(margin_of_victory)
        denom = 7.5 + 0.006 * max(elo_diff_winner, 0)
        return 20.0 * ((mov + 3) ** 0.8) / denom

    def new_season_reset(self):
        """Regress all ratings toward the mean at season boundary."""
        for team in list(self.ratings.keys()):
            self.ratings[team] = SEASON_CARRYOVER * self.ratings[team] + (1 - SEASON_CARRYOVER) * SEASON_MEAN

    def save(self, path: Path | None = None):
        p = path or ELO_FILE
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(self.ratings, indent=2))

    def load(self, path: Path | None = None):
        p = path or ELO_FILE
        if p.exists():
            self.ratings = json.loads(p.read_text())
            logger.info(f"Loaded Elo ratings for {len(self.ratings)} teams")
        else:
            logger.info("No saved Elo ratings found, starting fresh")
