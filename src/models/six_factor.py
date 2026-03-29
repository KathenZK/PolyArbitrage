"""Six-factor weighted probability model for NBA/NCAA game prediction.

Factors:
1. Home court advantage
2. Rest / back-to-back penalty
3. Recent form (weighted last 5 games)
4. Injury impact
5. Head-to-head record
6. Media sentiment (placeholder)

Each factor has an adaptive weight (0.5 - 1.5) updated by the calibrator.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path

logger = logging.getLogger(__name__)

WEIGHTS_FILE = Path(__file__).resolve().parent.parent.parent / "data" / "adaptive_weights.json"

DEFAULT_WEIGHTS = {
    "home_court": 1.0,
    "rest": 1.0,
    "form": 1.0,
    "injuries": 1.0,
    "h2h": 1.0,
    "sentiment": 1.0,
}

INJURY_IMPACT = {
    "OUT": -0.15,
    "DOUBTFUL": -0.10,
    "QUESTIONABLE": -0.07,
    "DAY_TO_DAY": -0.05,
    "PROBABLE": -0.02,
}

FORM_WEIGHTS = [0.30, 0.25, 0.20, 0.15, 0.10]
MAX_INJURY_PENALTY = -0.30
HOME_COURT_BONUS = 0.05
B2B_PENALTY = -0.08
REST_BONUS = 0.06


@dataclass
class GameContext:
    team_a_is_home: bool = True
    team_a_rest_days: int = 2
    team_b_rest_days: int = 2
    team_a_last5: list[int] = field(default_factory=lambda: [1, 1, 0, 1, 0])
    team_b_last5: list[int] = field(default_factory=lambda: [0, 1, 1, 0, 1])
    team_a_injuries: list[dict] = field(default_factory=list)
    team_b_injuries: list[dict] = field(default_factory=list)
    h2h_winrate_a: float = 0.5
    sentiment_a: float = 0.0
    sentiment_b: float = 0.0


class SixFactorModel:
    def __init__(self):
        self.weights = dict(DEFAULT_WEIGHTS)
        self._load_weights()

    def predict(self, context: GameContext) -> float:
        """Estimate team A's win probability given game context."""
        prob = 0.50

        # Factor 1: Home court
        if context.team_a_is_home:
            prob += HOME_COURT_BONUS * self.weights["home_court"]
        else:
            prob -= HOME_COURT_BONUS * self.weights["home_court"]

        # Factor 2: Rest
        if context.team_a_rest_days == 0:
            prob += B2B_PENALTY * self.weights["rest"]
        if context.team_b_rest_days == 0:
            prob += REST_BONUS * self.weights["rest"]

        # Factor 3: Recent form
        form_a = sum(
            w * r for w, r in zip(FORM_WEIGHTS, context.team_a_last5[:5])
        ) if context.team_a_last5 else 0.5
        form_b = sum(
            w * r for w, r in zip(FORM_WEIGHTS, context.team_b_last5[:5])
        ) if context.team_b_last5 else 0.5
        prob += (form_a - form_b) * 0.15 * self.weights["form"]

        # Factor 4: Injuries
        impact_a = sum(
            INJURY_IMPACT.get(inj.get("status", ""), 0) * inj.get("importance", 0.5)
            for inj in context.team_a_injuries
        )
        impact_b = sum(
            INJURY_IMPACT.get(inj.get("status", ""), 0) * inj.get("importance", 0.5)
            for inj in context.team_b_injuries
        )
        impact_a = max(impact_a, MAX_INJURY_PENALTY)
        impact_b = max(impact_b, MAX_INJURY_PENALTY)
        prob += (impact_b - impact_a) * self.weights["injuries"]

        # Factor 5: Head-to-head
        prob += (context.h2h_winrate_a - 0.5) * 0.1 * self.weights["h2h"]

        # Factor 6: Sentiment
        sentiment_diff = context.sentiment_a - context.sentiment_b
        prob += sentiment_diff * 0.03 * self.weights["sentiment"]

        return max(0.05, min(0.95, prob))

    def _load_weights(self):
        if WEIGHTS_FILE.exists():
            try:
                saved = json.loads(WEIGHTS_FILE.read_text())
                for k in self.weights:
                    if k in saved:
                        self.weights[k] = max(0.5, min(1.5, float(saved[k])))
                logger.info(f"Loaded adaptive weights: {self.weights}")
            except Exception as e:
                logger.warning(f"Could not load weights: {e}")

    def save_weights(self):
        WEIGHTS_FILE.parent.mkdir(parents=True, exist_ok=True)
        WEIGHTS_FILE.write_text(json.dumps(self.weights, indent=2))
