"""Calibrator: self-improving loop that adjusts model weights based on outcomes.

Runs after markets resolve, computes Brier score and log-loss, and updates
the six-factor model's adaptive weights.
"""

from __future__ import annotations

import logging
import math
from typing import Any

from src.models.six_factor import SixFactorModel
from src.output.db import get_connection, insert_calibration

logger = logging.getLogger(__name__)

MIN_SAMPLES = 20
WEIGHT_ADJUST = 0.10  # +-10% per cycle
WEIGHT_FLOOR = 0.5
WEIGHT_CEIL = 1.5


def brier_score(predictions: list[float], outcomes: list[int]) -> float:
    """Mean squared error between predicted probabilities and outcomes."""
    if not predictions:
        return 1.0
    return sum((p - o) ** 2 for p, o in zip(predictions, outcomes)) / len(predictions)


def log_loss(predictions: list[float], outcomes: list[int]) -> float:
    """Negative log-likelihood loss."""
    if not predictions:
        return float("inf")
    eps = 1e-15
    total = 0.0
    for p, o in zip(predictions, outcomes):
        p = max(eps, min(1 - eps, p))
        total += o * math.log(p) + (1 - o) * math.log(1 - p)
    return -total / len(predictions)


def accuracy(predictions: list[float], outcomes: list[int]) -> float:
    if not predictions:
        return 0.0
    correct = sum(1 for p, o in zip(predictions, outcomes) if (p >= 0.5) == (o == 1))
    return correct / len(predictions)


class Calibrator:
    """Adjusts SixFactorModel weights based on resolved trade outcomes."""

    def __init__(self, model: SixFactorModel):
        self.model = model

    def run(self, resolved_trades: list[dict]) -> dict[str, Any]:
        """
        Each trade should have:
          - predicted_prob: float (model's predicted probability)
          - outcome: int (1 = correct, 0 = incorrect)
          - factors_used: dict (which factor contributed how much)
        """
        if len(resolved_trades) < MIN_SAMPLES:
            logger.info(
                f"Only {len(resolved_trades)} samples (need {MIN_SAMPLES}), skipping calibration"
            )
            return {"skipped": True, "samples": len(resolved_trades)}

        predictions = [t["predicted_prob"] for t in resolved_trades]
        outcomes = [t["outcome"] for t in resolved_trades]

        bs = brier_score(predictions, outcomes)
        ll = log_loss(predictions, outcomes)
        acc = accuracy(predictions, outcomes)

        logger.info(f"Calibration: Brier={bs:.4f} LogLoss={ll:.4f} Accuracy={acc:.1%} n={len(predictions)}")

        self._update_weights(resolved_trades)

        self.model.save_weights()

        try:
            conn = get_connection()
            insert_calibration(
                conn, "six_factor", "sports",
                bs, ll, acc, len(predictions), self.model.weights,
            )
            conn.close()
        except Exception as e:
            logger.warning(f"Could not save calibration to DB: {e}")

        return {
            "brier_score": bs,
            "log_loss": ll,
            "accuracy": acc,
            "sample_count": len(predictions),
            "weights": dict(self.model.weights),
        }

    def _update_weights(self, trades: list[dict]):
        """Bump weights for factors that correlated with correct predictions."""
        factor_scores: dict[str, list[float]] = {k: [] for k in self.model.weights}

        for trade in trades:
            factors = trade.get("factors_used", {})
            was_correct = trade["outcome"] == 1
            for factor_name, contribution in factors.items():
                if factor_name in factor_scores:
                    aligned = (contribution > 0 and was_correct) or (contribution <= 0 and not was_correct)
                    factor_scores[factor_name].append(1.0 if aligned else 0.0)

        for factor_name, scores in factor_scores.items():
            if len(scores) < MIN_SAMPLES:
                continue
            hit_rate = sum(scores) / len(scores)
            if hit_rate > 0.55:
                self.model.weights[factor_name] = min(
                    WEIGHT_CEIL,
                    self.model.weights[factor_name] * (1 + WEIGHT_ADJUST),
                )
            elif hit_rate < 0.45:
                self.model.weights[factor_name] = max(
                    WEIGHT_FLOOR,
                    self.model.weights[factor_name] * (1 - WEIGHT_ADJUST),
                )

        logger.info(f"Updated weights: {self.model.weights}")
