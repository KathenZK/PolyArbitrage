"""Resolution rule checker for cross-platform arbitrage safety.

Flags events where Polymarket and Kalshi might resolve differently.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass

logger = logging.getLogger(__name__)

SAFE_EVENT_PATTERNS = [
    r"fed.*rate.*cut",
    r"fed.*rate.*hike",
    r"fed.*funds.*rate",
    r"bitcoin.*\$\d+",
    r"btc.*\$\d+",
    r"ethereum.*\$\d+",
    r"gdp.*q\d",
    r"cpi.*\d+",
    r"nonfarm.*payroll",
    r"unemployment.*rate",
    r"s&p.*\d+",
    r"nba.*champion",
    r"super.*bowl.*winner",
    r"ncaa.*champion",
]

RISKY_EVENT_PATTERNS = [
    r"perform",
    r"attend",
    r"announce",
    r"reveal",
    r"confirm",
    r"acknowledge",
    r"visit",
    r"meet",
    r"say",
    r"tweet",
    r"post",
    r"comment",
]


@dataclass
class ResolutionCheck:
    poly_question: str
    kalshi_title: str
    is_safe: bool
    risk_level: str  # "low", "medium", "high"
    warnings: list[str]


class ResolutionChecker:
    """Evaluates resolution-rule risk for cross-platform arbitrage."""

    def check(self, poly_question: str, kalshi_title: str) -> ResolutionCheck:
        warnings: list[str] = []
        poly_lower = poly_question.lower()
        kalshi_lower = kalshi_title.lower()

        is_safe_pattern = any(
            re.search(p, poly_lower) or re.search(p, kalshi_lower)
            for p in SAFE_EVENT_PATTERNS
        )

        is_risky_pattern = any(
            re.search(p, poly_lower) or re.search(p, kalshi_lower)
            for p in RISKY_EVENT_PATTERNS
        )

        if is_risky_pattern:
            warnings.append("Event contains subjective/ambiguous terms (perform, announce, etc.)")

        poly_nums = set(re.findall(r"\d+(?:\.\d+)?", poly_question))
        kalshi_nums = set(re.findall(r"\d+(?:\.\d+)?", kalshi_title))
        if poly_nums and kalshi_nums and poly_nums != kalshi_nums:
            warnings.append(
                f"Numerical thresholds differ: PM={poly_nums} vs Kalshi={kalshi_nums}"
            )

        poly_words = set(poly_lower.split())
        kalshi_words = set(kalshi_lower.split())
        overlap = poly_words & kalshi_words
        total = poly_words | kalshi_words
        word_overlap = len(overlap) / len(total) if total else 0

        if word_overlap < 0.3:
            warnings.append(f"Low textual overlap ({word_overlap:.0%}), events may differ")

        if is_safe_pattern and not is_risky_pattern and not warnings:
            risk_level = "low"
        elif is_risky_pattern or len(warnings) >= 2:
            risk_level = "high"
        else:
            risk_level = "medium"

        return ResolutionCheck(
            poly_question=poly_question,
            kalshi_title=kalshi_title,
            is_safe=risk_level == "low",
            risk_level=risk_level,
            warnings=warnings,
        )
