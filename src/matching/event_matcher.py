"""Cross-platform event matcher: finds the same event on Polymarket and Kalshi.

Uses a multi-stage pipeline:
1. Text similarity via TF-IDF + cosine (lightweight, no GPU needed)
2. Date proximity check
3. Numerical value matching (thresholds like "$100k", "25bps")
4. Manual override map for known pairs
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime

logger = logging.getLogger(__name__)

MANUAL_OVERRIDES: dict[str, str] = {
    # polymarket_slug : kalshi_event_ticker
    # Add known pairs here as they're discovered
}


@dataclass
class MatchedPair:
    poly_id: str
    poly_question: str
    poly_end_date: str
    kalshi_ticker: str
    kalshi_title: str
    kalshi_close_time: str
    confidence: float
    match_method: str


class EventMatcher:
    """Matches events across Polymarket and Kalshi using text similarity."""

    def __init__(self, min_confidence: float = 0.65):
        self._min_confidence = min_confidence
        self._overrides = dict(MANUAL_OVERRIDES)

    def match(
        self,
        poly_markets: list[dict],
        kalshi_markets: list[dict],
    ) -> list[MatchedPair]:
        """Find matching event pairs. Returns list of MatchedPair."""
        if not poly_markets or not kalshi_markets:
            return []

        matches: list[MatchedPair] = []

        poly_texts = [self._normalize(m.get("question", "")) for m in poly_markets]
        kalshi_texts = [self._normalize(m.get("title", "")) for m in kalshi_markets]

        similarity_matrix = self._compute_similarities(poly_texts, kalshi_texts)

        used_kalshi: set[int] = set()

        for i, pm in enumerate(poly_markets):
            best_j = -1
            best_score = 0.0

            for j, km in enumerate(kalshi_markets):
                if j in used_kalshi:
                    continue

                score = similarity_matrix[i][j]

                date_bonus = self._date_proximity_bonus(
                    pm.get("endDate", ""), km.get("close_time", "")
                )
                score += date_bonus

                num_bonus = self._numerical_match_bonus(
                    pm.get("question", ""), km.get("title", "")
                )
                score += num_bonus

                if score > best_score:
                    best_score = score
                    best_j = j

            override_ticker = self._overrides.get(pm.get("slug", ""))
            if override_ticker:
                for j, km in enumerate(kalshi_markets):
                    if km.get("ticker", "") == override_ticker:
                        best_j = j
                        best_score = 1.0
                        break

            if best_j >= 0 and best_score >= self._min_confidence:
                km = kalshi_markets[best_j]
                used_kalshi.add(best_j)
                matches.append(MatchedPair(
                    poly_id=pm.get("id", ""),
                    poly_question=pm.get("question", ""),
                    poly_end_date=pm.get("endDate", ""),
                    kalshi_ticker=km.get("ticker", ""),
                    kalshi_title=km.get("title", ""),
                    kalshi_close_time=km.get("close_time", ""),
                    confidence=round(min(best_score, 1.0), 3),
                    match_method="override" if best_score == 1.0 else "similarity",
                ))

        return matches

    @staticmethod
    def _normalize(text: str) -> str:
        text = text.lower().strip()
        text = re.sub(r"[^\w\s$%.]", " ", text)
        text = re.sub(r"\s+", " ", text)
        return text

    @staticmethod
    def _compute_similarities(texts_a: list[str], texts_b: list[str]) -> list[list[float]]:
        """Compute cosine similarity matrix using TF-IDF."""
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity

            all_texts = texts_a + texts_b
            if not all_texts:
                return []

            vectorizer = TfidfVectorizer(
                ngram_range=(1, 2),
                max_features=5000,
                stop_words="english",
            )
            tfidf = vectorizer.fit_transform(all_texts)

            a_matrix = tfidf[: len(texts_a)]
            b_matrix = tfidf[len(texts_a):]

            sim = cosine_similarity(a_matrix, b_matrix)
            return sim.tolist()

        except ImportError:
            logger.warning("scikit-learn not installed; falling back to Jaccard similarity")
            return EventMatcher._jaccard_matrix(texts_a, texts_b)

    @staticmethod
    def _jaccard_matrix(texts_a: list[str], texts_b: list[str]) -> list[list[float]]:
        def jaccard(a: str, b: str) -> float:
            set_a = set(a.split())
            set_b = set(b.split())
            if not set_a or not set_b:
                return 0.0
            return len(set_a & set_b) / len(set_a | set_b)

        return [[jaccard(a, b) for b in texts_b] for a in texts_a]

    @staticmethod
    def _date_proximity_bonus(date_a: str, date_b: str) -> float:
        try:
            dt_a = datetime.fromisoformat(date_a.replace("Z", "+00:00"))
            dt_b = datetime.fromisoformat(date_b.replace("Z", "+00:00"))
            diff_days = abs((dt_a - dt_b).days)
            if diff_days <= 1:
                return 0.15
            elif diff_days <= 7:
                return 0.08
            elif diff_days <= 30:
                return 0.03
        except (ValueError, TypeError):
            pass
        return 0.0

    @staticmethod
    def _numerical_match_bonus(text_a: str, text_b: str) -> float:
        nums_a = set(re.findall(r"\d+(?:\.\d+)?", text_a))
        nums_b = set(re.findall(r"\d+(?:\.\d+)?", text_b))
        if nums_a and nums_b and nums_a & nums_b:
            return 0.10
        return 0.0

    def add_override(self, poly_slug: str, kalshi_ticker: str):
        self._overrides[poly_slug] = kalshi_ticker
