"""Base strategy interface for all arbitrage strategies."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Action(str, Enum):
    BUY_ALL_YES = "BUY_ALL_YES"
    BUY_ALL_NO = "BUY_ALL_NO"
    BUY_YES = "BUY_YES"
    BUY_NO = "BUY_NO"
    CROSS_ARB = "CROSS_ARB"
    SKIP = "SKIP"


@dataclass
class Opportunity:
    strategy: str
    event_title: str
    action: Action
    edge_pct: float
    details: dict[str, Any] = field(default_factory=dict)
    market_ids: list[str] = field(default_factory=list)
    settlement_date: str = ""
    estimated_profit_usd: float = 0.0
    confidence: float = 1.0

    @property
    def is_actionable(self) -> bool:
        return self.action != Action.SKIP and self.edge_pct > 0


class BaseStrategy(ABC):
    """All strategies implement scan() and calculate_edge()."""

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    async def scan(self) -> list[Opportunity]:
        """Scan for opportunities. Returns list of detected opportunities."""
        ...

    @abstractmethod
    def calculate_edge(self, **kwargs) -> float:
        """Compute the edge percentage for a given opportunity."""
        ...
