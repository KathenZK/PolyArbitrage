"""ESPN Stats API client for NBA/NCAA schedule, scores, injuries, and team data."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import aiohttp

logger = logging.getLogger(__name__)

ESPN_BASE = "https://site.api.espn.com/apis/site/v2/sports/basketball"


@dataclass
class Team:
    id: str
    name: str
    abbreviation: str
    display_name: str
    record: str = ""
    wins: int = 0
    losses: int = 0


@dataclass
class InjuryReport:
    player_name: str
    status: str  # OUT, DOUBTFUL, QUESTIONABLE, DAY_TO_DAY, PROBABLE
    detail: str = ""


@dataclass
class GameInfo:
    game_id: str
    home_team: Team
    away_team: Team
    home_score: int | None = None
    away_score: int | None = None
    status: str = "pre"  # pre, in, post
    start_time: str = ""
    venue: str = ""
    home_injuries: list[InjuryReport] = field(default_factory=list)
    away_injuries: list[InjuryReport] = field(default_factory=list)


class ESPNClient:
    """Async client for ESPN's unofficial REST API (no auth required)."""

    def __init__(self):
        self._session: aiohttp.ClientSession | None = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def _get(self, url: str, params: dict | None = None) -> Any:
        session = await self._ensure_session()
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as r:
            r.raise_for_status()
            return await r.json()

    async def get_scoreboard(self, sport: str = "nba", date: str = "") -> list[GameInfo]:
        """Get today's games (or a specific date YYYYMMDD)."""
        league = "mens-college-basketball" if sport == "ncaa" else sport
        url = f"{ESPN_BASE}/{league}/scoreboard"
        params = {}
        if date:
            params["dates"] = date

        data = await self._get(url, params)
        games: list[GameInfo] = []

        for event in data.get("events", []):
            try:
                competition = event["competitions"][0]
                competitors = competition.get("competitors", [])
                if len(competitors) < 2:
                    continue

                home_raw = next((c for c in competitors if c.get("homeAway") == "home"), competitors[0])
                away_raw = next((c for c in competitors if c.get("homeAway") == "away"), competitors[1])

                home_team = self._parse_team(home_raw.get("team", {}))
                away_team = self._parse_team(away_raw.get("team", {}))

                home_record = home_raw.get("records", [{}])
                if home_record:
                    rec = home_record[0].get("summary", "0-0")
                    home_team.record = rec
                    parts = rec.split("-")
                    if len(parts) == 2:
                        home_team.wins = int(parts[0])
                        home_team.losses = int(parts[1])

                away_record = away_raw.get("records", [{}])
                if away_record:
                    rec = away_record[0].get("summary", "0-0")
                    away_team.record = rec
                    parts = rec.split("-")
                    if len(parts) == 2:
                        away_team.wins = int(parts[0])
                        away_team.losses = int(parts[1])

                status_raw = event.get("status", {}).get("type", {})
                status = status_raw.get("state", "pre")

                home_score = None
                away_score = None
                if status != "pre":
                    home_score = int(home_raw.get("score", 0) or 0)
                    away_score = int(away_raw.get("score", 0) or 0)

                games.append(GameInfo(
                    game_id=event.get("id", ""),
                    home_team=home_team,
                    away_team=away_team,
                    home_score=home_score,
                    away_score=away_score,
                    status=status,
                    start_time=event.get("date", ""),
                    venue=competition.get("venue", {}).get("fullName", ""),
                ))
            except (KeyError, IndexError, ValueError) as e:
                logger.debug(f"Error parsing ESPN event: {e}")
                continue

        return games

    async def get_team_schedule(self, sport: str, team_id: str) -> list[dict]:
        league = "mens-college-basketball" if sport == "ncaa" else sport
        url = f"{ESPN_BASE}/{league}/teams/{team_id}/schedule"
        data = await self._get(url)
        return data.get("events", [])

    async def get_standings(self, sport: str = "nba") -> list[dict]:
        league = "mens-college-basketball" if sport == "ncaa" else sport
        url = f"{ESPN_BASE}/{league}/standings"
        data = await self._get(url)
        entries = []
        for group in data.get("children", []):
            for standing in group.get("standings", {}).get("entries", []):
                entries.append(standing)
        return entries

    @staticmethod
    def _parse_team(raw: dict) -> Team:
        return Team(
            id=raw.get("id", ""),
            name=raw.get("name", ""),
            abbreviation=raw.get("abbreviation", ""),
            display_name=raw.get("displayName", raw.get("name", "")),
        )
