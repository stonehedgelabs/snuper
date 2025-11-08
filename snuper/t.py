import datetime as dt
import json
import logging
import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

import rapidfuzz

from snuper.constants import GAME_RUNTIME_SECONDS, MGM_NFL_TEAMS, MGM_MLB_TEAMS, MGM_NBA_TEAMS


class Event:
    """In-memory representation of a scheduled or live sporting event."""

    def __init__(  # pylint: disable=too-many-positional-arguments
        self,
        event_id: str,
        league: str,
        url: str,
        start_time: dt.datetime,
        away: tuple[str, str],
        home: tuple[str, str],
        selections: list[Any] | None,
        sportdata_game: dict | None = None,
        rollinginsight_game: dict | None = None,
    ) -> None:
        """Store identifying metadata and cached selections for an event."""

        self.event_id = event_id
        self.league = league.lower()
        self.url = url
        self.start_time = start_time
        self.away = away
        self.home = home
        self.selections = selections
        self.sportdata_game = sportdata_game
        self.rollinginsight_game = rollinginsight_game
        self.log = logging.getLogger(self.__class__.__name__)

    def get_key(self) -> str:
        """Return the canonical league:event_id key used by monitors."""

        return f"{self.league}:{self.event_id}"

    def has_started(self) -> bool:
        """Return ``True`` once the event's scheduled start time has passed."""

        return self.start_time <= dt.datetime.now(dt.timezone.utc)

    def game_label(self) -> str:

        # League-to-team mapping
        league_team_sets = {
            "mlb": MGM_MLB_TEAMS,
            "nfl": MGM_NFL_TEAMS,
            "nba": MGM_NBA_TEAMS,
        }

        teams = league_team_sets.get(self.league.lower())
        if not teams:
            self.log.warning("Unknown league for event: %s", self.league)
            return f"{self.away} @ {self.home}"

        def resolve_team_name(tokens: tuple[str, str]) -> str:
            """Resolve the most likely canonical team name using fuzzy matching."""
            slug = "-".join(tokens).lower()

            # Find the best fuzzy match within this league’s team set
            match, _score, _ = rapidfuzz.process.extractOne(slug, teams, score_cutoff=70)

            # Fallback if no close match is found
            canonical = match if match else slug

            # Convert canonical slug to readable title form
            return " ".join(word.capitalize() for word in canonical.split("-"))

        away_name = resolve_team_name(self.away)
        home_name = resolve_team_name(self.home)
        return f"{away_name} @ {home_name}"

    def is_finished(self) -> bool:
        """Return ``True`` when the event has run longer than the max runtime."""

        delta = dt.datetime.now(dt.timezone.utc) - self.start_time
        return delta.total_seconds() > GAME_RUNTIME_SECONDS

    def to_dict(self) -> dict[str, Any]:
        """Serialize the event for snapshot persistence."""

        return {
            "event_id": self.event_id,
            "sportdata_game": self.sportdata_game,
            "rollinginsight_game": self.rollinginsight_game,
            "league": self.league,
            "event_url": self.url,
            "start_time": self.start_time.isoformat(),
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "selections": self.selections,
            "away": self.away,
            "home": self.home,
        }

    def set_rollinginsight_game(self, game: dict[str, Any]) -> None:
        """Set the rollinginsight_game data for this event."""

        self.rollinginsight_game = game

    def __repr__(self) -> str:
        """Return a concise display string useful in logs."""

        return f"<Event[{self.league}, {self.event_id}, {self.away[0]}@{self.home[0]}]>"


class Selection:
    """Wrap a selection payload with its parent event id."""

    def __init__(self, event_id: str, data: dict[str, Any]) -> None:
        """Store the event id and selection payload for later serialisation."""

        self.event_id = event_id
        self.data = data

    def to_dict(self) -> dict[str, Any]:
        """Return a dictionary ready to be appended to the odds log."""

        payload = {"event_id": self.event_id}
        payload.update(self.data)
        return payload


class SelectionChange:
    """Represent a timestamped selection change in JSONL logs."""

    def __init__(self, label: str, selection: Selection) -> None:
        """Capture the selection and record the observation time."""

        self.created_at = dt.datetime.now(dt.timezone.utc).isoformat()
        self.label = label
        self.selection = selection

    def to_json(self) -> str:
        """Serialise the selection change into a JSON string."""

        return json.dumps(
            {
                "created_at": self.created_at,
                "label": self.label,
                "data": self.selection.to_dict(),
            }
        )


class Team:
    def __init__(  # pylint: disable=too-many-positional-arguments
        self,
        name: str,
        league: str,
        league_color: str,
        city: str | None = None,
        subreddit: str | None = None,
        abbreviation: str | None = None,
        sport_radar_io_team_id: int | None = None,
    ):
        self.name = name
        self.league = league.lower()
        self.league_color = league_color
        self.city = city
        self.subreddit = subreddit
        self.abbreviation = abbreviation
        self.sport_radar_io_team_id = sport_radar_io_team_id
        self.id = self.generate_team_id()

    def generate_team_id(self) -> str:
        league_lower = self.league.lower()
        city_short = re.sub(r"\s+", "", (self.city or "").lower())
        team_short = re.sub(r"\s+", "-", self.name.lower())
        return f"{league_lower}--{city_short}-{team_short}"

    def __repr__(self):
        return f"<Team {self.name} ({self.abbreviation or ''})>"


@dataclass(slots=True)
class SportdataGame:
    """Represent a game from Sportdata API."""

    game_id: int
    global_game_id: int
    score_id: int
    game_key: str
    season: int
    season_type: int
    status: str
    canceled: bool
    date: str
    day: str
    date_time: str
    date_time_utc: str
    away_team: str
    home_team: str
    global_away_team_id: int
    global_home_team_id: int
    away_team_id: int
    home_team_id: int
    stadium_id: int
    closed: bool | None = None
    last_updated: str | None = None
    is_closed: bool | None = None
    week: int | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "SportdataGame":
        week_value = data.get("Week")
        return cls(
            game_id=int(data["GameID"]),
            global_game_id=int(data["GlobalGameID"]),
            score_id=int(data["ScoreID"]),
            game_key=str(data["GameKey"]),
            season=int(data["Season"]),
            season_type=int(data["SeasonType"]),
            status=str(data["Status"]),
            canceled=bool(data["Canceled"]),
            date=str(data["Date"]),
            day=str(data["Day"]),
            date_time=str(data["DateTime"]),
            date_time_utc=str(data["DateTimeUTC"]),
            away_team=str(data["AwayTeam"]),
            home_team=str(data["HomeTeam"]),
            global_away_team_id=int(data["GlobalAwayTeamID"]),
            global_home_team_id=int(data["GlobalHomeTeamID"]),
            away_team_id=int(data["AwayTeamID"]),
            home_team_id=int(data["HomeTeamID"]),
            stadium_id=int(data["StadiumID"]),
            closed=data.get("Closed"),
            last_updated=data.get("LastUpdated"),
            is_closed=data.get("IsClosed"),
            week=int(week_value) if week_value is not None else None,
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "GameID": self.game_id,
            "GlobalGameID": self.global_game_id,
            "ScoreID": self.score_id,
            "GameKey": self.game_key,
            "Season": self.season,
            "SeasonType": self.season_type,
            "Status": self.status,
            "Canceled": self.canceled,
            "Date": self.date,
            "Day": self.day,
            "DateTime": self.date_time,
            "DateTimeUTC": self.date_time_utc,
            "AwayTeam": self.away_team,
            "HomeTeam": self.home_team,
            "GlobalAwayTeamID": self.global_away_team_id,
            "GlobalHomeTeamID": self.global_home_team_id,
            "AwayTeamID": self.away_team_id,
            "HomeTeamID": self.home_team_id,
            "StadiumID": self.stadium_id,
            "Closed": self.closed,
            "LastUpdated": self.last_updated,
            "IsClosed": self.is_closed,
            "Week": self.week,
        }


@dataclass(slots=True)
class RollingInsightsGame:
    """Represent a game from Rolling Insights API."""

    away_team: str
    home_team: str
    away_team_id: int
    home_team_id: int
    game_id: str
    game_time: str
    season_type: str
    season: str
    status: str
    event_name: str | None = None
    round: str | None = None
    broadcast: str | None = None

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> "RollingInsightsGame":
        """Create a RollingInsightsGame from an API response dictionary."""
        return cls(
            away_team=str(data["away_team"]),
            home_team=str(data["home_team"]),
            away_team_id=int(data["away_team_id"]),
            home_team_id=int(data["home_team_id"]),
            game_id=str(data["game_ID"]),
            game_time=str(data["game_time"]),
            season_type=str(data["season_type"]),
            season=str(data["season"]),
            status=str(data["status"]),
            event_name=data.get("event_name"),
            round=data.get("round"),
            broadcast=data.get("broadcast"),
        )


class RollingInsightsScheduleResponse:
    """Represent the response from Rolling Insights schedule API."""

    def __init__(self, data: dict[str, list[dict[str, Any]]]) -> None:
        """Store the schedule response data."""

        self.data = data

    def get_games_for_league(self, league: str) -> list[dict[str, Any]]:
        """Return the list of games for a specific league."""

        return self.data.get(league.upper(), [])


class SportdataScheduleResponse:
    """Represent the response from Sportdata schedule API."""

    def __init__(self, games: list[dict[str, Any]]) -> None:
        """Store the schedule response data."""

        self.games = games
