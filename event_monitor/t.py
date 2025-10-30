import datetime as dt
import json
import re
import logging
from typing import Any, Optional

import rapidfuzz

from event_monitor.constants import GAME_RUNTIME_SECONDS, MGM_NFL_TEAMS, MGM_MLB_TEAMS, MGM_NBA_TEAMS


class Event:
    """In-memory representation of a scheduled or live sporting event."""

    def __init__(
        self,
        event_id: str,
        league: str,
        url: str,
        start_time: dt.datetime,
        away: tuple[str, str],
        home: tuple[str, str],
        selections: Optional[list],
    ) -> None:
        """Store identifying metadata and cached selections for an event."""

        self.event_id = event_id
        self.league = league.lower()
        self.url = url
        self.start_time = start_time
        self.away = away
        self.home = home
        self.selections = selections
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
            match, score, _ = rapidfuzz.process.extractOne(slug, teams, score_cutoff=70)

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
            "league": self.league,
            "event_url": self.url,
            "start_time": self.start_time.isoformat(),
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "selections": self.selections,
            "away": self.away,
            "home": self.home,
        }

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
    def __init__(
        self,
        name: str,
        league: str,
        league_color: str,
        city: str = None,
        subreddit: str = None,
        abbreviation: str = None,
        sport_radar_io_team_id: int = None,
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
