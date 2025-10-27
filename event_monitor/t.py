import datetime as dt
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from event_monitor.constants import GAME_RUNTIME_SECONDS


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
        self.league = league
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
