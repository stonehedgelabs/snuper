import datetime as dt
import logging
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

game_runtime = 5 * 3600  # 5 hours

class Event:
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
        self.event_id = event_id
        self.league = league
        self.url = url
        self.start_time = start_time
        self.away = away
        self.home = home
        self.selections = selections
        self.log = logging.getLogger(self.__class__.__name__)

    def get_key(self) -> str:
        return f"{self.league}:{self.event_id}"

    def has_started(self) -> bool:
        return self.start_time <= dt.datetime.now(dt.timezone.utc)

    def is_finished(self) -> bool:
        delta = dt.datetime.now(dt.timezone.utc) - self.start_time
        return delta.total_seconds() > game_runtime

    def to_dict(self) -> dict[str, Any]:
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
        return f"<Event[{self.league}, {self.event_id}, {self.away[0]}@{self.home[0]}]>"

