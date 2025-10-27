from __future__ import annotations

import abc
import datetime as dt
import json
import logging
from pathlib import Path
from typing import Iterable, Sequence

from event_monitor.t import Event

__all__ = ["BaseEventScraper", "event_filepath", "odds_filepath"]

_TIMESTAMP_FORMAT = "%Y%m%d"


def _current_stamp(now: dt.datetime | None = None) -> str:
    current = now or dt.datetime.now()
    return current.strftime(_TIMESTAMP_FORMAT)


def event_filepath(output_dir: Path, league: str, *, timestamp: str | None = None) -> Path:
    ts = timestamp or _current_stamp()
    return Path(output_dir) / "events" / f"{league}-{ts}.json"


def odds_filepath(
    output_dir: Path,
    league: str,
    event_id: str,
    *,
    timestamp: str | None = None,
) -> Path:
    ts = timestamp or _current_stamp()
    return Path(output_dir) / "odds" / f"{league}-{ts}-{event_id}.json"


class BaseEventScraper(abc.ABC):
    """Shared behavior for sportsbook event scrapers."""

    def __init__(self, leagues: Sequence[str]) -> None:
        if not leagues:
            raise ValueError("leagues must not be empty")
        self.leagues = [league.lower() for league in leagues]
        self.log = logging.getLogger(self.__class__.__name__)

    @abc.abstractmethod
    async def scrape_today(self, league: str) -> list[Event]:
        """Return events scheduled for the current day in the target league."""

    def save(self, events: Iterable[Event], league: str, output_dir: Path) -> Path | None:
        """Persist scraped events without overwriting existing artifacts."""

        output_dir = Path(output_dir)
        data = list(events)
        if not data:
            self.log.warning("No games to save for %s today.", league)
            return None

        path = event_filepath(output_dir, league)
        if path.exists():
            self.log.warning("File %s already exists. Skipping.", path)
            return None

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump([event.to_dict() for event in data], fh, indent=2)
        self.log.info("Saved %d events to %s", len(data), path)
        return path

    async def scrape_and_save_all(self, output_dir: Path) -> list[Path]:
        """Scrape every configured league and save results, skipping pre-existing files."""

        output_dir = Path(output_dir)
        paths: list[Path] = []
        for league in self.leagues:
            path = event_filepath(output_dir, league)
            if path.exists():
                self.log.warning("File %s already exists. Skipping.", path)
                continue

            try:
                events = await self.scrape_today(league)
            except Exception as exc:  # pragma: no cover - safety net for CLI usage
                self.log.error("Failed to scrape %s: %s", league, exc)
                continue

            saved = self.save(events, league, output_dir)
            if saved:
                paths.append(saved)
        return paths
