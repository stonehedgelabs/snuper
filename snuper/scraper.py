from __future__ import annotations

import abc
import datetime as dt
import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tzlocal import get_localzone

from snuper.constants import SPORTS
from snuper.utils import current_stamp, event_filepath, match_sportdata_game, match_rollinginsight_game
from snuper.t import Event
from snuper.sinks import SelectionSink

__all__ = ["BaseEventScraper", "ScrapeContext"]


@dataclass(slots=True)
class ScrapeContext:
    """Bundle run-level metadata shared across scraper hooks."""

    league: str
    sport: str
    output_dir: Path
    input_dir: Path
    stamp: str = field(default_factory=current_stamp)
    run_at: dt.datetime = field(default_factory=lambda: dt.datetime.now(dt.timezone.utc))
    cache: dict[str, Any] = field(default_factory=dict)
    overwrite: bool = False
    merge_sportdata_games: bool = False
    merge_rollinginsights_games: bool = False


class BaseEventScraper(abc.ABC):
    """Base class that provides league bookkeeping and persistence helpers."""

    def __init__(
        self,
        leagues: Sequence[str],
        *,
        output_dir: Path | None = None,
        input_dir: Path | None = None,
    ) -> None:
        """Record the set of supported leagues for the scraper instance."""

        if not leagues:
            raise ValueError("leagues must not be empty")
        self.leagues = [league.lower() for league in leagues]
        self.log = logging.getLogger(self.__class__.__name__)
        self.output_dir = Path(output_dir) if output_dir is not None else None
        self.local_tz = get_localzone()
        self.input_dir = Path(input_dir) if input_dir is not None else self.output_dir

    @abc.abstractmethod
    async def scrape_today(
        self,
        context: ScrapeContext,
        source_events: Sequence[Event] | None = None,
    ) -> list[Event]:
        """Return the list of events scheduled today for ``league``."""

    async def load_source_events(self, context: ScrapeContext) -> Sequence[Event] | None:
        """Return any prerequisite events required for ``context.league``."""

        _ = context
        return None

    def save(
        self,
        events: Iterable[Event],
        league: str,
        output_dir: Path | str | None = None,
        *,
        timestamp: str | None = None,
        overwrite: bool = False,
    ) -> Path | None:
        """Persist scraped events to disk unless a prior snapshot exists."""

        destination = Path(output_dir) if output_dir is not None else self.output_dir
        if destination is None:
            raise ValueError("output_dir is not configured for this scraper")
        data = list(events)
        path = event_filepath(destination, league, timestamp=timestamp)
        if path.exists() and not overwrite:
            self.log.warning("%s - file %s already exists. skipping.", self.__class__.__name__, path)
            return None
        if path.exists() and overwrite:
            self.log.info("%s - overwriting existing snapshot %s", self.__class__.__name__, path)

        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", encoding="utf-8") as fh:
            json.dump([event.to_dict() for event in data], fh, indent=2)
        self.log.info(
            "%s - saved %d events to %s",
            self.__class__.__name__,
            len(data),
            path,
        )
        return path

    async def scrape_and_save_all(
        self,
        output_dir: Path | str | None = None,
        *,
        leagues: Sequence[str] | None = None,
        overwrite: bool = False,
        sink: SelectionSink | None = None,
        provider: str | None = None,
        merge_sportdata_games: bool = False,
        merge_rollinginsights_games: bool = False,
    ) -> list[Path]:
        """Scrape each configured league and persist the resulting snapshots."""

        destination = Path(output_dir) if output_dir is not None else self.output_dir
        if destination is None:
            raise ValueError("output_dir is not configured for this scraper")
        source_root = self.input_dir if self.input_dir is not None else destination
        requested = [league.lower() for league in leagues or self.leagues]
        seen: set[str] = set()
        paths: list[Path] = []
        for league in requested:
            if league not in self.leagues:
                self.log.warning("%s - unsupported league %s requested. skipping.", self.__class__.__name__, league)
                continue
            if league in seen:
                continue
            seen.add(league)
            sport = SPORTS[league]
            context = ScrapeContext(
                league,
                sport,
                destination,
                source_root,
                overwrite=overwrite,
                merge_sportdata_games=merge_sportdata_games,
                merge_rollinginsights_games=merge_rollinginsights_games,
            )
            path = event_filepath(destination, league, timestamp=context.stamp)
            if path.exists() and not overwrite:
                self.log.warning("%s - file %s already exists. skipping.", self.__class__.__name__, path)
                continue

            try:
                source_events = await self.load_source_events(context)
                events = await self.scrape_today(context, source_events)
            except Exception as exc:  # pragma: no cover - safety net for CLI usage
                self.log.error("%s - failed to scrape %s: %s", self.__class__.__name__, league, exc)
                continue

            if merge_sportdata_games:
                self.log.info(
                    "%s - matching Sportdata games for %d %s events", self.__class__.__name__, len(events), league
                )
                matched_count = 0
                for event in events:
                    try:
                        await match_sportdata_game(event)
                        matched_count += 1
                    except Exception as exc:
                        self.log.warning(
                            "%s - failed to match Sportdata %s game for event %s: %s",
                            self.__class__.__name__,
                            league,
                            event.event_id,
                            exc,
                        )
                self.log.info(
                    "%s - successfully matched %d/%d %s events to Sportdata games",
                    self.__class__.__name__,
                    matched_count,
                    len(events),
                    league,
                )
            if merge_rollinginsights_games:
                self.log.info(
                    "%s - matching Rolling Insights games for %d %s events in %s",
                    self.__class__.__name__,
                    len(events),
                    league,
                    league,
                )
                matched_count = 0
                for event in events:
                    try:
                        await match_rollinginsight_game(event)
                        matched_count += 1
                    except Exception as exc:
                        self.log.warning(
                            "%s - failed to match Rolling Insights game for %s event %s: %s",
                            self.__class__.__name__,
                            league,
                            event.event_id,
                            exc,
                        )
                self.log.info(
                    "%s - successfully matched %d/%d %s events to Rolling Insights games",
                    self.__class__.__name__,
                    matched_count,
                    len(events),
                    league,
                )

            saved: Path | None = None
            if sink and provider:
                event_count = len(events)
                self.log.info(
                    "%s - %s preparing to persist %d events for %s/%s",
                    self.__class__.__name__,
                    sink.__class__.__name__,
                    event_count,
                    provider,
                    league,
                )
                saved = await sink.save_snapshot(
                    provider=provider,
                    league=league,
                    events=events,
                    timestamp=context.stamp,
                    output_dir=destination,
                    overwrite=overwrite,
                )
            else:
                saved = self.save(
                    events,
                    league,
                    destination,
                    timestamp=context.stamp,
                    overwrite=overwrite,
                )
            if saved:
                paths.append(saved)
        return paths
