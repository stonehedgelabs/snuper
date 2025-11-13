from __future__ import annotations

import abc
import asyncio
import contextlib
import datetime as dt
import logging
import time
from pathlib import Path
from collections.abc import Sequence
from typing import Any

from tzlocal import get_localzone

from snuper.t import Event
from snuper.utils import configure_colored_logger
from snuper.sinks import SelectionSink

__all__ = ["BaseRunner", "BaseMonitor"]


class BaseRunner(abc.ABC):
    """Interface that streams odds updates for an individual event."""

    def __init__(
        self,
        *,
        heartbeat_interval: float | None = None,
        log_color: str | None = None,
    ) -> None:
        """Initialise logging and optional heartbeat cadence."""

        if log_color:
            self.log = configure_colored_logger(self.__class__.__name__, log_color)
        else:
            self.log = logging.getLogger(self.__class__.__name__)
        self.heartbeat_interval = heartbeat_interval
        self.local_tz = get_localzone()
        self._last_heartbeat = time.time()

    @abc.abstractmethod
    async def run(
        self,
        event: Event,
        output_dir: Path,
        league: str,
        provider: str,
        sink: SelectionSink,
    ) -> None:
        """Stream odds updates for ``event`` until completion."""

    def reset_heartbeat(self) -> None:
        """Mark the current moment as the latest heartbeat emission."""

        self._last_heartbeat = time.time()

    def maybe_emit_heartbeat(
        self,
        event: Event,
        *,
        stats: dict[str, Any] | None = None,
        force: bool = False,
    ) -> None:
        """Emit a heartbeat log if forced or the interval has elapsed."""

        if force or (self.heartbeat_interval and time.time() - self._last_heartbeat >= self.heartbeat_interval):
            self.emit_heartbeat(event, stats=stats or {})
            self._last_heartbeat = time.time()

    @abc.abstractmethod
    def emit_heartbeat(self, event: Event, *, stats: dict[str, Any]) -> None:
        """Log runner progress using subclass-specific formatting."""


class BaseMonitor:
    """Coordinator that spawns runners for every in-progress event."""

    def __init__(
        self,
        input_dir: Path,
        runner: BaseRunner,
        *,
        output_dir: Path | None = None,
        concurrency: int = 0,
        log_color: str | None = None,
        leagues: Sequence[str] | None = None,
        provider: str,
        sink: SelectionSink,
    ) -> None:
        """Store directory paths, runner instance, sink, and concurrency policy."""

        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) if output_dir else self.input_dir
        self.runner = runner
        self.concurrency = concurrency
        if log_color:
            self.log = configure_colored_logger(self.__class__.__name__, log_color)
        else:
            self.log = logging.getLogger(self.__class__.__name__)
        self.active_tasks: dict[str, asyncio.Task[None]] = {}
        self._semaphore = asyncio.Semaphore(concurrency) if concurrency and concurrency > 0 else None
        self._league_filter = {league.lower() for league in leagues} if leagues else None
        self.provider = provider
        self.sink = sink
        self.local_tz = get_localzone()

    def event_key(self, event: Event) -> str:
        """Build a unique key used to track active runner tasks."""

        return event.get_key()

    def should_monitor(self, event: Event) -> bool:
        """Return ``True`` if the event has started and is still live."""

        start_time = event.start_time
        if start_time.tzinfo is None:
            start_time = start_time.replace(tzinfo=dt.timezone.utc)
            event.start_time = start_time
        local_start_time = event.start_time.astimezone(self.local_tz)
        now_local = dt.datetime.now(self.local_tz)

        if now_local < local_start_time:
            self.log.info(
                "%s - not starting monitor because %s start time %s (local timezone of start_time) has not started yet",
                self.__class__.__name__,
                event,
                local_start_time.isoformat(),
            )
            return False
        return not event.is_finished()

    async def _prune_tasks(self, active_ids: dict[str, set[str]]) -> None:
        """Stop runners whose associated events no longer appear live."""

        for key, task in list(self.active_tasks.items()):
            league, event_id = key.split(":", 1)

            if task.done():
                exc = task.exception()
                if exc:
                    self.log.error("%s - runner for %s raised %s", self.__class__.__name__, key, exc, exc_info=exc)
                self.active_tasks.pop(key)
                continue

            live_ids = active_ids.get(league)
            if live_ids is not None and event_id in live_ids:
                continue

            self.log.info("%s - cleaning up finished task for %s", self.__class__.__name__, key)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            self.active_tasks.pop(key, None)

    async def _run_event(self, event: Event, league: str) -> None:
        """Execute runner logic for a single event, respecting throttling."""

        if self._semaphore:
            async with self._semaphore:
                await self.runner.run(event, self.output_dir, league, self.provider, self.sink)
        else:
            await self.runner.run(event, self.output_dir, league, self.provider, self.sink)

    async def _start_tasks(self, events: Sequence[tuple[str, Event]]) -> None:
        """Launch runner tasks for any newly discovered live events."""

        for league, event in events:
            key = self.event_key(event)
            if key in self.active_tasks:
                self.log.info("%s - already running task for %s", self.__class__.__name__, event)
                continue

            if self._semaphore and len(self.active_tasks) >= self.concurrency:
                self.log.info(
                    "%s - concurrency limit %s reached; postponing monitor for %s",
                    self.__class__.__name__,
                    self.concurrency,
                    event,
                )
                break

            self.log.info("%s - starting monitor for %s", self.__class__.__name__, event)
            task = asyncio.create_task(self._run_event(event, league))
            self.active_tasks[key] = task

    async def run_once(self) -> None:
        """Fetch snapshots from the sink, prune finished tasks, and start newcomers."""

        snapshots = await self.sink.load_snapshots(
            provider=self.provider,
            leagues=list(self._league_filter) if self._league_filter else None,
            output_dir=self.output_dir,
        )
        self.log.info(
            "%s - fetched daily events for %d leagues from sink",
            self.__class__.__name__,
            len(snapshots),
        )
        if not snapshots:
            self.log.warning("%s - no daily events available from sink", self.__class__.__name__)
            await self._prune_tasks({})
            return

        active_map: dict[str, set[str]] = {}
        to_start: list[tuple[str, Event]] = []

        for league in sorted(snapshots):
            events = snapshots[league]
            live_events = [event for event in events if self.should_monitor(event)]
            active_ids = {event.event_id for event in live_events}
            active_map[league] = active_ids
            self.log.info(
                "%s - league %s has %d live games",
                self.__class__.__name__,
                league,
                len(live_events),
            )
            for event in live_events:
                to_start.append((league, event))

        await self._prune_tasks(active_map)
        await self._start_tasks(to_start)
