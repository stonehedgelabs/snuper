from __future__ import annotations

import abc
import asyncio
import contextlib
import datetime as dt
import logging
import time
from pathlib import Path
from typing import Any, Sequence

from event_monitor.t import Event
from event_monitor.utils import load_events

__all__ = ["BaseRunner", "BaseMonitor"]


class BaseRunner(abc.ABC):
    """Interface that streams odds updates for an individual event."""

    def __init__(self, *, heartbeat_interval: float | None = None) -> None:
        """Initialise logging and optional heartbeat cadence."""

        self.log = logging.getLogger(self.__class__.__name__)
        self.heartbeat_interval = heartbeat_interval
        self._last_heartbeat = time.time()

    @abc.abstractmethod
    async def run(self, event: Event, output_dir: Path, league: str) -> None:
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

        if force or (
            self.heartbeat_interval
            and time.time() - self._last_heartbeat >= self.heartbeat_interval
        ):
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
    ) -> None:
        """Store directory paths, runner instance, and concurrency policy."""

        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir) if output_dir else self.input_dir
        self.runner = runner
        self.concurrency = concurrency
        self.log = logging.getLogger(self.__class__.__name__)
        self.active_tasks: dict[str, asyncio.Task[None]] = {}
        self._semaphore = (
            asyncio.Semaphore(concurrency) if concurrency and concurrency > 0 else None
        )

    def event_key(self, event: Event) -> str:
        """Build a unique key used to track active runner tasks."""

        return event.get_key()

    def should_monitor(self, event: Event) -> bool:
        """Return ``True`` if the event has started and is still live."""

        return event.has_started() and not event.is_finished()

    def file_glob(self) -> str:
        """Return the glob pattern that selects today's event snapshots."""

        today = dt.datetime.now().strftime("%Y%m%d")
        return f"*-{today}.json"

    def _get_event_files(self) -> list[Path]:
        """Return the sorted list of snapshot files to inspect."""

        return sorted(self.input_dir.glob(self.file_glob()))

    async def _prune_tasks(self, active_ids: dict[str, set[str]]) -> None:
        """Stop runners whose associated events no longer appear live."""

        for key, task in list(self.active_tasks.items()):
            league, event_id = key.split(":", 1)

            if task.done():
                exc = task.exception()
                if exc:
                    self.log.error("Runner for %s raised %s", key, exc, exc_info=exc)
                self.active_tasks.pop(key)
                continue

            live_ids = active_ids.get(league)
            if live_ids is None or event_id in live_ids:
                continue

            self.log.info("Cleaning up finished task for %s", key)
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
            await self.active_tasks.pop(key, None)

    async def _run_event(self, event: Event, league: str) -> None:
        """Execute runner logic for a single event, respecting throttling."""

        if self._semaphore:
            async with self._semaphore:
                await self.runner.run(event, self.output_dir, league)
        else:
            await self.runner.run(event, self.output_dir, league)

    async def _start_tasks(self, events: Sequence[tuple[str, Event]]) -> None:
        """Launch runner tasks for any newly discovered live events."""

        for league, event in events:
            key = self.event_key(event)
            if key in self.active_tasks:
                self.log.info("Already running task for %s", event)
                continue

            if self._semaphore and len(self.active_tasks) >= self.concurrency:
                self.log.info(
                    "Concurrency limit %s reached; postponing monitor for %s",
                    self.concurrency,
                    event,
                )
                break

            self.log.info("Starting monitor for %s", event)
            task = asyncio.create_task(self._run_event(event, league))
            self.active_tasks[key] = task

    async def run_once(self) -> None:
        """Inspect snapshot files, prune finished tasks, and start newcomers."""

        files = self._get_event_files()
        if not files:
            self.log.warning("No event files found for today.")
            return

        active_map: dict[str, set[str]] = {}
        to_start: list[tuple[str, Event]] = []

        for file_path in files:
            try:
                league, events = load_events(file_path)
            except Exception as exc:  # pragma: no cover - guard for runtime errors
                self.log.error("Failed to load %s: %s", file_path, exc)
                continue

            live_events = [event for event in events if self.should_monitor(event)]
            active_map[league] = {event.event_id for event in live_events}
            self.log.info(
                "Found %d live games for league %s.", len(live_events), league
            )
            for event in live_events:
                to_start.append((league, event))

        await self._prune_tasks(active_map)
        await self._start_tasks(to_start)
