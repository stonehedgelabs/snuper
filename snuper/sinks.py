from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import logging
from copy import deepcopy
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, cast
from collections.abc import Sequence

from snuper.t import Event
from snuper.utils import current_stamp, event_filepath, load_events, odds_filepath

try:  # pragma: no cover - optional dependency imports
    from sqlalchemy import (
        Column,
        DateTime,
        Index,
        Integer,
        LargeBinary,
        MetaData,
        String,
        Table,
        create_engine,
        func,
        select,
    )
    from sqlalchemy.dialects.postgresql import JSONB, insert as pg_insert
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.types import JSON
except ImportError:  # pragma: no cover - handled lazily when sink unused
    Column = DateTime = Index = Integer = LargeBinary = MetaData = String = Table = create_engine = func = select = None
    JSONB = JSON = SQLAlchemyError = pg_insert = None

try:  # pragma: no cover - optional dependency imports
    from redis.asyncio import Redis
    from redis.exceptions import RedisError
except ImportError:  # pragma: no cover - handled lazily when sink unused
    Redis = None
    RedisError = Exception  # type: ignore[assignment]

logger = logging.getLogger("snuper.sinks")


class SinkType(str, Enum):
    """Supported sink destinations for SelectionChange persistence."""

    FS = "fs"
    RDS = "rds"
    CACHE = "cache"


class SelectionSink(Protocol):
    """Protocol consumed by scrapers/monitors to persist snapshots and odds updates."""

    async def save(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        raw_event: Any,
        selection_update: dict[str, Any],
        output_dir: Path | None = None,
    ) -> None: ...

    def describe_destination(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        output_dir: Path | None = None,
    ) -> str | None: ...

    async def close(self) -> None: ...

    async def save_snapshot(
        self,
        *,
        provider: str,
        league: str,
        events: Sequence[Event],
        timestamp: str | None = None,
        output_dir: Path | None = None,
        overwrite: bool = False,
    ) -> Path | None: ...

    async def load_snapshots(
        self,
        *,
        provider: str,
        leagues: Sequence[str] | None = None,
        timestamp: str | None = None,
        output_dir: Path | None = None,
        verbose: bool = False,
    ) -> dict[str, list[Event]]: ...


@dataclass(slots=True)
class _SinkRecord:
    event_id: str
    league: str
    provider: str
    raw_event: Any
    raw_data: bytes | None
    selection_update: dict[str, Any]
    received_at: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "league": self.league,
            "provider": self.provider,
            "received_at": self.received_at,
            "raw_event": self.raw_event,
            "selection_update": self.selection_update,
        }


class BaseSink(SelectionSink):
    """Helper base class providing shared behaviours."""

    def __init__(self) -> None:
        super().__init__()

    async def save(  # pragma: no cover - abstract helper
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        raw_event: Any,
        selection_update: dict[str, Any],
        output_dir: Path | None = None,
    ) -> None:
        raise NotImplementedError

    def describe_destination(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        output_dir: Path | None = None,
    ) -> str | None:
        return None

    async def close(self) -> None:  # pragma: no cover - default no-op
        return

    async def save_snapshot(
        self,
        *,
        provider: str,
        league: str,
        events: Sequence[Event],
        timestamp: str | None = None,
        output_dir: Path | None = None,
        overwrite: bool = False,
    ) -> Path | None:  # pragma: no cover - abstract helper
        raise NotImplementedError

    async def load_snapshots(
        self,
        *,
        provider: str,
        leagues: Sequence[str] | None = None,
        timestamp: str | None = None,
        output_dir: Path | None = None,
        verbose: bool = False,
    ) -> dict[str, list[Event]]:  # pragma: no cover - abstract helper
        raise NotImplementedError


def _iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _coerce_raw_data(raw_event: Any) -> bytes | None:
    if raw_event is None:
        return None
    if isinstance(raw_event, (bytes, bytearray, memoryview)):
        return bytes(raw_event)
    if isinstance(raw_event, str):
        return raw_event.encode("utf-8")
    try:
        return json.dumps(raw_event, ensure_ascii=False).encode("utf-8")
    except (TypeError, ValueError):
        return str(raw_event).encode("utf-8")


def _normalise_raw_event(raw_event: Any) -> Any:
    if raw_event is None:
        return None
    if isinstance(raw_event, (dict, list, int, float, bool)):
        return raw_event
    if isinstance(raw_event, bytes):
        try:
            text = raw_event.decode("utf-8")
        except UnicodeDecodeError:
            encoded = base64.b64encode(raw_event).decode("ascii")
            return {"encoding": "base64", "data": encoded}
        return _normalise_raw_event(text)
    if isinstance(raw_event, str):
        stripped = raw_event.strip()
        if not stripped:
            return ""
        try:
            return json.loads(stripped)
        except json.JSONDecodeError:
            return stripped
    return json.loads(json.dumps(raw_event, default=str))


def _build_record(
    *,
    provider: str,
    league: str,
    event: Event,
    raw_event: Any,
    selection_update: dict[str, Any],
) -> _SinkRecord:
    received_at = selection_update.get("created_at") if isinstance(selection_update, dict) else None
    if not isinstance(received_at, str) or not received_at:
        received_at = _iso_now()
    normalised_raw = _normalise_raw_event(raw_event)
    raw_bytes = _coerce_raw_data(raw_event)
    return _SinkRecord(
        event_id=event.event_id,
        league=league,
        provider=provider,
        raw_event=normalised_raw,
        raw_data=raw_bytes,
        selection_update=selection_update,
        received_at=received_at,
    )


def _clone_json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return deepcopy(value)
    return value


def _format_selection_payload(selection_update: Any) -> dict[str, Any]:
    if not isinstance(selection_update, dict):
        return {"data": _clone_json(selection_update)}

    inner = selection_update.get("data")
    if inner is not None:
        selection_data = _clone_json(inner)
    else:
        selection_data = {
            key: _clone_json(value) for key, value in selection_update.items() if key not in {"label", "created_at"}
        }

    payload: dict[str, Any] = {"data": selection_data}

    label = selection_update.get("label")
    if label is not None:
        payload["label"] = label

    created_at = selection_update.get("created_at")
    if created_at is not None:
        payload["created_at"] = created_at

    return payload


def _events_to_payload(events: Sequence[Event]) -> list[dict[str, Any]]:
    return [event.to_dict() for event in events]


def _event_from_dict(payload: dict[str, Any]) -> Event:
    source = payload.get("event", payload)
    start_time = dt.datetime.fromisoformat(source["start_time"])
    away = tuple(source["away"])
    home = tuple(source["home"])
    selections = source.get("selections")
    return Event(
        source["event_id"],
        source["league"],
        source["event_url"],
        start_time,
        away,
        home,
        selections,
    )


class FilesystemSelectionSink(BaseSink):
    """Filesystem-based sink for storing sportsbook data as JSON files.

    This is the simplest and fastest sink option, ideal for getting started
    without database setup. Data is stored in a structured directory tree:

    Structure:
        {base_dir}/{provider}/events/YYYYMMDD-{league}.json  # Daily snapshots
        {base_dir}/{provider}/odds/YYYYMMDD-{league}-{event_id}.json  # Live odds

    File Formats:
        - Events: Single JSON array containing all games for the day
        - Odds: Newline-delimited JSON (JSONL) with one record per line
    """

    def __init__(self, base_dir: Path) -> None:
        """Initialize filesystem sink with base directory.

        Args:
            base_dir: Root directory for storing all sportsbook data
        """
        super().__init__()
        self.base_dir = Path(base_dir)

    def describe_destination(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        output_dir: Path | None = None,
    ) -> str | None:
        root = Path(output_dir) if output_dir else self.base_dir
        path = odds_filepath(root, league, event.event_id)
        return str(path)

    async def save(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        raw_event: Any,
        selection_update: dict[str, Any],
        output_dir: Path | None = None,
    ) -> None:
        """Append a single odds update to the JSONL file for this event.

        Each line in the output file contains a timestamped JSON object with:
        - provider: Source sportsbook name
        - league: Sport league (nba, nfl, mlb)
        - event: Game metadata (teams, start time, etc.)
        - selection_update: Current odds/spreads for all markets
        - raw: Original provider response (for debugging)
        - timestamp: ISO format timestamp of this update
        """
        root = Path(output_dir) if output_dir else self.base_dir
        record = _build_record(
            provider=provider,
            league=league,
            event=event,
            raw_event=raw_event,
            selection_update=selection_update,
        )
        path = odds_filepath(root, league, event.event_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(record.to_dict(), ensure_ascii=False)

        def _write() -> None:
            with open(path, "a", encoding="utf-8") as handle:
                handle.write(payload + "\n")

        try:
            await asyncio.to_thread(_write)
        except OSError as exc:  # pragma: no cover - filesystem failure guard
            logger.warning("filesystem sink failed for %s: %s", path, exc)

    async def save_snapshot(
        self,
        *,
        provider: str,
        league: str,
        events: Sequence[Event],
        timestamp: str | None = None,
        output_dir: Path | None = None,
        overwrite: bool = False,
    ) -> Path | None:
        """Save daily event snapshot to JSON file.

        Creates or updates a JSON file containing all events for a given
        league and date. Each file contains an array of event objects with:
        - event_id: Unique identifier for the game
        - league: Sport league
        - event_url: Link to game page on sportsbook
        - start_time: Game start time in ISO format
        - away/home: Team names as [display_name, identifier]
        - selections: Initial odds/spreads for all markets

        Args:
            provider: Sportsbook provider name
            league: Sport league (nba, nfl, mlb)
            events: List of Event objects to save
            timestamp: Optional timestamp (defaults to now)
            output_dir: Override base directory if specified
            overwrite: Whether to replace existing snapshots

        Returns:
            Path to saved snapshot file, or None if skipped
        """
        root = Path(output_dir) if output_dir else self.base_dir
        if root is None:
            raise ValueError("filesystem sink requires an output directory")
        stamp = timestamp or current_stamp()
        path = event_filepath(root, league, timestamp=stamp)
        if path.exists() and not overwrite:
            logger.info("filesystem sink - snapshot %s already exists; skipping write", path)
            return path

        payload = _events_to_payload(events)

        def _write_snapshot() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("w", encoding="utf-8") as handle:
                json.dump(payload, handle, indent=2)

        try:
            await asyncio.to_thread(_write_snapshot)
        except OSError as exc:  # pragma: no cover - filesystem failure guard
            logger.warning("filesystem sink failed to write snapshot %s: %s", path, exc)
            return None
        logger.info("filesystem sink - saved snapshot for %s/%s to %s", provider, league, path)
        return path

    async def load_snapshots(
        self,
        *,
        provider: str,
        leagues: Sequence[str] | None = None,
        timestamp: str | None = None,
        output_dir: Path | None = None,
        verbose: bool = False,
    ) -> dict[str, list[Event]]:
        del verbose  # Filesystem sink doesn't need verbose logging for load operations
        root = Path(output_dir) if output_dir else self.base_dir
        if root is None:
            return {}
        stamp = timestamp or current_stamp()
        events_dir = root / "events"
        league_filter: set[str] | None = {league.lower() for league in leagues} if leagues else None

        def _read() -> dict[str, list[Event]]:
            if not events_dir.exists():
                return {}
            results: dict[str, list[Event]] = {}
            if league_filter:
                # pylint: disable=not-an-iterable
                files = [events_dir / f"{stamp}-{league}.json" for league in league_filter]
            else:
                files = list(events_dir.glob(f"{stamp}-*.json"))
            for file_path in files:
                if not file_path.exists():
                    continue
                try:
                    league_name, loaded_events = load_events(file_path)
                except Exception as exc:  # pragma: no cover - defensive runtime guard
                    logger.warning("filesystem sink failed to load %s: %s", file_path, exc)
                    continue
                results[league_name.lower()] = loaded_events
            return results

        return await asyncio.to_thread(_read)


class RdsSelectionSink(BaseSink):
    """Persist odds updates into a relational data store."""

    def __init__(self, *, uri: str, table_name: str, selection_table_name: str | None = None) -> None:
        super().__init__()
        if create_engine is None or MetaData is None or JSON is None or func is None:
            msg = "sqlalchemy must be installed to use the RDS sink"
            raise RuntimeError(msg)
        # Configure connection pool to handle long-running tasks and idle timeouts
        self._engine = create_engine(
            uri,
            future=True,
            pool_pre_ping=True,  # Verify connections before using them
            pool_recycle=3600,  # Recycle connections after 1 hour
            pool_size=5,  # Maintain 5 connections in the pool
            max_overflow=10,  # Allow up to 15 total connections
        )
        self._table_name = table_name
        self._selection_table_name = selection_table_name or table_name
        self._metadata = MetaData()
        typed_func = cast(Any, func)
        if typed_func is None:
            raise RuntimeError("sqlalchemy func helper unavailable")
        if JSONB is not None and self._engine.dialect.name == "postgresql":
            json_type_cls = JSONB
        else:
            json_type_cls = JSON
        self._func: Any = typed_func
        self._table = Table(
            table_name,
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("provider", String, nullable=False),
            Column("league", String, nullable=False),
            Column("event_id", String, nullable=False),
            Column("raw_data", LargeBinary, nullable=True),
            Column("data", json_type_cls(), nullable=False),
            # pylint: disable=not-callable
            Column("created_at", DateTime(timezone=True), server_default=self._func.now(), nullable=False),
        )
        self._index = Index(
            f"ix_{table_name}_provider_event",
            self._table.c.provider,
            self._table.c.event_id,
        )
        # Unique index for snapshots table only (enables upserts)
        self._upsert_index = Index(
            f"ux_{table_name}_provider_league_event",
            self._table.c.provider,
            self._table.c.league,
            self._table.c.event_id,
            unique=True,
        )
        if self._selection_table_name == table_name:
            self._selection_table = self._table
            self._selection_index = self._index
        else:
            self._selection_table = Table(
                self._selection_table_name,
                self._metadata,
                Column("id", Integer, primary_key=True, autoincrement=True),
                Column("provider", String, nullable=False),
                Column("league", String, nullable=False),
                Column("event_id", String, nullable=False),
                Column("raw_data", LargeBinary, nullable=True),
                Column("data", json_type_cls(), nullable=False),
                # pylint: disable=not-callable
                Column("created_at", DateTime(timezone=True), server_default=self._func.now(), nullable=False),
            )
            self._selection_index = Index(
                f"ix_{self._selection_table_name}_provider_event",
                self._selection_table.c.provider,
                self._selection_table.c.event_id,
            )
        self._ready = False
        self._lock: asyncio.Lock | None = None
        self._supports_upsert = self._engine.dialect.name == "postgresql" and pg_insert is not None

    async def _prepare(self) -> None:
        if self._ready:
            return
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._ready:
                return

            def _create() -> None:
                tables_to_create = [self._table]
                if self._selection_table is not self._table:
                    tables_to_create.append(self._selection_table)
                self._metadata.create_all(
                    self._engine,
                    tables=tables_to_create,
                )
                self._index.create(self._engine, checkfirst=True)
                self._upsert_index.create(self._engine, checkfirst=True)
                if self._selection_table is not self._table:
                    self._selection_index.create(self._engine, checkfirst=True)

            await asyncio.to_thread(_create)
            self._ready = True

    def describe_destination(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        output_dir: Path | None = None,
    ) -> str | None:
        del provider, league, event, output_dir
        return self._selection_table_name

    async def save(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        raw_event: Any,
        selection_update: dict[str, Any],
        output_dir: Path | None = None,
    ) -> None:
        await self._prepare()
        record = _build_record(
            provider=provider,
            league=league,
            event=event,
            raw_event=raw_event,
            selection_update=selection_update,
        )

        def _insert() -> None:
            with self._engine.begin() as conn:
                data_payload = _format_selection_payload(record.selection_update)
                stmt = self._selection_table.insert().values(
                    provider=record.provider,
                    league=record.league,
                    event_id=record.event_id,
                    raw_data=record.raw_data,
                    data=data_payload,
                )
                conn.execute(stmt)

        # Retry logic for transient connection errors
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                await asyncio.to_thread(_insert)
                break  # Success, exit retry loop
            except SQLAlchemyError as exc:  # pragma: no cover - database failure guard
                error_msg = str(exc)
                is_connection_error = any(
                    phrase in error_msg.lower()
                    for phrase in [
                        "server closed the connection",
                        "connection was closed",
                        "connection refused",
                        "connection timeout",
                        "could not connect",
                    ]
                )
                if is_connection_error and attempt < max_retries:
                    logger.warning(
                        "rds sink connection error for %s/%s (attempt %d/%d): %s. Retrying...",
                        league,
                        event.event_id,
                        attempt,
                        max_retries,
                        exc,
                    )
                    await asyncio.sleep(2 ** (attempt - 1))  # Exponential backoff: 1s, 2s, 4s
                else:
                    logger.warning("rds sink failed for %s/%s: %s", league, event.event_id, exc)
                    break  # Non-retryable error or max retries reached

    async def save_snapshot(
        self,
        *,
        provider: str,
        league: str,
        events: Sequence[Event],
        timestamp: str | None = None,
        output_dir: Path | None = None,
        overwrite: bool = False,
    ) -> Path | None:
        del output_dir, overwrite  # unused for RDS sink
        await self._prepare()
        snapshot_date = timestamp or current_stamp()

        rows = [
            {
                "provider": provider,
                "league": league,
                "event_id": event.event_id,
                "data": {
                    "snapshot_timestamp": snapshot_date,
                    "event": event.to_dict(),
                },
            }
            for event in events
        ]

        if not rows:
            logger.info(
                "rds sink - no events to persist for %s/%s at %s",
                provider,
                league,
                snapshot_date,
            )
            return None

        def _insert_snapshot() -> None:
            with self._engine.begin() as conn:
                if self._supports_upsert:
                    stmt = pg_insert(self._table).values(rows)
                    stmt = stmt.on_conflict_do_update(
                        index_elements=[
                            self._table.c.provider,
                            self._table.c.league,
                            self._table.c.event_id,
                        ],
                        set_={
                            "data": stmt.excluded.data,
                            "created_at": self._func.now(),  # pylint: disable=not-callable
                        },
                    )
                    conn.execute(stmt)
                else:
                    conn.execute(self._table.insert(), rows)

        # Retry logic for transient connection errors
        max_retries = 3
        for attempt in range(1, max_retries + 1):
            try:
                await asyncio.to_thread(_insert_snapshot)
                logger.info(
                    "rds sink - persisted %d events for %s/%s to table %s",
                    len(rows),
                    provider,
                    league,
                    self._table_name,
                )
                break  # Success, exit retry loop
            except SQLAlchemyError as exc:  # pragma: no cover - database failure guard
                error_msg = str(exc)
                is_connection_error = any(
                    phrase in error_msg.lower()
                    for phrase in [
                        "server closed the connection",
                        "connection was closed",
                        "connection refused",
                        "connection timeout",
                        "could not connect",
                    ]
                )
                if is_connection_error and attempt < max_retries:
                    logger.warning(
                        "rds sink connection error for %s/%s (attempt %d/%d): %s. Retrying...",
                        provider,
                        league,
                        attempt,
                        max_retries,
                        exc,
                    )
                    await asyncio.sleep(2 ** (attempt - 1))  # Exponential backoff: 1s, 2s, 4s
                else:
                    logger.warning("rds sink failed to save snapshot for %s/%s: %s", provider, league, exc)
                    break  # Non-retryable error or max retries reached
        return None

    async def load_snapshots(
        self,
        *,
        provider: str,
        leagues: Sequence[str] | None = None,
        timestamp: str | None = None,
        output_dir: Path | None = None,
        verbose: bool = False,
    ) -> dict[str, list[Event]]:
        del output_dir  # unused
        await self._prepare()
        league_filter = {league.lower() for league in leagues} if leagues else None

        def _fetch() -> dict[str, list[Event]]:
            active_league_filter: set[str] | None = set(league_filter) if league_filter is not None else None

            stmt = (
                select(
                    self._table.c.league,
                    self._table.c.event_id,
                    self._table.c.data,
                    self._table.c.created_at,
                )
                .where(self._table.c.provider == provider)
                .order_by(
                    self._table.c.league,
                    self._table.c.created_at.desc(),
                    self._table.c.event_id,
                )
            )
            if active_league_filter is not None:
                stmt = stmt.where(self._table.c.league.in_(active_league_filter))

            with self._engine.begin() as conn:
                rows = conn.execute(stmt).all()

            if verbose:
                logger.info(
                    "rds sink - fetched %d rows for provider %s",
                    len(rows),
                    provider,
                )

            per_league_events: dict[str, dict[str, Event]] = {}

            for row in rows:
                league_value = (row.league or "").lower()
                if active_league_filter is not None and league_value not in active_league_filter:
                    continue
                data_payload = row.data or {}
                event_payload = data_payload.get("event", data_payload)
                if not isinstance(event_payload, dict):
                    logger.debug(
                        "rds sink - skipping row for %s/%s due to unexpected payload format",
                        provider,
                        league_value,
                    )
                    continue
                try:
                    event_obj = _event_from_dict(event_payload)
                except Exception as exc:  # pragma: no cover - defensive guard
                    logger.warning(
                        "rds sink - failed to parse event payload for %s/%s: %s",
                        provider,
                        league_value,
                        exc,
                    )
                    continue
                bucket = per_league_events.setdefault(league_value, {})
                if event_obj.event_id in bucket:
                    continue
                bucket[event_obj.event_id] = event_obj

            results: dict[str, list[Event]] = {}
            for league_value, league_events in per_league_events.items():
                ordered = sorted(league_events.values(), key=lambda item: item.start_time)
                results[league_value] = ordered
                if verbose:
                    logger.info(
                        "rds sink - read %d events from %s/%s",
                        len(ordered),
                        provider,
                        league_value,
                    )
            if verbose:
                logger.info(
                    "rds sink - returning %d leagues to monitor for provider %s",
                    len(results),
                    provider,
                )
            return results

        try:
            return await asyncio.to_thread(_fetch)
        except SQLAlchemyError as exc:  # pragma: no cover - database failure guard
            logger.warning("rds sink failed to load snapshots for %s: %s", provider, exc)
            return {}


class CacheSelectionSink(BaseSink):
    """Push odds updates into a cache supporting Redis list operations."""

    def __init__(self, *, uri: str, ttl: int, max_items: int) -> None:
        super().__init__()
        if Redis is None:
            msg = "redis must be installed to use the cache sink"
            raise RuntimeError(msg)
        if ttl <= 0:
            raise ValueError("cache ttl must be a positive integer")
        if max_items <= 0:
            raise ValueError("cache max items must be a positive integer")
        self._client = Redis.from_url(uri)
        self._ttl = ttl
        self._max_items = max_items

    def describe_destination(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        output_dir: Path | None = None,
    ) -> str | None:
        base = f"snuper:{league}:{event.event_id}"
        return f"Redis lists {base}:raw and {base}:selection"

    async def save(
        self,
        *,
        provider: str,
        league: str,
        event: Event,
        raw_event: Any,
        selection_update: dict[str, Any],
        output_dir: Path | None = None,
    ) -> None:
        base = f"snuper:{league}:{event.event_id}"
        raw_key = f"{base}:raw"
        selection_key = f"{base}:selection"
        record = _build_record(
            provider=provider,
            league=league,
            event=event,
            raw_event=raw_event,
            selection_update=selection_update,
        )
        raw_payload = json.dumps(
            {
                "event_id": record.event_id,
                "league": record.league,
                "provider": record.provider,
                "received_at": record.received_at,
                "payload": record.raw_event,
            },
            ensure_ascii=False,
        )
        selection_payload = json.dumps(
            {
                "event_id": record.event_id,
                "league": record.league,
                "provider": record.provider,
                "received_at": record.received_at,
                "payload": record.selection_update,
            },
            ensure_ascii=False,
        )
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                pipe.rpush(raw_key, raw_payload)
                pipe.rpush(selection_key, selection_payload)
                pipe.expire(raw_key, self._ttl)
                pipe.expire(selection_key, self._ttl)
                pipe.ltrim(raw_key, -self._max_items, -1)
                pipe.ltrim(selection_key, -self._max_items, -1)
                await pipe.execute()
        except RedisError as exc:  # pragma: no cover - cache failure guard
            logger.warning("cache sink failed for %s/%s: %s", league, event.event_id, exc)

    async def close(self) -> None:
        await self._client.close()

    async def save_snapshot(
        self,
        *,
        provider: str,
        league: str,
        events: Sequence[Event],
        timestamp: str | None = None,
        output_dir: Path | None = None,
        overwrite: bool = False,
    ) -> Path | None:
        del output_dir, overwrite  # unused for cache sink
        stamp = timestamp or current_stamp()
        snapshot_key = f"snuper:snapshots:{provider}:{league}"
        leagues_key = f"snuper:snapshots:{provider}:leagues"
        payload = json.dumps(
            {"timestamp": stamp, "events": _events_to_payload(events)},
            ensure_ascii=False,
        )
        try:
            async with self._client.pipeline(transaction=False) as pipe:
                pipe.set(snapshot_key, payload, ex=self._ttl)
                pipe.sadd(leagues_key, league)
                pipe.expire(leagues_key, self._ttl)
                await pipe.execute()
        except RedisError as exc:  # pragma: no cover - cache failure guard
            logger.warning("cache sink failed to save snapshot for %s/%s: %s", provider, league, exc)
        return None

    async def load_snapshots(
        self,
        *,
        provider: str,
        leagues: Sequence[str] | None = None,
        timestamp: str | None = None,
        output_dir: Path | None = None,
        verbose: bool = False,
    ) -> dict[str, list[Event]]:
        del output_dir, verbose  # unused for cache sink
        leagues_key = f"snuper:snapshots:{provider}:leagues"
        try:
            if leagues is None:
                league_members = await self._client.smembers(leagues_key)
                league_list = sorted(l.decode("utf-8") if isinstance(l, bytes) else str(l) for l in league_members)
            else:
                league_list = list(leagues)
        except RedisError as exc:  # pragma: no cover - cache failure guard
            logger.warning("cache sink failed to read snapshot leagues for %s: %s", provider, exc)
            return {}

        if not league_list:
            return {}

        keys = [f"snuper:snapshots:{provider}:{league}" for league in league_list]
        try:
            values = await self._client.mget(keys)
        except RedisError as exc:  # pragma: no cover - cache failure guard
            logger.warning("cache sink failed to load snapshots for %s: %s", provider, exc)
            return {}

        results: dict[str, list[Event]] = {}
        for league, value in zip(league_list, values):
            if value is None:
                continue
            if isinstance(value, bytes):
                text = value.decode("utf-8")
            else:
                text = str(value)
            try:
                payload_obj = json.loads(text)
            except json.JSONDecodeError:
                logger.warning("cache sink snapshot for %s/%s is not valid JSON", provider, league)
                continue
            if timestamp and payload_obj.get("timestamp") != timestamp:
                continue
            events_payload = payload_obj.get("events", [])
            events = [_event_from_dict(item) for item in events_payload]
            results[league.lower()] = events
        return results


def build_sink(
    *,
    sink_type: SinkType,
    fs_sink_dir: Path | None = None,
    rds_uri: str | None = None,
    rds_table: str | None = None,
    rds_selection_table: str | None = None,
    cache_uri: str | None = None,
    cache_ttl: int | None = None,
    cache_max_items: int | None = None,
) -> SelectionSink:
    if sink_type is SinkType.FS:
        if fs_sink_dir is None:
            raise ValueError("filesystem sink requires fs_sink_dir")
        return FilesystemSelectionSink(fs_sink_dir)
    if sink_type is SinkType.RDS:
        if not rds_uri or not rds_table:
            raise ValueError("rds sink requires rds_uri and rds_table")
        return RdsSelectionSink(
            uri=rds_uri,
            table_name=rds_table,
            selection_table_name=rds_selection_table,
        )
    if sink_type is SinkType.CACHE:
        if not cache_uri or cache_ttl is None or cache_max_items is None:
            raise ValueError("cache sink requires cache_uri, cache_ttl, and cache_max_items")
        return CacheSelectionSink(uri=cache_uri, ttl=cache_ttl, max_items=cache_max_items)
    raise ValueError(f"Unsupported sink type: {sink_type}")
