from __future__ import annotations

import asyncio
import base64
import datetime as dt
import json
import logging
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Protocol, cast
from collections.abc import Sequence

from snuper.t import Event
from snuper.utils import current_stamp, event_filepath, load_events, odds_filepath

try:  # pragma: no cover - optional dependency imports
    from sqlalchemy import Column, DateTime, Index, Integer, MetaData, String, Table, create_engine, func, select
    from sqlalchemy.dialects.postgresql import JSONB
    from sqlalchemy.exc import SQLAlchemyError
    from sqlalchemy.types import JSON
except ImportError:  # pragma: no cover - handled lazily when sink unused
    Column = DateTime = Index = Integer = MetaData = String = Table = create_engine = func = select = None
    JSONB = JSON = SQLAlchemyError = None

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
    ) -> dict[str, list[Event]]: ...


@dataclass(slots=True)
class _SinkRecord:
    event_id: str
    league: str
    provider: str
    raw_event: Any
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
    ) -> dict[str, list[Event]]:  # pragma: no cover - abstract helper
        raise NotImplementedError


def _iso_now() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


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
    return _SinkRecord(
        event_id=event.event_id,
        league=league,
        provider=provider,
        raw_event=normalised_raw,
        selection_update=selection_update,
        received_at=received_at,
    )


def _events_to_payload(events: Sequence[Event]) -> list[dict[str, Any]]:
    return [event.to_dict() for event in events]


def _event_from_dict(payload: dict[str, Any]) -> Event:
    start_time = dt.datetime.fromisoformat(payload["start_time"])
    away = tuple(payload["away"])
    home = tuple(payload["home"])
    selections = payload.get("selections")
    return Event(
        payload["event_id"],
        payload["league"],
        payload["event_url"],
        start_time,
        away,
        home,
        selections,
    )


class FilesystemSelectionSink(BaseSink):
    """Append odds updates to JSONL files on disk."""

    def __init__(self, base_dir: Path) -> None:
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
    ) -> dict[str, list[Event]]:
        root = Path(output_dir) if output_dir else self.base_dir
        if root is None:
            return {}
        stamp = timestamp or current_stamp()
        events_dir = root / "events"
        league_filter = {league.lower() for league in leagues} if leagues else None

        def _read() -> dict[str, list[Event]]:
            if not events_dir.exists():
                return {}
            results: dict[str, list[Event]] = {}
            if league_filter:
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

    def __init__(self, *, uri: str, table_name: str) -> None:
        super().__init__()
        if create_engine is None or MetaData is None or JSON is None or func is None:
            msg = "sqlalchemy must be installed to use the RDS sink"
            raise RuntimeError(msg)
        self._engine = create_engine(uri, future=True)
        self._table_name = table_name
        self._metadata = MetaData()
        typed_func = cast(Any, func)
        if typed_func is None:
            raise RuntimeError("sqlalchemy func helper unavailable")
        if JSONB is not None and self._engine.dialect.name == "postgresql":
            json_type_cls = JSONB
        else:
            json_type_cls = JSON
        self._table = Table(
            table_name,
            self._metadata,
            Column("id", Integer, primary_key=True, autoincrement=True),
            Column("provider", String, nullable=False),
            Column("league", String, nullable=False),
            Column("event_id", String, nullable=False),
            Column("data", json_type_cls(), nullable=False),
            # pylint: disable=not-callable
            Column("created_at", DateTime(timezone=True), server_default=typed_func.now(), nullable=False),
        )
        self._index = Index(
            f"ix_{table_name}_provider_event",
            self._table.c.provider,
            self._table.c.event_id,
        )
        snapshot_table_name = f"{table_name}_snapshots"
        self._snapshot_table = Table(
            snapshot_table_name,
            self._metadata,
            Column("snapshot_id", Integer, primary_key=True, autoincrement=True),
            Column("provider", String, nullable=False),
            Column("league", String, nullable=False),
            Column("snapshot_date", String, nullable=False),
            Column("payload", json_type_cls(), nullable=False),
            # pylint: disable=not-callable
            Column("received_at", DateTime(timezone=True), server_default=typed_func.now(), nullable=False),
        )
        self._snapshot_index = Index(
            f"ix_{snapshot_table_name}_provider_league",
            self._snapshot_table.c.provider,
            self._snapshot_table.c.league,
            self._snapshot_table.c.snapshot_date,
        )
        self._ready = False
        self._lock: asyncio.Lock | None = None

    async def _prepare(self) -> None:
        if self._ready:
            return
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            if self._ready:
                return

            def _create() -> None:
                self._metadata.create_all(
                    self._engine,
                    tables=[self._table, self._snapshot_table],
                )
                self._index.create(self._engine, checkfirst=True)
                self._snapshot_index.create(self._engine, checkfirst=True)

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
        return self._table_name

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
                stmt = self._table.insert().values(
                    provider=record.provider,
                    league=record.league,
                    event_id=record.event_id,
                    data={
                        "raw_event": record.raw_event,
                        "selection_update": record.selection_update,
                        "received_at": record.received_at,
                    },
                )
                conn.execute(stmt)

        try:
            await asyncio.to_thread(_insert)
        except SQLAlchemyError as exc:  # pragma: no cover - database failure guard
            logger.warning("rds sink failed for %s/%s: %s", league, event.event_id, exc)

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
        payload = _events_to_payload(events)

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
                conn.execute(self._table.insert(), rows)
                stmt = self._snapshot_table.insert().values(
                    provider=provider,
                    league=league,
                    snapshot_date=snapshot_date,
                    payload=payload,
                )
                conn.execute(stmt)

        try:
            await asyncio.to_thread(_insert_snapshot)
            logger.info(
                "rds sink - persisted %d events for %s/%s to table %s",
                len(rows),
                provider,
                league,
                self._table_name,
            )
        except SQLAlchemyError as exc:  # pragma: no cover - database failure guard
            logger.warning("rds sink failed to save snapshot for %s/%s: %s", provider, league, exc)
        return None

    async def load_snapshots(
        self,
        *,
        provider: str,
        leagues: Sequence[str] | None = None,
        timestamp: str | None = None,
        output_dir: Path | None = None,
    ) -> dict[str, list[Event]]:
        del output_dir  # unused
        await self._prepare()
        league_filter = {league.lower() for league in leagues} if leagues else None
        snapshot_date = timestamp

        def _fetch() -> dict[str, list[Event]]:
            stmt = (
                select(
                    self._snapshot_table.c.league,
                    self._snapshot_table.c.payload,
                    self._snapshot_table.c.snapshot_date,
                    self._snapshot_table.c.received_at,
                )
                .where(self._snapshot_table.c.provider == provider)
                .order_by(
                    self._snapshot_table.c.league,
                    self._snapshot_table.c.snapshot_date.desc(),
                    self._snapshot_table.c.received_at.desc(),
                )
            )
            if league_filter:
                stmt = stmt.where(self._snapshot_table.c.league.in_(league_filter))
            if snapshot_date:
                stmt = stmt.where(self._snapshot_table.c.snapshot_date == snapshot_date)

            with self._engine.begin() as conn:
                rows = conn.execute(stmt).all()

            results: dict[str, list[Event]] = {}
            for row in rows:
                league_value = row.league.lower()
                if league_value in results:
                    continue
                try:
                    payload = row.payload or []
                except AttributeError:  # pragma: no cover - safety net for older SQLAlchemy versions
                    payload = row[1] if len(row) > 1 else []
                    league_value = (row[0] or "").lower()
                    if league_value in results:
                        continue
                events = [_event_from_dict(item) for item in payload]
                results[league_value] = events
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
    ) -> dict[str, list[Event]]:
        del output_dir  # unused for cache sink
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
        return RdsSelectionSink(uri=rds_uri, table_name=rds_table)
    if sink_type is SinkType.CACHE:
        if not cache_uri or cache_ttl is None or cache_max_items is None:
            raise ValueError("cache sink requires cache_uri, cache_ttl, and cache_max_items")
        return CacheSelectionSink(uri=cache_uri, ttl=cache_ttl, max_items=cache_max_items)
    raise ValueError(f"Unsupported sink type: {sink_type}")
