import datetime as dt
import json
import logging
from pathlib import Path
from typing import Optional

from event_monitor.constants import RESET, DATE_STAMP_FORMAT
from event_monitor.t import Event


def current_stamp(now: dt.datetime | None = None) -> str:
    """Return the YYYYMMDD stamp used when naming snapshot files."""

    current = now or dt.datetime.now()
    return current.strftime(DATE_STAMP_FORMAT)


def event_filepath(output_dir: Path, league: str, *, timestamp: str | None = None) -> Path:
    """Build the filesystem path for an event snapshot JSON file."""

    ts = timestamp or current_stamp()
    return Path(output_dir) / "events" / f"{ts}-{league}.json"


def odds_filepath(
    output_dir: Path,
    league: str,
    event_id: str,
    *,
    timestamp: str | None = None,
) -> Path:
    """Build the filesystem path for an odds stream JSONL log."""

    ts = timestamp or current_stamp()
    return Path(output_dir) / "odds" / f"{ts}-{league}-{event_id}.json"


def decimal_to_american(decimal_odds: str) -> str:
    """Convert decimal odds (e.g. '1.80') to American format ('-125')."""
    try:
        d = float(decimal_odds)
        if d >= 2.0:
            return f"+{int((d - 1) * 100):d}"
        else:
            return f"-{int(100 / (d - 1)):d}"
    except Exception:
        return None


def load_events(file_path: Path) -> tuple[str, list[Event]]:
    """Read a JSON snapshot and return its league plus Event objects."""
    league = file_path.stem.split("-")[1]
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    events = []
    for e in data:
        start_time = dt.datetime.fromisoformat(e["start_time"])
        away = tuple(e["away"])
        home = tuple(e["home"])
        events.append(
            Event(
                e["event_id"],
                e["league"],
                e["event_url"],
                start_time,
                away,
                home,
                e["selections"],
            )
        )
    return league, events


class _ColorPrefixFilter(logging.Filter):
    """Inject ANSI color codes ahead of logger messages once per record."""

    def __init__(self, color: str) -> None:
        super().__init__()
        self.color = color

    def filter(self, record: logging.LogRecord) -> bool:  # type: ignore[type-arg]
        if getattr(record, "_colorized", None) == self.color:
            return True

        record.msg = f"{self.color}{record.msg}{RESET}"
        setattr(record, "_colorized", self.color)
        return True


def configure_colored_logger(name: str, color: str) -> logging.Logger:
    """Return logger ``name`` that prefixes messages with ``color`` codes."""

    logger = logging.getLogger(name)
    existing: Optional[str] = getattr(logger, "_color_prefix", None)
    if existing == color:
        return logger

    filt: Optional[_ColorPrefixFilter] = getattr(logger, "_color_filter", None)
    if filt is None:
        filt = _ColorPrefixFilter(color)
        logger.addFilter(filt)
        setattr(logger, "_color_filter", filt)
    else:
        filt.color = color

    setattr(logger, "_color_prefix", color)
    return logger
