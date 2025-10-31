import datetime as dt
import json
import logging
import re
from pathlib import Path

from snuper.constants import RESET, DATE_STAMP_FORMAT
from snuper.t import Event, Team


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


def decimal_to_american(decimal_odds: str) -> str | None:
    """Convert decimal odds (e.g. '1.80') to American format ('-125')."""

    try:
        value = float(decimal_odds)
    except (TypeError, ValueError):
        return None

    if value >= 2.0:
        return f"+{int((value - 1) * 100):d}"

    if value <= 1:
        return None

    return f"-{int(100 / (value - 1)):d}"


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
    existing: str | None = getattr(logger, "_color_prefix", None)
    if existing == color:
        return logger

    filt: _ColorPrefixFilter | None = getattr(logger, "_color_filter", None)
    if filt is None:
        filt = _ColorPrefixFilter(color)
        logger.addFilter(filt)
        setattr(logger, "_color_filter", filt)
    else:
        filt.color = color

    setattr(logger, "_color_prefix", color)
    return logger


def format_duration(seconds: float) -> str:
    """Return a short human-readable duration like ``'1hr 2mins'``."""

    if seconds <= 0:
        return "0s"

    total_seconds = int(seconds)
    minutes, secs = divmod(total_seconds, 60)
    hours, mins = divmod(minutes, 60)
    days, hrs = divmod(hours, 24)

    parts: list[str] = []
    if days:
        suffix = "day" if days == 1 else "days"
        parts.append(f"{days}{suffix}")
    if hrs:
        suffix = "hr" if hrs == 1 else "hrs"
        parts.append(f"{hrs}{suffix}")
    if mins:
        suffix = "min" if mins == 1 else "mins"
        parts.append(f"{mins}{suffix}")

    if not parts:
        result = f"{secs}s"
    else:
        if len(parts) < 2 and secs and not days:
            parts.append(f"{secs}s")
        result = " ".join(parts[:2])

    return result


def format_bytes(num_bytes: int) -> str:
    """Return a compact string such as ``'2.3GB'`` for ``num_bytes``."""

    if num_bytes <= 0:
        return "0B"

    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    value = float(num_bytes)

    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(value)}B"
            if value < 10:
                return f"{value:.1f}{unit}"
            return f"{value:.0f}{unit}"
        value /= 1024

    return ""


def format_rate_per_sec(count: int, elapsed_seconds: float) -> str:
    """Format ``count`` occurrences across ``elapsed_seconds`` as ``'1.2/s'``."""

    if elapsed_seconds <= 0 or count <= 0:
        return "0.0/s"

    rate = count / elapsed_seconds
    if rate >= 100:
        return f"{rate:,.0f}/s"
    return f"{rate:,.1f}/s"


def mgm_constant_to_team(constant_name: str, teams: list[Team]) -> Team | None:
    """
    Given an MGM_* constant (e.g. 'MGM_MLB_LOS_ANGELES_DODGERS'),
    return the matching Team object from the provided teams list.
    """
    match = re.match(r"^MGM_([A-Z]+)_(.*)$", constant_name)
    if not match:
        return None

    league = match.group(1)
    team_slug = match.group(2).lower().replace("_", "-")

    # Remove prefix 'mgm_<league>_' and convert back to slug form
    for team in teams:
        slug = f"{(team.city or '').lower().replace(' ', '-')}-{team.name.lower().replace(' ', '-')}"
        slug = re.sub(r"[^a-z0-9\-]", "", slug)
        if slug == team_slug and team.league.upper() == league:
            return team

    return None


def team_to_mgm_constant(team: Team) -> str | None:
    """
    Given a Team, return its MGM_* constant name (e.g. 'MGM_MLB_LOS_ANGELES_DODGERS').
    """
    team_slug = f"{(team.city or '').lower().replace(' ', '-')}-{team.name.lower().replace(' ', '-')}"
    team_slug = re.sub(r"[^a-z0-9\-]", "", team_slug)  # sanitize
    MGM_LEAGUE_MAP = {
        "NFL": "MGM_NFL_TEAMS",
        "NBA": "MGM_NBA_TEAMS",
        "MLB": "MGM_MLB_TEAMS",
        "NHL": "MGM_NHL_TEAMS",
        "MLS": "MGM_MLS_TEAMS",
    }

    mgm_set_name = MGM_LEAGUE_MAP.get(team.league)
    if not mgm_set_name:
        return None

    # get the actual set object dynamically
    mgm_set = globals().get(mgm_set_name, set())

    if team_slug in mgm_set:
        # format to MGM_<LEAGUE>_<CITY>_<TEAM> in uppercase, underscores instead of hyphens
        return f"MGM_{team.league}_{team_slug.replace('-', '_').upper()}"
    return None
