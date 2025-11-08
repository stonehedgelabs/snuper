import datetime as dt
import json
import logging
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import httpx
import rapidfuzz

from snuper.config import get_config
from snuper.constants import RESET, DATE_STAMP_FORMAT
from snuper.t import Event, Team, SportdataGame


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


_SPORTDATA_RESOURCE_MAP: dict[str, tuple[str, str]] = {
    "nfl": ("football", "nfl"),
    "nba": ("basketball", "nba"),
    "mlb": ("baseball", "mlb"),
}


def _team_tokens_to_slug(team_tokens: tuple[str, str]) -> str:
    slug = "-".join(part for part in team_tokens if part)
    slug = re.sub(r"-+", "-", slug)
    return slug.strip("-")


@lru_cache(maxsize=None)
def _load_sportdata_team_maps(league: str) -> tuple[dict[str, str], dict[str, str]]:
    league_lower = league.lower()
    resource_dirs = _SPORTDATA_RESOURCE_MAP.get(league_lower)
    if resource_dirs is None:
        raise ValueError(f"Unsupported league for Sportdata lookup: {league}")

    root = Path(__file__).resolve().parent.parent
    teams_path = root / "resources" / "sports" / resource_dirs[0] / resource_dirs[1] / "teams.json"
    with teams_path.open("r", encoding="utf-8") as handle:
        entries: list[dict[str, Any]] = json.load(handle)

    slug_to_abbrev: dict[str, str] = {}
    abbrev_to_slug: dict[str, str] = {}
    for entry in entries:
        raw_id = entry.get("id")
        abbreviation = entry.get("abbreviation")
        if not raw_id or not abbreviation:
            continue
        slug = raw_id.split("--", 1)[-1].lower()
        abbr = str(abbreviation).upper()
        slug_to_abbrev[slug] = abbr
        abbrev_to_slug[abbr] = slug

    return slug_to_abbrev, abbrev_to_slug


def _sportdata_game_date(game: SportdataGame) -> dt.date:
    timestamp = game.date_time_utc or game.date_time or game.date
    normalized = timestamp.replace("Z", "+00:00")
    dt_obj = dt.datetime.fromisoformat(normalized)
    if dt_obj.tzinfo is None:
        dt_obj = dt_obj.replace(tzinfo=dt.timezone.utc)
    else:
        dt_obj = dt_obj.astimezone(dt.timezone.utc)
    return dt_obj.date()


def _match_sportdata_team_abbreviation(event_team_tokens: tuple[str, str], api_abbreviation: str, league: str) -> bool:
    """
    Match event team tokens to Sportdata team abbreviation.
    Returns True if the abbreviation matches.
    """
    _, abbrev_to_slug = _load_sportdata_team_maps(league)
    team_slug = _team_tokens_to_slug(event_team_tokens).lower()

    # Check if the abbreviation maps to the team slug
    expected_slug = abbrev_to_slug.get(api_abbreviation.upper())
    if expected_slug and expected_slug == team_slug:
        return True

    # Fallback: fuzzy match the abbreviation to the team tokens
    event_team_str = " ".join(event_team_tokens).lower()
    abbrev_str = api_abbreviation.lower()
    score = rapidfuzz.fuzz.ratio(event_team_str, abbrev_str)
    return score >= 70


def _fuzzy_match_team_name(event_team_tokens: tuple[str, str], api_team_name: str) -> bool:
    """
    Fuzzy match event team tokens to API team name.
    Returns True if the match score is above the threshold.
    """
    # Convert event team tokens to a searchable string
    event_team_str = " ".join(event_team_tokens).lower()
    api_team_str = api_team_name.lower()

    # Use rapidfuzz to get similarity score
    score = rapidfuzz.fuzz.ratio(event_team_str, api_team_str)
    # Use a threshold of 70 (same as used in game_label method)
    return score >= 70


async def match_rollinginsight_game(event: Event) -> None:
    """
    Fetch Rolling Insights schedule and match the event to a game.
    Sets event.rollinginsight_game to the matched game dict.
    Raises an error if zero or multiple matches are found.
    """
    logger = logging.getLogger(__name__)

    rsc_token = os.environ["ROLLING_INSIGHTS_CLIENT_SECRET"]
    current_date = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d")
    event_date = event.start_time.strftime("%Y-%m-%d")

    league_upper = event.league.upper()

    base_url = "https://rest.datafeeds.rolling-insights.com/api/v1"
    url = f"{base_url}/schedule/{current_date}/{league_upper}?RSC_token={rsc_token}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch Rolling Insights schedule for event %s: %s", event.event_id, e)
            raise RuntimeError(f"Failed to fetch Rolling Insights schedule: {e}") from e
        except Exception as e:
            logger.error(
                "Unexpected error while fetching Rolling Insights schedule for event %s: %s", event.event_id, e
            )
            raise RuntimeError(f"Failed to fetch Rolling Insights schedule: {e}") from e

    if "data" not in data:
        logger.error(
            "Invalid response format from Rolling Insights API: missing 'data' key for event %s", event.event_id
        )
        raise ValueError("Invalid response format: missing 'data' key")

    league_data = data["data"].get(league_upper, [])
    if not league_data:
        logger.warning(
            "No league data found for %s on %s (event %s: %s @ %s)",
            league_upper,
            current_date,
            event.event_id,
            event.away,
            event.home,
        )

    # Filter games by date and fuzzy match teams
    matches = []
    for game in league_data:
        # Extract date from game_time (format: "Sun, 09 Nov 2025 01:00:00 GMT")
        # Parse the game_time to extract the date
        try:
            game_time_str = game["game_time"]
            if game_time_str:
                # Parse GMT time and convert to UTC date
                game_dt = dt.datetime.strptime(game_time_str, "%a, %d %b %Y %H:%M:%S %Z")
                game_date = game_dt.strftime("%Y-%m-%d")
            else:
                # Fallback: try to extract date from game_ID if available
                game_id = game["game_ID"]
                if game_id and len(game_id) >= 8:
                    # game_ID format: "20251108-12-5", first 8 chars are date
                    game_date = f"{game_id[:4]}-{game_id[4:6]}-{game_id[6:8]}"
                else:
                    continue
        except (ValueError, KeyError) as e:
            logger.warning(
                "Failed to parse game date for game in league %s (event %s): %s",
                league_upper,
                event.event_id,
                e,
            )
            continue
        except Exception as e:
            logger.error(
                "Unexpected error parsing game date for game in league %s (event %s): %s",
                league_upper,
                event.event_id,
                e,
            )
            continue

        # Check if date matches
        if game_date != event_date:
            continue

        # Fuzzy match teams
        home_team = game["home_team"]
        away_team = game["away_team"]

        home_matches = _fuzzy_match_team_name(event.home, home_team)
        away_matches = _fuzzy_match_team_name(event.away, away_team)

        if home_matches and away_matches:
            matches.append(game)

    # Validate we have exactly one match
    if len(matches) == 0:
        logger.warning(
            "No matching game found for event %s (%s @ %s) on %s",
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise ValueError(
            f"No matching game found for event {event.event_id} " f"({event.away} @ {event.home}) on {event_date}"
        )
    if len(matches) > 1:
        logger.error(
            "Multiple matching games found (%d) for event %s (%s @ %s) on %s",
            len(matches),
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise ValueError(
            f"Multiple matching games found ({len(matches)}) for event {event.event_id} "
            f"({event.away} @ {event.home}) on {event_date}"
        )

    # Set the matched game
    event.set_rollinginsight_game(matches[0])


async def match_sportdata_game(event: Event) -> None:
    """
    Fetch Sportdata schedule and match the event to a game.
    Sets event.sportdata_game to the matched game dict.
    Raises an error if zero or multiple matches are found.
    """
    logger = logging.getLogger(__name__)

    # Get API key from environment
    api_key = os.environ.get("SPORTSDATAIO_API_KEY")
    if not api_key:
        raise ValueError("SPORTSDATAIO_API_KEY environment variable is not set")

    # Get config to access season information
    try:
        config = get_config()
    except RuntimeError:
        logger.error("Configuration not loaded. Please provide --config argument.")
        raise ValueError("Configuration must be loaded to match Sportdata games") from None

    # Get season from config based on event league
    try:
        season_info = config.seasons.get_season(event.league)
        season = season_info.regular
    except KeyError as e:
        logger.error("No season configuration found for league %s (event %s)", event.league, event.event_id)
        raise ValueError(f"No season configuration found for league {event.league}") from e

    # Get event date in YYYY-MM-DD format for filtering
    event_date = event.start_time.strftime("%Y-%m-%d")

    # Build the API URL
    base_url = config.api.sportsdata_base_url
    league_lower = event.league.lower()
    url = f"{base_url}/{league_lower}/scores/json/SchedulesBasic/{season}?key={api_key}"

    # Fetch the schedule
    async with httpx.AsyncClient(timeout=config.api.request_timeout) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch Sportdata schedule for event %s: %s", event.event_id, e)
            raise RuntimeError(f"Failed to fetch Sportdata schedule: {e}") from e
        except Exception as e:
            logger.error("Unexpected error while fetching Sportdata schedule for event %s: %s", event.event_id, e)
            raise RuntimeError(f"Failed to fetch Sportdata schedule: {e}") from e

    if not isinstance(data, list):
        logger.error("Invalid response format from Sportdata API: expected list for event %s", event.event_id)
        raise ValueError("Invalid response format: expected list")

    if not data:
        logger.warning(
            "No games found in Sportdata schedule for league %s season %s (event %s)",
            league_lower,
            season,
            event.event_id,
        )

    # Filter games by date and status, then match teams
    matches = []
    for game in data:
        # Filter by status - only "Scheduled" games
        status = game["Status"]
        if status != "Scheduled":
            continue

        # Extract date from DateTimeUTC
        try:
            date_time_utc = game["DateTimeUTC"]
            # Parse the datetime and extract date
            game_dt = dt.datetime.fromisoformat(date_time_utc.replace("Z", "+00:00"))
            if game_dt.tzinfo is None:
                game_dt = game_dt.replace(tzinfo=dt.timezone.utc)
            else:
                game_dt = game_dt.astimezone(dt.timezone.utc)
            game_date = game_dt.strftime("%Y-%m-%d")
        except (ValueError, KeyError) as e:
            logger.warning(
                "Failed to parse game date for game in league %s (event %s): %s",
                league_lower,
                event.event_id,
                e,
            )
            continue
        except Exception as e:
            logger.error(
                "Unexpected error parsing game date for game in league %s (event %s): %s",
                league_lower,
                event.event_id,
                e,
            )
            continue

        # Check if date matches
        if game_date != event_date:
            continue

        # Match teams using abbreviations
        home_team_abbrev = game["HomeTeam"]
        away_team_abbrev = game["AwayTeam"]

        home_matches = _match_sportdata_team_abbreviation(event.home, home_team_abbrev, event.league)
        away_matches = _match_sportdata_team_abbreviation(event.away, away_team_abbrev, event.league)

        if home_matches and away_matches:
            matches.append(game)

    # Validate we have exactly one match
    if len(matches) == 0:
        logger.warning(
            "No matching game found for event %s (%s @ %s) on %s",
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise ValueError(
            f"No matching game found for event {event.event_id} " f"({event.away} @ {event.home}) on {event_date}"
        )
    if len(matches) > 1:
        logger.error(
            "Multiple matching games found (%d) for event %s (%s @ %s) on %s",
            len(matches),
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise ValueError(
            f"Multiple matching games found ({len(matches)}) for event {event.event_id} "
            f"({event.away} @ {event.home}) on {event_date}"
        )

    # Set the matched game
    event.sportdata_game = matches[0]
