import datetime as dt
import json
import logging
import sys
import os
import re
from pathlib import Path

import httpx
import rapidfuzz
from tzlocal import get_localzone

from snuper.config import get_config
from snuper.constants import RESET, DATE_STAMP_FORMAT
from snuper.t import Event, Team, SportdataGame, RollingInsightsGame

logger = logging.getLogger(__name__)


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

    colored_logger = logging.getLogger(name)
    existing: str | None = getattr(colored_logger, "_color_prefix", None)
    if existing == color:
        return colored_logger

    filt: _ColorPrefixFilter | None = getattr(colored_logger, "_color_filter", None)
    if filt is None:
        filt = _ColorPrefixFilter(color)
        colored_logger.addFilter(filt)
        setattr(colored_logger, "_color_filter", filt)
    else:
        filt.color = color

    setattr(colored_logger, "_color_prefix", color)
    return colored_logger


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
    team_slug = re.sub(r"[^a-z0-9\-]", "", team_slug)
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

    mgm_set = globals().get(mgm_set_name, set())

    if team_slug in mgm_set:
        return f"MGM_{team.league}_{team_slug.replace('-', '_').upper()}"
    return None


NBA_TEAMS_BY_ABBREV = {
    "ATL": "Atlanta Hawks",
    "BOS": "Boston Celtics",
    "BKN": "Brooklyn Nets",
    "CHA": "Charlotte Hornets",
    "CHI": "Chicago Bulls",
    "CLE": "Cleveland Cavaliers",
    "DAL": "Dallas Mavericks",
    "DEN": "Denver Nuggets",
    "DET": "Detroit Pistons",
    "GSW": "Golden State Warriors",
    "HOU": "Houston Rockets",
    "IND": "Indiana Pacers",
    "LAC": "Los Angeles Clippers",
    "LAL": "Los Angeles Lakers",
    "MEM": "Memphis Grizzlies",
    "MIA": "Miami Heat",
    "MIL": "Milwaukee Bucks",
    "MIN": "Minnesota Timberwolves",
    "NOP": "New Orleans Pelicans",
    "NYK": "New York Knicks",
    "OKC": "Oklahoma City Thunder",
    "ORL": "Orlando Magic",
    "PHI": "Philadelphia 76ers",
    "PHX": "Phoenix Suns",
    "POR": "Portland Trail Blazers",
    "SAC": "Sacramento Kings",
    "SAS": "San Antonio Spurs",
    "TOR": "Toronto Raptors",
    "UTA": "Utah Jazz",
    "WAS": "Washington Wizards",
}


def _get_team_abbreviation_from_tokens(event_team_tokens: tuple[str, ...], league: str) -> str | None:
    """
    Convert event team tokens to SportData abbreviation.
    For NBA: Uses fuzzy matching against NBA_TEAMS_BY_ABBREV values to find the abbreviation.
    Returns the abbreviation (e.g., "DAL", "WAS") or None if no match found.
    """
    if league.lower() != "nba":
        return None

    event_team_str = " ".join(event_team_tokens).lower()

    best_abbrev = None
    best_score = 0

    for abbrev, team_name in NBA_TEAMS_BY_ABBREV.items():
        score = rapidfuzz.fuzz.ratio(event_team_str, team_name.lower())
        if score > best_score:
            best_score = score
            best_abbrev = abbrev

    if best_score >= 60:
        return best_abbrev

    return None


def _match_sportdata_team_abbreviation(event_team_tokens: tuple[str, ...], api_abbreviation: str, league: str) -> bool:
    """
    Match event team tokens to Sportdata team abbreviation.
    Returns True if the abbreviation matches.
    """
    expected_abbrev = _get_team_abbreviation_from_tokens(event_team_tokens, league)

    return expected_abbrev is not None and expected_abbrev.upper() == api_abbreviation.upper()


def _extract_mascot_from_tokens(team_tokens: tuple[str, ...]) -> str:
    """
    Extract the mascot (team name) from event team tokens.

    The mascot is always the last element(s) of the tuple:
    - ('phoenix', 'suns') -> 'suns'
    - ('los', 'angeles', 'lakers') -> 'lakers'
    - ('golden', 'state', 'warriors') -> 'warriors'
    - ('portland', 'trail-blazers') -> 'trail-blazers'
    """
    if not team_tokens:
        return ""
    return team_tokens[-1].lower()


def _extract_mascot_from_team_name(team_name: str) -> str:
    """
    Extract the mascot from a full team name.

    Examples:
    - 'Phoenix Suns' -> 'suns'
    - 'Los Angeles Lakers' -> 'lakers'
    - 'Golden State Warriors' -> 'warriors'
    - 'Portland Trail Blazers' -> 'trail blazers'
    """
    words = team_name.lower().split()
    if not words:
        return ""

    if len(words) >= 2 and len(words[-1]) <= 2:  # pylint: disable=chained-comparison
        return " ".join(words[-2:])

    two_word_mascots = {"trail blazers", "thunder hawks", "timberwolves"}
    last_two = " ".join(words[-2:]) if len(words) >= 2 else ""
    if last_two in two_word_mascots:
        return last_two

    return words[-1]


def _fuzzy_match_team_name(event_team_tokens: tuple[str, ...], api_team_name: str) -> bool:
    """
    Fuzzy match event team tokens to API team name.
    Returns True if the match score is above the threshold.

    First tries matching the full team name (e.g., "no pelicans" vs "New Orleans Pelicans"),
    then falls back to mascot-only matching for cases with multi-word mascots.
    """

    def normalize(s: str) -> str:
        return re.sub(r"\s+", " ", s.lower().replace("-", " ").strip())

    api_team_normalized = normalize(api_team_name)

    event_team_str = normalize(" ".join(event_team_tokens))

    full_score = rapidfuzz.fuzz.token_sort_ratio(event_team_str, api_team_normalized)

    if full_score >= 60:
        return True

    full_score_ratio = rapidfuzz.fuzz.ratio(event_team_str, api_team_normalized)
    if full_score_ratio >= 60:
        return True

    api_mascot = _extract_mascot_from_team_name(api_team_name)
    api_mascot_normalized = normalize(api_mascot)

    max_words = min(3, len(event_team_tokens))

    for n in range(1, max_words + 1):
        event_mascot_words = event_team_tokens[-n:]
        event_mascot = normalize(" ".join(event_mascot_words))

        score = rapidfuzz.fuzz.ratio(event_mascot, api_mascot_normalized)

        if score >= 60:
            return True

    return False


async def match_rollinginsight_game(event: Event) -> None:
    """
    Fetch Rolling Insights schedule and match the event to a game.
    Sets event.rollinginsight_game to the matched game dict.
    Raises an error if zero or multiple matches are found.
    """

    rsc_token = os.environ["ROLLING_INSIGHTS_CLIENT_SECRET"]

    local_tz = get_localzone()
    logger.info("RollingInsights matcher using local timezone: %s", local_tz)
    today_local = dt.datetime.now(local_tz).date()
    current_date = today_local.strftime("%Y-%m-%d")

    event_start_local = event.start_time.astimezone(local_tz)
    event_date = event_start_local.strftime("%Y-%m-%d")

    league_upper = event.league.upper()

    base_url = "https://rest.datafeeds.rolling-insights.com/api/v1"
    url = f"{base_url}/schedule/{current_date}/{league_upper}?RSC_token={rsc_token}"

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            _rollinginsights_game = RollingInsightsGame.from_dict(data)
        except httpx.HTTPError as e:
            logger.error("Failed to fetch Rolling Insights schedule for event %s: %s", event.event_id, e)
            raise RuntimeError("Failed to fetch Rolling Insights schedule: %s" % e) from e
        except Exception as e:
            logger.error(
                "Unexpected error while fetching Rolling Insights schedule for event %s: %s", event.event_id, e
            )
            raise RuntimeError("Failed to fetch Rolling Insights schedule: %s" % e) from e

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

    matches = []
    for game in league_data:
        home_team = game["home_team"]
        away_team = game["away_team"]
        try:
            game_time_str = game["game_time"]
            if game_time_str:
                game_dt_gmt = dt.datetime.strptime(game_time_str, "%a, %d %b %Y %H:%M:%S %Z")
                game_dt_utc = game_dt_gmt.replace(tzinfo=dt.timezone.utc)
                game_dt_local = game_dt_utc.astimezone(local_tz)
                game_date = game_dt_local.strftime("%Y-%m-%d")
            else:
                game_id = game["game_ID"]
                if game_id and len(game_id) >= 8:
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

        if game_date != event_date:
            continue

        home_matches = _fuzzy_match_team_name(event.home, home_team)
        away_matches = _fuzzy_match_team_name(event.away, away_team)

        if home_matches and away_matches:
            matches.append(game)

    if len(matches) == 0:
        logger.warning(
            "No matching RollingInsights %s game found for event %s (%s @ %s) on %s -- %s",
            event.league,
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        sys.exit(
            "No matching RollingInsights game found for event %s (%s @ %s) on %s\n%s"
            % (event.event_id, event.away, event.home, event_date, event.to_dict())
        )
    if len(matches) > 1:
        logger.error(
            "Multiple matching %s games found (%d) for event %s (%s @ %s) on %s",
            event.league,
            len(matches),
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise ValueError(
            "Multiple matching %s games found (%d) for event %s (%s @ %s) on %s"
            % (event.league, len(matches), event.event_id, event.away, event.home, event_date)
        )

    event.set_rollinginsight_game(matches[0])


async def match_sportdata_game(event: Event) -> None:
    """
    Fetch Sportdata schedule and match the event to a game.
    Sets event.sportdata_game to the matched game dict.
    Raises an error if zero or multiple matches are found.
    """

    api_key = os.environ.get("SPORTSDATAIO_API_KEY")
    if not api_key:
        raise ValueError("SPORTSDATAIO_API_KEY environment variable is not set")

    try:
        config = get_config()
    except RuntimeError:
        logger.error("Configuration not loaded. Please provide --config argument.")
        raise ValueError("Configuration must be loaded to match Sportdata games") from None

    try:
        season_info = config.seasons.get_season(event.league)
        season = season_info.regular
    except KeyError as e:
        logger.error("No season configuration found for league %s (event %s)", event.league, event.event_id)
        raise ValueError("No season configuration found for league %s" % event.league) from e

    event_date = event.start_time.strftime("%Y-%m-%d")

    base_url = config.api.sportsdata_base_url
    league_lower = event.league.lower()
    url = f"{base_url}/{league_lower}/scores/json/SchedulesBasic/{season}?key={api_key}"

    async with httpx.AsyncClient(timeout=config.api.request_timeout) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
            _sportdata_game = SportdataGame.from_dict(data)
        except httpx.HTTPError as e:
            logger.error("Failed to fetch Sportdata schedule for event %s: %s", event.event_id, e)
            raise RuntimeError("Failed to fetch Sportdata schedule: %s" % e) from e
        except Exception as e:
            logger.error("Unexpected error while fetching Sportdata schedule for event %s: %s", event.event_id, e)
            raise RuntimeError("Failed to fetch Sportdata schedule: %s" % e) from e

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

    matches = []
    scheduled_games_count = sum(1 for g in data if g.get("Status") == "Scheduled")
    logger.info(
        "Event %s: Searching %d scheduled games (out of %d total) for date %s",
        event.event_id,
        scheduled_games_count,
        len(data),
        event_date,
    )

    for game in data:

        status = game["Status"]
        if status in ("Final", "F/OT", "Canceled", "Cancelled", "Suspended"):
            continue

        try:
            date_time_utc = game["DateTimeUTC"]
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

        if game_date != event_date:
            continue

        remappings = {"NO": "NOP", "SA": "SAS", "NY": "NYK", "GS": "GSW", "PHO": "PHX"}

        home_team_abbrev = game["HomeTeam"]
        home_team_abbrev = remappings.get(home_team_abbrev, home_team_abbrev)

        away_team_abbrev = game["AwayTeam"]
        away_team_abbrev = remappings.get(away_team_abbrev, away_team_abbrev)

        home_matches = _match_sportdata_team_abbreviation(event.home, home_team_abbrev, event.league)
        away_matches = _match_sportdata_team_abbreviation(event.away, away_team_abbrev, event.league)

        if home_matches and away_matches:
            matches.append(game)

    if len(matches) == 0:
        logger.warning(
            "No matching Sportdata %s game found for event %s (%s @ %s) on %s",
            event.league,
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        sys.exit(
            "No matching Sportdata %s game found for event %s (%s @ %s) on %s"
            % (event.league, event.event_id, event.away, event.home, event_date)
        )
    if len(matches) > 1:
        logger.error(
            "Multiple matching %s games found (%d) for event %s (%s @ %s) on %s",
            event.league,
            len(matches),
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise ValueError(
            "Multiple matching %s games found (%d) for event %s (%s @ %s) on %s"
            % (event.league, len(matches), event.event_id, event.away, event.home, event_date)
        )

    event.sportdata_game = matches[0]
