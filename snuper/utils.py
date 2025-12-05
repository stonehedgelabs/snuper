"""Utility functions for event handling, team matching, logging, and data formatting.

This module provides helper functions for:
- File path generation for event snapshots and odds logs
- Odds format conversion (decimal to American)
- Event data loading and serialization
- Colored logging configuration
- Duration and byte size formatting
- Team name fuzzy matching for sportdata and Rolling Insights APIs
"""

import asyncio
import datetime as dt
import json
import logging
import os
import re
from pathlib import Path

import httpx
import rapidfuzz
from tzlocal import get_localzone

from snuper.config import get_config
from snuper.constants import RESET, DATE_STAMP_FORMAT, League
from snuper.t import Event, Team, SportdataGame, RollingInsightsGame  # pylint: disable=unused-import

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


# NBA team abbreviation to full name mapping.
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

# NFL team abbreviation to full name mapping.
NFL_TEAMS_BY_ABBREV = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LV": "Las Vegas Raiders",
    "LAC": "Los Angeles Chargers",
    "LAR": "Los Angeles Rams",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SF": "San Francisco 49ers",
    "SEA": "Seattle Seahawks",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WAS": "Washington Commanders",
}

# MLB team abbreviation to full name mapping.
MLB_TEAMS_BY_ABBREV = {
    "ARI": "Arizona Diamondbacks",
    "ATL": "Atlanta Braves",
    "BAL": "Baltimore Orioles",
    "BOS": "Boston Red Sox",
    "CHC": "Chicago Cubs",
    "CHW": "Chicago White Sox",
    "CIN": "Cincinnati Reds",
    "CLE": "Cleveland Guardians",
    "COL": "Colorado Rockies",
    "DET": "Detroit Tigers",
    "HOU": "Houston Astros",
    "KC": "Kansas City Royals",
    "LAA": "Los Angeles Angels",
    "LAD": "Los Angeles Dodgers",
    "MIA": "Miami Marlins",
    "MIL": "Milwaukee Brewers",
    "MIN": "Minnesota Twins",
    "NYM": "New York Mets",
    "NYY": "New York Yankees",
    "OAK": "Oakland Athletics",
    "PHI": "Philadelphia Phillies",
    "PIT": "Pittsburgh Pirates",
    "SD": "San Diego Padres",
    "SF": "San Francisco Giants",
    "SEA": "Seattle Mariners",
    "STL": "St. Louis Cardinals",
    "TB": "Tampa Bay Rays",
    "TEX": "Texas Rangers",
    "TOR": "Toronto Blue Jays",
    "WSH": "Washington Nationals",
}


def _get_team_abbreviation_from_tokens(event_team_tokens: tuple[str, ...], league: str) -> str | None:
    """
    Convert event team tokens to SportData abbreviation.
    For NBA: Uses fuzzy matching against NBA_TEAMS_BY_ABBREV values to find the abbreviation.
    Returns the abbreviation (e.g., "DAL", "WAS") or None if no match found.
    """
    teams = NBA_TEAMS_BY_ABBREV
    if league.lower() == League.MLB.value:
        teams = MLB_TEAMS_BY_ABBREV
    if league.lower() == League.NFL.value:
        teams = NFL_TEAMS_BY_ABBREV

    event_team_str = " ".join(event_team_tokens).lower()

    best_abbrev = None
    best_score = 0

    for abbrev, team_name in teams.items():
        score = rapidfuzz.fuzz.ratio(event_team_str, team_name.lower())
        if score > best_score:
            best_score = score
            best_abbrev = abbrev

    logger.debug("_get_team_abbreviation_from_tokens: %s %s", best_score, best_abbrev)

    if best_score >= 55:
        # print(best_score, best_abbrev, event_team_tokens)
        return best_abbrev

    return None


def _match_sportdata_team_abbreviation(event_team_tokens: tuple[str, ...], api_abbreviation: str, league: str) -> bool:
    """
    Match event team tokens to sportdata team abbreviation.
    Returns True if the abbreviation matches.
    """
    expected_abbrev = _get_team_abbreviation_from_tokens(event_team_tokens, league)

    logger.debug("_match_sportdata_team_abbreviation: %s %s", expected_abbrev, api_abbreviation)

    # print(expected_abbrev, api_abbreviation)

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
    """Fuzzy match event team tokens to API team name.

    Returns True if the match score is above the threshold.

    First tries matching the full team name (e.g., "no pelicans" vs "New Orleans Pelicans"),
    then falls back to mascot-only matching for cases with multi-word mascots.

    Optimized to avoid repeated normalization calls.
    """

    def normalize(s: str) -> str:
        return re.sub(r"\s+", " ", s.lower().replace("-", " ").strip())

    # Cache normalized values to avoid repeated computation.
    api_team_normalized = normalize(api_team_name)
    event_team_str = normalize(" ".join(event_team_tokens))

    # Try full team name matching first (two different scoring methods).
    full_score = rapidfuzz.fuzz.token_sort_ratio(event_team_str, api_team_normalized)
    if full_score >= 60:
        return True

    full_score_ratio = rapidfuzz.fuzz.ratio(event_team_str, api_team_normalized)
    if full_score_ratio >= 60:
        return True

    # Fall back to mascot matching - pre-compute normalized mascot.
    api_mascot = _extract_mascot_from_team_name(api_team_name)
    api_mascot_normalized = normalize(api_mascot)

    # Pre-compute all possible event mascot variations to avoid repeated work.
    max_words = min(3, len(event_team_tokens))
    event_mascot_variants = [normalize(" ".join(event_team_tokens[-n:])) for n in range(1, max_words + 1)]

    # Check all mascot variants against the API mascot.
    for event_mascot in event_mascot_variants:
        score = rapidfuzz.fuzz.ratio(event_mascot, api_mascot_normalized)
        if score >= 60:
            len_diff = abs(len(event_mascot) - len(api_mascot_normalized))
            if len_diff >= 3:
                continue

            return True

    return False


async def fetch_rollinginsights_schedule(league: str, max_retries: int = 3) -> dict[str, dict[str, list[dict]]]:
    """
    Fetch Rolling Insights schedule for a league and date with retry logic.
    Returns the full API response data dict with structure: {"data": {"LEAGUE": [games...]}}.
    """
    rsc_token = os.environ["ROLLING_INSIGHTS_CLIENT_SECRET"]

    local_tz = get_localzone()
    today_local = dt.datetime.now(local_tz).date()
    current_date = today_local.strftime("%Y-%m-%d")

    league_upper = league.upper()

    base_url = "https://rest.datafeeds.rolling-insights.com/api/v1"
    url = f"{base_url}/schedule/{current_date}/{league_upper}?RSC_token={rsc_token}"

    # Add headers to prevent caching and force fresh response
    headers = {
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    }

    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                logger.info(
                    "Fetching Rolling Insights schedule for %s (attempt %d/%d): %s",
                    league,
                    attempt + 1,
                    max_retries,
                    url.replace(rsc_token, "***"),
                )
                response = await client.get(url, headers=headers)

                # Handle 304 Not Modified by retrying with exponential backoff
                # 304 means "not modified" but we don't have a cache, so we retry to force a fresh response
                if response.status_code == 304:
                    if attempt < max_retries - 1:
                        wait_time = 2**attempt
                        logger.warning(
                            "Server returned 304 Not Modified for %s despite cache-busting headers. "
                            "Retrying in %ds to force fresh response (attempt %d/%d)...",
                            league,
                            wait_time,
                            attempt + 1,
                            max_retries,
                        )
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(
                            "Failed to fetch Rolling Insights schedule for %s after %d attempts: "
                            "Server consistently returning 304 Not Modified. URL: %s",
                            league,
                            max_retries,
                            url.replace(rsc_token, "***"),
                        )
                        raise Exception(f"Server returned 304 Not Modified for {league} after {max_retries} attempts")

                # Now check for errors (4xx, 5xx)
                response.raise_for_status()
                data = response.json()

                if "data" not in data:
                    logger.error(
                        "Invalid response format from Rolling Insights API: missing 'data' key for league %s",
                        league,
                    )
                    raise Exception(f"Invalid response format for {league}")

                logger.info("Successfully fetched Rolling Insights schedule for %s", league)
                return data

        except httpx.ReadTimeout:
            if attempt < max_retries - 1:
                wait_time = 2**attempt
                logger.warning(
                    "ReadTimeout fetching Rolling Insights schedule for %s (attempt %d/%d). "
                    "Server took longer than 60s to respond. Retrying in %ds...",
                    league,
                    attempt + 1,
                    max_retries,
                    wait_time,
                )
                await asyncio.sleep(wait_time)
            else:
                logger.error(
                    "Failed to fetch Rolling Insights schedule for %s after %d attempts: "
                    "Server consistently timing out (>60s). URL: %s",
                    league,
                    max_retries,
                    url.replace(rsc_token, "***"),
                )
                raise Exception(f"ReadTimeout for {league} after {max_retries} attempts")
        except httpx.HTTPStatusError as e:
            logger.error(
                "Failed to fetch Rolling Insights schedule for %s: HTTP %d - %s. URL: %s",
                league,
                e.response.status_code,
                e.response.text,
                url.replace(rsc_token, "***"),
            )
            raise Exception(f"HTTP {e.response.status_code} for {league}") from e
        except httpx.HTTPError as e:
            logger.error(
                "Failed to fetch Rolling Insights schedule for %s: Network error: %s: %s. URL: %s",
                league,
                type(e).__name__,
                e,
                url.replace(rsc_token, "***"),
            )
            raise Exception(f"Network error for {league}: {type(e).__name__}") from e
        except Exception as e:
            logger.error(
                "Unexpected error while fetching Rolling Insights schedule for %s: %s: %s. URL: %s",
                league,
                type(e).__name__,
                e,
                url.replace(rsc_token, "***"),
            )
            raise

    # Should never reach here
    logger.error("Failed to fetch Rolling Insights schedule for %s after %d attempts", league, max_retries)
    raise Exception(f"Failed after {max_retries} attempts for {league}")


async def match_rollinginsight_game(event: Event, schedule_data: dict[str, dict[str, list[dict]]]) -> None:
    """
    Match the event to a game from the provided Rolling Insights schedule data.
    Sets event.rollinginsight_game to the matched game dict.
    Raises an error if zero or multiple matches are found.

    Args:
        event: The event to match
        schedule_data: API response with structure {"data": {"LEAGUE": [games...]}}
    """

    local_tz = get_localzone()
    event_start_local = event.start_time.astimezone(local_tz)
    event_date = event_start_local.strftime("%Y-%m-%d")

    league_upper = event.league.upper()

    league_data = schedule_data["data"].get(league_upper, [])
    if not league_data:
        logger.warning(
            "No league data found for %s on %s (event %s: %s @ %s)",
            league_upper,
            event_date,
            event.event_id,
            event.away,
            event.home,
        )

    matches = []
    for game in league_data:
        # _rollinginsights_game = RollingInsightsGame.from_dict(game)
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
                "Failed to parse game date for RollingInsight game in league %s (event %s): %s",
                league_upper,
                event.event_id,
                e,
            )
            continue
        except Exception as e:
            logger.error(
                "Unexpected error parsing game date for RollingInsight game in league %s (event %s): %s",
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
        logger.error(
            "No matching RollingInsights %s game found for event %s (%s @ %s) on %s",
            event.league,
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise Exception(f"No matching RollingInsights game for event {event.event_id}")
    if len(matches) > 1:
        logger.error(
            "Multiple matching RollingInsights %s games found (%d) for event %s (%s @ %s) on %s",
            event.league,
            len(matches),
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise Exception(f"Multiple matching RollingInsights games ({len(matches)}) for event {event.event_id}")

    event.set_rollinginsight_game(matches[0])


async def match_sportdata_game(event: Event) -> None:
    api_key = os.environ.get("SPORTSDATAIO_API_KEY")
    if not api_key:
        raise ValueError("SPORTSDATAIO_API_KEY environment variable is not set")

    try:
        config = get_config()
    except RuntimeError:
        logger.error("Configuration not loaded. Please provide --config argument.")
        raise Exception("Configuration not loaded")

    try:
        season_info = config.seasons.get_season(event.league)
        season = season_info.regular
    except KeyError:
        logger.error("No season configuration found for league %s (event %s)", event.league, event.event_id)
        raise Exception(f"No season config for {event.league}")

    local_tz = get_localzone()
    event_date = event.start_time.astimezone(local_tz).strftime("%Y-%m-%d")

    base_url = config.api.sportsdata_base_url
    league_lower = event.league.lower()
    url = f"{base_url}/{league_lower}/scores/json/SchedulesBasic/{season}?key={api_key}"

    async with httpx.AsyncClient(timeout=config.api.request_timeout) as client:
        try:
            response = await client.get(url)
            response.raise_for_status()
            data = response.json()
        except httpx.HTTPError as e:
            logger.error("Failed to fetch sportdata schedule for event %s: %s", event.event_id, e)
            raise Exception(f"Failed to fetch sportdata schedule for event {event.event_id}") from e
        except Exception as e:
            logger.error("Unexpected error while fetching sportdata schedule for event %s: %s", event.event_id, e)
            raise

    if not isinstance(data, list):
        logger.error("Invalid response format from sportdata API: expected list for event %s", event.event_id)
        raise Exception(f"Invalid sportdata response format for event {event.event_id}")

    matches = []
    scheduled_games_count = sum(1 for g in data if g.get("Status") == "Scheduled")
    logger.debug(
        "event %s: searching %d scheduled games (out of %d total) for date %s",
        event.event_id,
        scheduled_games_count,
        len(data),
        event_date,
    )

    now = dt.datetime.now(local_tz)
    start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    end_of_day = start_of_day + dt.timedelta(days=1)

    # Team abbreviation remappings - defined once outside loop for efficiency.
    nba_remappings = {"NO": "NOP", "SA": "SAS", "NY": "NYK", "GS": "GSW", "PHO": "PHX"}

    for game in data:
        status = game.get("Status")
        if not status or status in ("Final", "F/OT", "Canceled", "Cancelled", "Suspended"):
            continue

        try:
            date_time_utc = game["DateTimeUTC"]
            game_dt = dt.datetime.fromisoformat(date_time_utc.replace("Z", "+00:00"))
            if game_dt.tzinfo is None:
                game_dt = game_dt.replace(tzinfo=dt.timezone.utc)
            game_local = game_dt.astimezone(local_tz)
        except Exception as e:
            logger.error(
                "Failed to parse game date for sportdata game in league %s (event %s): %s",
                league_lower,
                event.event_id,
                e,
            )
            raise Exception(f"Failed to parse sportdata game date for event {event.event_id}") from e

        if not start_of_day <= game_local < end_of_day:
            continue

        home_team_abbrev = game["HomeTeam"]
        away_team_abbrev = game["AwayTeam"]
        if league_lower == League.NBA.value:
            home_team_abbrev = nba_remappings.get(game["HomeTeam"], game["HomeTeam"])
            away_team_abbrev = nba_remappings.get(game["AwayTeam"], game["AwayTeam"])

        home_matches = _match_sportdata_team_abbreviation(event.home, home_team_abbrev, event.league)
        away_matches = _match_sportdata_team_abbreviation(event.away, away_team_abbrev, event.league)

        if home_matches and away_matches:
            matches.append(game)

    if len(matches) == 0:
        logger.error(
            "No matching sportdata %s game found for event %s (%s @ %s) on %s",
            event.league,
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise Exception(f"No matching sportdata game for event {event.event_id}")

    if len(matches) > 1:
        logger.error(
            "Multiple matching sportdata %s games found (%d) for event %s (%s @ %s) on %s",
            event.league,
            len(matches),
            event.event_id,
            event.away,
            event.home,
            event_date,
        )
        raise Exception(f"Multiple matching sportdata games ({len(matches)}) for event {event.event_id}")

    event.sportdata_game = matches[0]
