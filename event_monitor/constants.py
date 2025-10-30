from __future__ import annotations

import enum

# Shared ANSI palette for structured logging
CYAN = "\033[96m"
"""ANSI escape code for cyan log output."""

RED = "\033[91m"
"""ANSI escape code for red log output."""

YELLOW = "\033[93m"
"""ANSI escape code for yellow log output."""

RESET = "\033[0m"
"""ANSI escape code that resets color formatting."""

# Persistence helpers
DATE_STAMP_FORMAT = "%Y%m%d"
"""Default format for timestamped artifact filenames."""

# Supported leagues
SUPPORTED_LEAGUES = ["nba", "nfl", "mlb"]
"""Canonical list of leagues supported across scrapers."""

# Game lifecycle defaults (seconds) - this is an upper bound on game time.
GAME_RUNTIME_SECONDS = 14400
"""Default event duration used to determine when games expire."""

# DraftKings configuration
DRAFTKINGS_WEBSOCKET_URL = "wss://sportsbook-ws-us-nj.draftkings.com/websocket?format=msgpack&locale=en"
"""Primary DraftKings websocket endpoint for odds streaming."""

# Bovada configuration
BOVADA_WEBSOCKET_URL = (
    "wss://services.bovada.lv/services/sports/subscription/71908712-7357-0123-5739-564427734220"
    "?X-Atmosphere-tracking-id=0&X-Atmosphere-Framework=3.1.0-javascript"
    "&X-Atmosphere-Transport=websocket"
)
"""Primary Bovada websocket endpoint for odds streaming."""

BOVADA_EVENT_LOG_INTERVAL = 500
"""Frequency for heartbeat logging of Bovada websocket events."""

BOVADA_HEARTBEAT_SECONDS = 60
"""Seconds between forced heartbeat logs for Bovada events."""

BOVADA_MAX_TIME_SINCE_LAST_EVENT = 60 * 30
"""Number of idle seconds tolerated before assuming a Bovada game ended."""

DRAFTKINGS_SPREAD_MARKET_TYPE = "Spread"
"""DraftKings market type identifier used when filtering spreads."""

DRAFTKINGS_DEFAULT_MONITOR_INTERVAL = 30
"""Seconds between monitor sweeps when driving the CLI loop."""

DRAFTKINGS_MAX_TIME_SINCE_LAST_EVENT = 60 * 10
"""Number of idle seconds tolerated before assuming a game ended."""

DRAFTKINGS_EVENT_LOG_INTERVAL = 500
"""Frequency for heartbeat logging of DraftKings websocket events."""

DRAFTKINGS_HEARTBEAT_SECONDS = 60
"""Seconds between forced heartbeat logs for DraftKings events."""

DRAFTKINGS_LEAGUE_URLS = {
    "nfl": "https://sportsbook.draftkings.com/leagues/football/nfl",
    "mlb": "https://sportsbook.draftkings.com/leagues/baseball/mlb",
    "nba": "https://sportsbook.draftkings.com/leagues/basketball/nba",
}
"""Landing pages used when scraping DraftKings event metadata."""

# MGM configuration
MGM_DEFAULT_MONITOR_INTERVAL = 1
"""Seconds between successive MGM page reloads while polling."""

MGM_MONITOR_SWEEP_INTERVAL = 30
"""Seconds between BetMGM monitor sweeps of stored event snapshots."""

MGM_EVENT_LOG_INTERVAL = 10
"""Number of captured snapshots between MGM heartbeat logs."""

MAX_IDLE_SECONDS = 300
"""Maximum idle time before polling terminates a session."""

MGM_HEARTBEAT_SECONDS = 60
"""Seconds between forced heartbeat logs for MGM events."""

MAX_RUNNER_ERRORS = 5
"""Maximum consecutive runner errors tolerated before stopping."""

MGM_LEAGUE_URLS = {
    "nfl": "https://www.co.betmgm.com/en/sports/football-11/today",
    "mlb": "https://www.co.betmgm.com/en/sports/baseball-23/today",
    "nba": "https://www.co.betmgm.com/en/sports/basketball-7/today",
}
"""League index pages used to collect MGM event slugs."""
MGM_NBA_TEAMS = {
    "atlanta-hawks",
    "boston-celtics",
    "brooklyn-nets",
    "charlotte-hornets",
    "chicago-bulls",
    "cleveland-cavaliers",
    "dallas-mavericks",
    "denver-nuggets",
    "detroit-pistons",
    "golden-state-warriors",
    "houston-rockets",
    "indiana-pacers",
    "los-angeles-clippers",
    "los-angeles-lakers",
    "memphis-grizzlies",
    "miami-heat",
    "milwaukee-bucks",
    "minnesota-timberwolves",
    "new-orleans-pelicans",
    "new-york-knicks",
    "oklahoma-city-thunder",
    "orlando-magic",
    "philadelphia-76ers",
    "phoenix-suns",
    "portland-trail-blazers",
    "sacramento-kings",
    "san-antonio-spurs",
    "toronto-raptors",
    "utah-jazz",
    "washington-wizards",
}
"""Lowercase MGM team slugs that flag NBA events during filtering."""

MGM_NFL_TEAMS = {
    "arizona-cardinals",
    "atlanta-falcons",
    "baltimore-ravens",
    "buffalo-bills",
    "carolina-panthers",
    "chicago-bears",
    "cincinnati-bengals",
    "cleveland-browns",
    "dallas-cowboys",
    "denver-broncos",
    "detroit-lions",
    "green-bay-packers",
    "houston-texans",
    "indianapolis-colts",
    "jacksonville-jaguars",
    "kansas-city-chiefs",
    "las-vegas-raiders",
    "los-angeles-chargers",
    "los-angeles-rams",
    "miami-dolphins",
    "minnesota-vikings",
    "new-england-patriots",
    "new-orleans-saints",
    "new-york-giants",
    "new-york-jets",
    "philadelphia-eagles",
    "pittsburgh-steelers",
    "san-francisco-49ers",
    "seattle-seahawks",
    "tampa-bay-buccaneers",
    "tennessee-titans",
    "washington-commanders",
}
"""Lowercase MGM team slugs that flag NFL events during filtering."""

MGM_MLB_TEAMS = {
    "arizona-diamondbacks",
    "atlanta-braves",
    "baltimore-orioles",
    "boston-red-sox",
    "chicago-cubs",
    "chicago-white-sox",
    "cincinnati-reds",
    "cleveland-guardians",
    "colorado-rockies",
    "detroit-tigers",
    "houston-astros",
    "kansas-city-royals",
    "los-angeles-angels",
    "los-angeles-dodgers",
    "miami-marlins",
    "milwaukee-brewers",
    "minnesota-twins",
    "new-york-mets",
    "new-york-yankees",
    "oakland-athletics",
    "philadelphia-phillies",
    "pittsburgh-pirates",
    "san-diego-padres",
    "san-francisco-giants",
    "seattle-mariners",
    "st-louis-cardinals",
    "tampa-bay-rays",
    "texas-rangers",
    "toronto-blue-jays",
    "washington-nationals",
}
"""Lowercase MGM team slugs that flag MLB events during filtering."""


class League(enum.Enum):
    NBA = "nba"
    MLB = "mlb"
    NFL = "nfl"


"""Sports leagues"""


class Provider(enum.Enum):
    DraftKings = "draftkings"
    BetMGM = "betmgm"
    FanDuel = "fanduel"
    Bovada = "bovada"


"""Sports data providers"""


SPORTS = {
    "nba": "basketball",
    "mlb": "baseball",
    "nfl": "football",
}
"""Sports"""
