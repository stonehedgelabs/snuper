from __future__ import annotations

# Shared ANSI palette for structured logging
CYAN = "\033[96m"
"""ANSI escape code for cyan log output."""

RED = "\033[91m"
"""ANSI escape code for red log output."""

RESET = "\033[0m"
"""ANSI escape code that resets color formatting."""

# Persistence helpers
DATE_STAMP_FORMAT = "%Y%m%d"
"""Default format for timestamped artifact filenames."""

# Supported leagues
SUPPORTED_LEAGUES = ["nba", "nfl", "mlb"]
"""Canonical list of leagues supported across scrapers."""

# Game lifecycle defaults (seconds)
GAME_RUNTIME_SECONDS = 5 * 3600
"""Default event duration used to determine when games expire."""

# DraftKings configuration
DRAFTKINGS_WEBSOCKET_URL = "wss://sportsbook-ws-us-nj.draftkings.com/websocket?format=msgpack&locale=en"
"""Primary DraftKings websocket endpoint for odds streaming."""

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

MGM_EVENT_LOG_INTERVAL = 10
"""Number of captured snapshots between MGM heartbeat logs."""

MGM_MAX_IDLE_SECONDS = 120
"""Maximum idle time before MGM polling terminates a session."""

MGM_HEARTBEAT_SECONDS = 60
"""Seconds between forced heartbeat logs for MGM events."""

MGM_LEAGUE_URLS = {
    "nfl": "https://www.co.betmgm.com/en/sports/football-11/betting/usa-9/nfl-35",
    "mlb": "https://www.co.betmgm.com/en/sports/baseball-23/betting/usa-9/mlb-75",
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
