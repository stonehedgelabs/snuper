from __future__ import annotations

import enum


class League(enum.Enum):
    """Sports leagues."""

    NBA = "nba"
    MLB = "mlb"
    NFL = "nfl"


class Provider(enum.Enum):
    """Sports data providers."""

    DraftKings = "draftkings"
    BetMGM = "betmgm"
    FanDuel = "fanduel"
    Bovada = "bovada"


class TaskType(enum.Enum):
    """CLI task types."""

    SCRAPE = "scrape"
    MONITOR = "monitor"


# ANSI escape code for cyan log output.
CYAN = "\033[96m"

# ANSI escape code for red log output.
RED = "\033[91m"

# ANSI escape code for yellow log output.
YELLOW = "\033[93m"

# ANSI escape code that resets color formatting.
RESET = "\033[0m"

# Default format for timestamped artifact filenames.
DATE_STAMP_FORMAT = "%Y%m%d"

# Canonical list of leagues supported across scrapers.
SUPPORTED_LEAGUES = [League.NBA.value, League.NFL.value, League.MLB.value]

# Default event duration used to determine when games expire (seconds).
GAME_RUNTIME_SECONDS = 3.5 * 3600

# Primary DraftKings websocket endpoint for odds streaming.
DRAFTKINGS_WEBSOCKET_URL = "wss://sportsbook-ws-us-nj.draftkings.com/websocket?format=msgpack&locale=en"

# Frequency for heartbeat logging of Bovada websocket events.
BOVADA_EVENT_LOG_INTERVAL = 200

# Seconds between forced heartbeat logs for Bovada events.
BOVADA_HEARTBEAT_SECONDS = 60

# Number of idle seconds tolerated before assuming a Bovada game ended.
BOVADA_MAX_TIME_SINCE_LAST_EVENT = 60 * 30

# DraftKings market type identifier used when filtering spreads.
DRAFTKINGS_SPREAD_MARKET_TYPE = "Spread"

# Seconds between monitor sweeps when driving the CLI loop.
DRAFTKINGS_DEFAULT_MONITOR_INTERVAL = 30

# Number of idle seconds tolerated before assuming a game ended.
DRAFTKINGS_MAX_TIME_SINCE_LAST_EVENT = 60 * 10

# Frequency for heartbeat logging of DraftKings websocket events.
DRAFTKINGS_EVENT_LOG_INTERVAL = 1000

# Seconds between forced heartbeat logs for DraftKings events.
DRAFTKINGS_HEARTBEAT_SECONDS = 60

# Landing pages used when scraping DraftKings event metadata.
DRAFTKINGS_LEAGUE_URLS = {
    League.NFL.value: "https://sportsbook.draftkings.com/leagues/football/nfl",
    League.MLB.value: "https://sportsbook.draftkings.com/leagues/baseball/mlb",
    League.NBA.value: "https://sportsbook.draftkings.com/leagues/basketball/nba",
}

# Seconds between successive MGM page reloads while polling.
MGM_DEFAULT_MONITOR_INTERVAL = 1

# Milliseconds to wait for page load time in Playwright.
MGM_PAGE_LOAD_TIME = 5_000

# Seconds between BetMGM monitor sweeps of stored event snapshots.
MGM_MONITOR_SWEEP_INTERVAL = 30

# Number of captured snapshots between MGM heartbeat logs.
MGM_EVENT_LOG_INTERVAL = 10

# Maximum idle time before polling terminates a session.
MAX_IDLE_SECONDS = 300

# Seconds between forced heartbeat logs for MGM events.
MGM_HEARTBEAT_SECONDS = 60

# Maximum consecutive runner errors tolerated before stopping.
MAX_RUNNER_ERRORS = 5

# League index pages used to collect MGM event slugs.
MGM_LEAGUE_URLS = {
    League.NFL.value: "https://www.co.betmgm.com/en/sports/football-11/today",
    League.MLB.value: "https://www.co.betmgm.com/en/sports/baseball-23/today",
    League.NBA.value: "https://www.co.betmgm.com/en/sports/basketball-7/today",
}
# Lowercase MGM team slugs that flag NBA events during filtering.
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

# Lowercase MGM team slugs that flag NFL events during filtering.
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

# Lowercase MGM team slugs that flag MLB events during filtering.
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


# Mapping from league codes to their parent sports.
SPORTS = {
    League.NBA.value: "basketball",
    League.MLB.value: "baseball",
    League.NFL.value: "football",
}
