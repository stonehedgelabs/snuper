import re
from typing import Optional


class Team:
    def __init__(
        self,
        name: str,
        league: str,
        league_color: str,
        city: str = None,
        subreddit: str = None,
        abbreviation: str = None,
        sport_radar_io_team_id: int = None,
    ):
        self.name = name
        self.league = league.lower()
        self.league_color = league_color
        self.city = city
        self.subreddit = subreddit
        self.abbreviation = abbreviation
        self.sport_radar_io_team_id = sport_radar_io_team_id
        self.id = self.generate_team_id()

    def generate_team_id(self) -> str:
        league_lower = self.league.lower()
        city_short = re.sub(r"\s+", "", (self.city or "").lower())
        team_short = re.sub(r"\s+", "-", self.name.lower())
        return f"{league_lower}--{city_short}-{team_short}"

    def __repr__(self):
        return f"<Team {self.name} ({self.abbreviation or ''})>"


def mgm_constant_to_team(constant_name: str, teams: list[Team]) -> Optional[Team]:
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


def team_to_mgm_constant(team: Team) -> Optional[str]:
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


nfl_teams = [
    Team(
        "Arizona Cardinals",
        "nfl",
        "#97233F",
        city="Arizona",
        subreddit="r/AZCardinals",
        abbreviation="ARI",
        sport_radar_io_team_id=1,
    ),
    Team(
        "Atlanta Falcons",
        "nfl",
        "#A71930",
        city="Atlanta",
        subreddit="r/falcons",
        abbreviation="ATL",
        sport_radar_io_team_id=2,
    ),
    Team(
        "Baltimore Ravens",
        "nfl",
        "#241773",
        city="Baltimore",
        subreddit="r/ravens",
        abbreviation="BAL",
        sport_radar_io_team_id=3,
    ),
    Team(
        "Buffalo Bills",
        "nfl",
        "#00338D",
        city="Buffalo",
        subreddit="r/buffalobills",
        abbreviation="BUF",
        sport_radar_io_team_id=4,
    ),
    Team(
        "Carolina Panthers",
        "nfl",
        "#0085CA",
        city="Carolina",
        subreddit="r/panthers",
        abbreviation="CAR",
        sport_radar_io_team_id=5,
    ),
    Team(
        "Chicago Bears",
        "nfl",
        "#0B162A",
        city="Chicago",
        subreddit="r/chibears",
        abbreviation="CHI",
        sport_radar_io_team_id=6,
    ),
    Team(
        "Cincinnati Bengals",
        "nfl",
        "#FB4F14",
        city="Cincinnati",
        subreddit="r/bengals",
        abbreviation="CIN",
        sport_radar_io_team_id=7,
    ),
    Team(
        "Cleveland Browns",
        "nfl",
        "#311D00",
        city="Cleveland",
        subreddit="r/browns",
        abbreviation="CLE",
        sport_radar_io_team_id=8,
    ),
    Team(
        "Dallas Cowboys",
        "nfl",
        "#003594",
        city="Dallas",
        subreddit="r/cowboys",
        abbreviation="DAL",
        sport_radar_io_team_id=9,
    ),
    Team(
        "Denver Broncos",
        "nfl",
        "#FB4F14",
        city="Denver",
        subreddit="r/denverbroncos",
        abbreviation="DEN",
        sport_radar_io_team_id=10,
    ),
    Team(
        "Detroit Lions",
        "nfl",
        "#0076B6",
        city="Detroit",
        subreddit="r/detroitlions",
        abbreviation="DET",
        sport_radar_io_team_id=11,
    ),
    Team(
        "Green Bay Packers",
        "nfl",
        "#203731",
        city="Green Bay",
        subreddit="r/GreenbayPackers",
        abbreviation="GB",
        sport_radar_io_team_id=12,
    ),
    Team(
        "Houston Texans",
        "nfl",
        "#03202F",
        city="Houston",
        subreddit="r/texans",
        abbreviation="HOU",
        sport_radar_io_team_id=13,
    ),
    Team(
        "Indianapolis Colts",
        "nfl",
        "#002C5F",
        city="Indianapolis",
        subreddit="r/colts",
        abbreviation="IND",
        sport_radar_io_team_id=14,
    ),
    Team(
        "Jacksonville Jaguars",
        "nfl",
        "#006778",
        city="Jacksonville",
        subreddit="r/jaguars",
        abbreviation="JAX",
        sport_radar_io_team_id=15,
    ),
    Team(
        "Kansas City Chiefs",
        "nfl",
        "#E31837",
        city="Kansas City",
        subreddit="r/kansascitychiefs",
        abbreviation="KC",
        sport_radar_io_team_id=16,
    ),
    Team(
        "Las Vegas Raiders",
        "nfl",
        "#000000",
        city="Las Vegas",
        subreddit="r/raiders",
        abbreviation="LV",
        sport_radar_io_team_id=25,
    ),
    Team(
        "Los Angeles Chargers",
        "nfl",
        "#0080C6",
        city="Los Angeles",
        subreddit="r/chargers",
        abbreviation="LAC",
        sport_radar_io_team_id=29,
    ),
    Team(
        "Los Angeles Rams",
        "nfl",
        "#003594",
        city="Los Angeles",
        subreddit="r/LosAngelesRams",
        abbreviation="LAR",
        sport_radar_io_team_id=32,
    ),
    Team(
        "Miami Dolphins",
        "nfl",
        "#008E97",
        city="Miami",
        subreddit="r/miamidolphins",
        abbreviation="MIA",
        sport_radar_io_team_id=19,
    ),
    Team(
        "Minnesota Vikings",
        "nfl",
        "#4F2683",
        city="Minneapolis",
        subreddit="r/minnesotavikings",
        abbreviation="MIN",
        sport_radar_io_team_id=20,
    ),
    Team(
        "New England Patriots",
        "nfl",
        "#002244",
        city="New England",
        subreddit="r/patriots",
        abbreviation="NE",
        sport_radar_io_team_id=21,
    ),
    Team(
        "New Orleans Saints",
        "nfl",
        "#D3BC8D",
        city="New Orleans",
        subreddit="r/saints",
        abbreviation="NO",
        sport_radar_io_team_id=22,
    ),
    Team(
        "New York Giants",
        "nfl",
        "#0B2265",
        city="New York",
        subreddit="r/nygiants",
        abbreviation="NYG",
        sport_radar_io_team_id=23,
    ),
    Team(
        "New York Jets",
        "nfl",
        "#125740",
        city="New York",
        subreddit="r/nyjets",
        abbreviation="NYJ",
        sport_radar_io_team_id=24,
    ),
    Team(
        "Philadelphia Eagles",
        "nfl",
        "#004C54",
        city="Philadelphia",
        subreddit="r/eagles",
        abbreviation="PHI",
        sport_radar_io_team_id=26,
    ),
    Team(
        "Pittsburgh Steelers",
        "nfl",
        "#FFB612",
        city="Pittsburgh",
        subreddit="r/steelers",
        abbreviation="PIT",
        sport_radar_io_team_id=28,
    ),
    Team(
        "San Francisco 49ers",
        "nfl",
        "#AA0000",
        city="San Francisco",
        subreddit="r/49ers",
        abbreviation="SF",
        sport_radar_io_team_id=31,
    ),
    Team(
        "Seattle Seahawks",
        "nfl",
        "#002244",
        city="Seattle",
        subreddit="r/seahawks",
        abbreviation="SEA",
        sport_radar_io_team_id=30,
    ),
    Team(
        "Tampa Bay Buccaneers",
        "nfl",
        "#D50A0A",
        city="Tampa Bay",
        subreddit="r/buccaneers",
        abbreviation="TB",
        sport_radar_io_team_id=33,
    ),
    Team(
        "Tennessee Titans",
        "nfl",
        "#0C2340",
        city="Nashville",
        subreddit="r/tennesseetitans",
        abbreviation="TEN",
        sport_radar_io_team_id=34,
    ),
    Team(
        "Washington Commanders",
        "nfl",
        "#5A1414",
        city="Washington",
        subreddit="r/commanders",
        abbreviation="WAS",
        sport_radar_io_team_id=35,
    ),
]

nba_teams = [
    Team("Atlanta Hawks", "nba", "#E03A3E", city="Atlanta", subreddit="r/AtlantaHawks", abbreviation="ATL"),
    Team("Boston Celtics", "nba", "#007A33", city="Boston", subreddit="r/bostonceltics", abbreviation="BOS"),
    Team("Brooklyn Nets", "nba", "#000000", city="Brooklyn", subreddit="r/GoNets", abbreviation="BKN"),
    Team("Charlotte Hornets", "nba", "#1D1160", city="Charlotte", subreddit="r/CharlotteHornets", abbreviation="CHA"),
    Team("Chicago Bulls", "nba", "#CE1141", city="Chicago", subreddit="r/chicagobulls", abbreviation="CHI"),
    Team("Cleveland Cavaliers", "nba", "#860038", city="Cleveland", subreddit="r/clevelandcavs", abbreviation="CLE"),
    Team("Dallas Mavericks", "nba", "#00538C", city="Dallas", subreddit="r/Mavericks", abbreviation="DAL"),
    Team("Denver Nuggets", "nba", "#0E2240", city="Denver", subreddit="r/DenverNuggets", abbreviation="DEN"),
    Team("Detroit Pistons", "nba", "#C8102E", city="Detroit", subreddit="r/DetroitPistons", abbreviation="DET"),
    Team("Golden State Warriors", "nba", "#1D428A", city="San Francisco", subreddit="r/warriors", abbreviation="GSW"),
    Team("Houston Rockets", "nba", "#CE1141", city="Houston", subreddit="r/rockets", abbreviation="HOU"),
    Team("Indiana Pacers", "nba", "#002D62", city="Indianapolis", subreddit="r/pacers", abbreviation="IND"),
    Team("Los Angeles Clippers", "nba", "#C8102E", city="Los Angeles", subreddit="r/LAClippers", abbreviation="LAC"),
    Team("Los Angeles Lakers", "nba", "#552583", city="Los Angeles", subreddit="r/lakers", abbreviation="LAL"),
    Team("Memphis Grizzlies", "nba", "#5D76A9", city="Memphis", subreddit="r/memphisgrizzlies", abbreviation="MEM"),
    Team("Miami Heat", "nba", "#98002E", city="Miami", subreddit="r/heat", abbreviation="MIA"),
    Team("Milwaukee Bucks", "nba", "#00471B", city="Milwaukee", subreddit="r/MkeBucks", abbreviation="MIL"),
    Team(
        "Minnesota Timberwolves", "nba", "#0C2340", city="Minneapolis", subreddit="r/timberwolves", abbreviation="MIN"
    ),
    Team("New Orleans Pelicans", "nba", "#0C2340", city="New Orleans", subreddit="r/NOLAPelicans", abbreviation="NOP"),
    Team("New York Knicks", "nba", "#F58426", city="New York", subreddit="r/NYKnicks", abbreviation="NYK"),
    Team("Oklahoma City Thunder", "nba", "#007AC1", city="Oklahoma City", subreddit="r/Thunder", abbreviation="OKC"),
    Team("Orlando Magic", "nba", "#0077C0", city="Orlando", subreddit="r/orlandomagic", abbreviation="ORL"),
    Team("Philadelphia 76ers", "nba", "#006BB6", city="Philadelphia", subreddit="r/sixers", abbreviation="PHI"),
    Team("Phoenix Suns", "nba", "#1D1160", city="Phoenix", subreddit="r/suns", abbreviation="PHX"),
    Team("Portland Trail Blazers", "nba", "#E03A3E", city="Portland", subreddit="r/ripcity", abbreviation="POR"),
    Team("Sacramento Kings", "nba", "#5A2D81", city="Sacramento", subreddit="r/kings", abbreviation="SAC"),
    Team("San Antonio Spurs", "nba", "#C4CED4", city="San Antonio", subreddit="r/nbaspurs", abbreviation="SAS"),
    Team("Toronto Raptors", "nba", "#CE1141", city="Toronto", subreddit="r/torontoraptors", abbreviation="TOR"),
    Team("Utah Jazz", "nba", "#002B5C", city="Salt Lake City", subreddit="r/UtahJazz", abbreviation="UTA"),
    Team(
        "Washington Wizards", "nba", "#002B5C", city="Washington", subreddit="r/washingtonwizards", abbreviation="WAS"
    ),
]


mlb_teams = [
    Team("Arizona Diamondbacks", "mlb", "#A71930", city="Phoenix", subreddit="r/azdiamondbacks", abbreviation="ARI"),
    Team("Atlanta Braves", "mlb", "#CE1141", city="Atlanta", subreddit="r/Braves", abbreviation="ATL"),
    Team("Baltimore Orioles", "mlb", "#DF4601", city="Baltimore", subreddit="r/Orioles", abbreviation="BAL"),
    Team("Boston Red Sox", "mlb", "#BD3039", city="Boston", subreddit="r/RedSox", abbreviation="BOS"),
    Team("Chicago Cubs", "mlb", "#0E3386", city="Chicago", subreddit="r/CHICubs", abbreviation="CHC"),
    Team("Chicago White Sox", "mlb", "#27251F", city="Chicago", subreddit="r/WhiteSox", abbreviation="CWS"),
    Team("Cincinnati Reds", "mlb", "#C6011F", city="Cincinnati", subreddit="r/Reds", abbreviation="CIN"),
    Team(
        "Cleveland Guardians", "mlb", "#E31937", city="Cleveland", subreddit="r/ClevelandGuardians", abbreviation="CLE"
    ),
    Team("Colorado Rockies", "mlb", "#33006F", city="Denver", subreddit="r/ColoradoRockies", abbreviation="COL"),
    Team("Detroit Tigers", "mlb", "#0C2340", city="Detroit", subreddit="r/MotorCityKitties", abbreviation="DET"),
    Team("Houston Astros", "mlb", "#002D62", city="Houston", subreddit="r/Astros", abbreviation="HOU"),
    Team("Kansas City Royals", "mlb", "#004687", city="Kansas City", subreddit="r/Royals", abbreviation="KC"),
    Team("Los Angeles Angels", "mlb", "#BA0021", city="Anaheim", subreddit="r/AngelsBaseball", abbreviation="LAA"),
    Team("Los Angeles Dodgers", "mlb", "#005A9C", city="Los Angeles", subreddit="r/Dodgers", abbreviation="LAD"),
    Team("Miami Marlins", "mlb", "#00A3E0", city="Miami", subreddit="r/letsgofish", abbreviation="MIA"),
    Team("Milwaukee Brewers", "mlb", "#12284B", city="Milwaukee", subreddit="r/Brewers", abbreviation="MIL"),
    Team("Minnesota Twins", "mlb", "#002B5C", city="Minneapolis", subreddit="r/minnesotatwins", abbreviation="MIN"),
    Team("New York Mets", "mlb", "#002D72", city="New York", subreddit="r/NewYorkMets", abbreviation="NYM"),
    Team("New York Yankees", "mlb", "#132448", city="New York", subreddit="r/NYYankees", abbreviation="NYY"),
    Team("Oakland Athletics", "mlb", "#003831", city="Oakland", subreddit="r/OaklandAthletics", abbreviation="OAK"),
    Team("Philadelphia Phillies", "mlb", "#E81828", city="Philadelphia", subreddit="r/Phillies", abbreviation="PHI"),
    Team("Pittsburgh Pirates", "mlb", "#FDB827", city="Pittsburgh", subreddit="r/Buccos", abbreviation="PIT"),
    Team("San Diego Padres", "mlb", "#2F241D", city="San Diego", subreddit="r/Padres", abbreviation="SD"),
    Team("San Francisco Giants", "mlb", "#FD5A1E", city="San Francisco", subreddit="r/SFGiants", abbreviation="SF"),
    Team("Seattle Mariners", "mlb", "#0C2C56", city="Seattle", subreddit="r/Mariners", abbreviation="SEA"),
    Team("St. Louis Cardinals", "mlb", "#C41E3A", city="St. Louis", subreddit="r/Cardinals", abbreviation="STL"),
    Team("Tampa Bay Rays", "mlb", "#092C5C", city="St. Petersburg", subreddit="r/TampaBayRays", abbreviation="TB"),
    Team("Texas Rangers", "mlb", "#003278", city="Arlington", subreddit="r/texasrangers", abbreviation="TEX"),
    Team("Toronto Blue Jays", "mlb", "#134A8E", city="Toronto", subreddit="r/Torontobluejays", abbreviation="TOR"),
    Team("Washington Nationals", "mlb", "#AB0003", city="Washington", subreddit="r/Nationals", abbreviation="WSH"),
]
