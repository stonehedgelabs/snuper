from __future__ import annotations
import asyncio
import argparse
import datetime as dt
import json
import logging
import os
import re
import time
import t
import pathlib
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import httpx
from playwright.async_api import async_playwright
from tzlocal import get_localzone

CYAN = "\033[96m"
RED = "\033[91m"
RESET = "\033[0m"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_LOOP_INTERVAL = 30
GAME_RUNTIME = 5 * 3600  # 5 hours

FANDUEL_LEAGUE_URLS = {
    "nfl": "https://sportsbook.fanduel.com/football/nfl",
    "mlb": "https://sportsbook.fanduel.com/baseball/mlb",
    "nba": "https://sportsbook.fanduel.com/basketball/nba",
}


def event_filepath(output_dir: pathlib.Path, league: str) -> pathlib.Path:
    path = pathlib.Path(output_dir) / "events"
    timestamp = dt.datetime.now().strftime("%Y%m%d")
    filename = f"{league}-{timestamp}.json"
    return path.joinpath(filename)


def odds_filepath(output_dir: pathlib.Path, league: str, event_id: str) -> pathlib.Path:
    path = pathlib.Path(output_dir) / "odds"
    timestamp = dt.datetime.now().strftime("%Y%m%d")
    filename = f"{league}-{timestamp}-{event_id}.json"
    return path.joinpath(filename)


def flatten_fanduel_markets(data: dict) -> list[dict[str, Any]]:
    """
    Convert FanDuel API market payloads into DK-style flat structures.
    Currently raises NotImplementedError until we know exact payload structure.
    """
    raise NotImplementedError("TODO: Implement FanDuel market flattener")


def parse_event_markets_fanduel(event_id: str) -> list[dict[str, Any]]:
    """
    Retrieve and parse FanDuel event market data.
    Example endpoint:
        https://smp.nj.sportsbook.fanduel.com/api/sports/fixedodds/readonly/v1/getMarketPrices?priceHistory=1
    """
    raise NotImplementedError("TODO: Implement FanDuel market request + flattening")


# ---------------------------------------------------------------------
# Event object
# ---------------------------------------------------------------------


class Event:
    def __init__(
        self,
        event_id: str,
        league: str,
        url: str,
        start_time: dt.datetime,
        away: tuple[str, str],
        home: tuple[str, str],
        selections: Optional[list],
    ) -> None:
        self.event_id = event_id
        self.league = league
        self.url = url
        self.start_time = start_time
        self.away = away
        self.home = home
        self.selections = selections
        self.log = logging.getLogger(self.__class__.__name__)

    def get_key(self) -> str:
        return f"{self.league}:{self.event_id}"

    def has_started(self) -> bool:
        return self.start_time <= dt.datetime.now(dt.timezone.utc)

    def is_finished(self) -> bool:
        delta = dt.datetime.now(dt.timezone.utc) - self.start_time
        return delta.total_seconds() > GAME_RUNTIME

    def to_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "league": self.league,
            "event_url": self.url,
            "start_time_utc": self.start_time.isoformat(),
            "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
            "selections": self.selections,
            "away": self.away,
            "home": self.home,
        }

    def __repr__(self) -> str:
        return f"<Event[{self.league}, {self.event_id}, {self.away[0]}@{self.home[0]}]>"


class EventScraper:
    def __init__(self) -> None:
        self.leagues = list(FANDUEL_LEAGUE_URLS.keys())
        self.log = logging.getLogger(self.__class__.__name__)
        self.local_tz = get_localzone()
        self.pattern_event_path = re.compile(
            r"^/(football|baseball)/[a-z]+/[a-z0-9\-@%]+-\d+$",
            re.IGNORECASE,
        )
        self.base_domain = "https://sportsbook.fanduel.com"

    def extract_team_info(self, event_url: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
        """Extract ('away', 'home') team tokens from FanDuel slug."""
        slug = urlparse(event_url).path.split("/")[-1]
        try:
            # pattern: new-york-jets-@-cincinnati-bengals-34844525
            away_part, home_part, _ = slug.split("-@-")
        except ValueError:
            return None

        def _parse_team(part: str) -> tuple[str, str]:
            tokens = part.strip("-").split("-")
            if len(tokens) >= 2:
                short = tokens[0].lower()
                name = "-".join(tokens[1:]).lower()
                return short, name
            return tokens[0].lower(), ""

        return _parse_team(away_part), _parse_team(home_part)

    async def scrape_today(self, league: str) -> list[Event]:
        """Scrape FanDuel league page and gather event URLs."""
        base_url = FANDUEL_LEAGUE_URLS.get(league.lower())
        if not base_url:
            raise ValueError(f"Unsupported league: {league}")

        self.log.info(f"Detected local timezone: {self.local_tz}")
        now = dt.datetime.now(self.local_tz)
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + dt.timedelta(days=1)
        events: list[Event] = []
        headers = {
            # ":authority": "sportsbook.fanduel.com",
            # ":method": "POST",
            # ":path": "/JMCVuBG8/xhr/api/v2/collector",
            # ":scheme": "https",
            "accept": "*/*",
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.7",
            "content-length": "817",
            "content-type": "application/x-www-form-urlencoded",
            "cookie": (
                "X-Sportsbook-Region=nj; "
                "amp_device_id=5e54feaf-7cbe-4bc8-97c2-23c7d08f900a; "
                "pxcts=d7f0f99b-b205-11f0-bb23-df6e4a68f1b8; "
                "_pxvid=d7f0ef38-b205-11f0-bb21-dc47fa1a7a3c; "
                "__pxvid=d81098a6-b205-11f0-bc95-d24f78a979e7; "
                "X-Site-Version=desktop; "
                "amp_session_id=1761443648772; "
                "LSKey-c$AMP_MKTG_5f3c163a17=JTdCJTIycmVmZXJyZXIlMjIlM0ElMjJodHRwcyUzQSUyRiUyRnd3dy5nb29nbGUuY29tJTJGJTIyJTJDJTIycmVmZXJyaW5nX2RvbWFpbiUyMiUzQSUyMnd3dy5nb29nbGUuY29tJTIyJTdE; "
                "LSKey-c$AMP_5f3c163a17=JTdCJTIyZGV2aWNlSWQlMjIlM0ElMjJlZDQ5OGQzYy03MmM5LTQ1OWMtOTdjMi0zZjEyOWQwN2Q0YzglMjIlMkMlMjJzZXNzaW9uSWQlMjIlM0ExNzYxNDQzNzE4MDI5JTJDJTIyb3B0T3V0JTIyJTNBZmFsc2UlMkMlMjJsYXN0RXZlbnRUaW1lJTIyJTNBMTc2MTQ0MzcxODUxOSUyQyUyMmxhc3RFdmVudElkJTIyJTNBMyU3RA==; "
                "x-keep-alive-last-fired=2025-10-26T02:05:24.384Z; "
                "_px3=e3e5cfe5c81b0d0d6efcfa46d40b57cb17c1875dd83bb79d1438f7d929deed42:"
                "GszoX/d4XUKwPEFUCQ2g6L6pANDBmakBLVii4R2bmtuMsp5iMpf4nD0HLSrIgxUxnKkix9sP0jmQUe3HTKztoQ==:"
                "1000:xCZMlMtdqPSHYg7vX+me6vlZsDe1TUGOxDihjB8LB+wHhUHraZa64NE8PZWy3SMjJ0s6LIsqApQ67WcgWuz7GizN1BS8LDGmat2WtNwf3jpm5fMXC6N23g3P87sxS3Dxk8Q7xyUvbL1b0cuUXajk3zNb7WUlmrS2Q3u+9AOGOD9vRlsGCTESXWGYjmLkYEhJ/O0iDEsf9rlQPyS2ChbEj7hPIHsSbSW+eFRK8UcUvzY=; "
                "_pxde=ce8740100789ea3bafebef238b1774c88c4635849b2fd1f4e9b75d4cc6d94e8b:"
                "eyJ0aW1lc3RhbXAiOjE3NjE0NDYzNzYwMjR9; "
                "_dd_s=aid=4813d639-931c-42b1-b66c-24464317b2d1&rum=0&expire=1761447501913&logs=1&id=6ac9367c-8261-4c11-9915-ee24783d5ef2&created=1761443648798"
            ),
            "origin": "https://sportsbook.fanduel.com",
            "priority": "u=1, i",
            "referer": "https://sportsbook.fanduel.com/baseball/mlb/los-angeles-dodgers-@-toronto-blue-jays-34872359",
            "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"macOS"',
            "sec-fetch-dest": "empty",
            "sec-fetch-mode": "cors",
            "sec-fetch-site": "same-origin",
            "sec-gpc": "1",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/140.0.0.0 Safari/537.36"
            ),
        }

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(locale="en-US", extra_http_headers=headers)
            page = await context.new_page()
            await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(4000)
            hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
            print(hrefs)
            await browser.close()

        # Filter for valid relative paths like /football/nfl/new-york-jets-@-cincinnati-bengals-34844525
        event_paths = sorted(set(h for h in hrefs if h and self.pattern_event_path.match(h)))
        event_urls = [self.base_domain + path for path in event_paths]

        self.log.info(f"Found {len(event_urls)} event URLs on {base_url}")

        for event_url in event_urls:
            try:
                event_id = event_url.split("-")[-1]
                # TODO: FanDuel doesn't show start times on list view; may require secondary fetch
                start_time_utc = dt.datetime.now(dt.timezone.utc)
                away, home = self.extract_team_info(event_url) or (("?", "?"), ("?", "?"))
                selections = parse_event_markets_fanduel(event_id)
                events.append(Event(event_id, league, event_url, start_time_utc, away, home, selections))
            except Exception as e:
                self.log.warning(f"Error parsing {event_url}: {e}")

        self.log.info(f"Total {len(events)} events for today in {league.upper()}.")
        return events

    def save(self, games: list[Event], league: str, output_dir: pathlib.Path) -> Optional[pathlib.Path]:
        if not games:
            self.log.warning(f"No games to save for {league} today.")
            return

        path = event_filepath(output_dir, league)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fp:
            json.dump([g.to_dict() for g in games], fp, indent=2)
        self.log.info(f"Saved {len(games)} events to {path}")
        return path

    async def scrape_and_save_all(self, output_dir: pathlib.Path) -> list[pathlib.Path]:
        paths = []
        for league in self.leagues:
            try:
                path = event_filepath(output_dir, league)
                if path.exists():
                    self.log.warning(f"File {path} already exists. Skipping.")
                    continue
                self.log.info(f"Scraping {league.upper()}...")
                games = await self.scrape_today(league)
                path = self.save(games, league, output_dir)
                if path:
                    paths.append(path)
            except Exception as e:
                self.log.error(f"Failed to scrape/save {league}: {e}")
        return paths


class Monitor:
    def __init__(self, input_dir: pathlib.Path, concurrency: int = 10) -> None:
        self.input_dir = pathlib.Path(input_dir)
        self.output_dir = self.input_dir
        self.concurrency = concurrency
        self.log = logging.getLogger(self.__class__.__name__)
        self.active_tasks: dict[str, asyncio.Task] = {}

    async def run_once(self) -> None:
        """
        Placeholder: would read event JSON files and poll FanDuel markets for live odds.
        FanDuel has no public websocket feed; this must be implemented with polling or private APIs.
        """
        raise NotImplementedError("TODO: Implement FanDuel live polling monitor")


async def main() -> None:
    parser = argparse.ArgumentParser(description="FanDuel Event Monitor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    scrape_p = sub.add_parser("scrape", help="Scrape today's events from FanDuel")
    scrape_p.add_argument("-o", "--output-dir", required=True, type=pathlib.Path)

    monitor_p = sub.add_parser("monitor", help="Monitor live FanDuel events")
    monitor_p.add_argument("--input-dir", required=True, type=pathlib.Path)

    args = parser.parse_args()

    if args.cmd == "scrape":
        scraper = EventScraper()
        await scraper.scrape_and_save_all(args.output_dir)
    elif args.cmd == "monitor":
        monitor = Monitor(args.input_dir)
        await monitor.run_once()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully.")
