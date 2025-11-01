from __future__ import annotations

import datetime as dt
import logging
import pathlib
import re
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright
from tzlocal import get_localzone

from snuper.runner import BaseMonitor
from snuper.scraper import BaseEventScraper, ScrapeContext
from snuper.t import Event
from snuper.sinks import SelectionSink

CYAN = "\033[96m"
"""ANSI escape code for cyan logs."""

RED = "\033[91m"
"""ANSI escape code for red logs."""

RESET = "\033[0m"
"""ANSI escape code that resets terminal colours."""

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s]: %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_LOOP_INTERVAL = 30
"""Seconds to wait between monitor passes when polling FanDuel."""

GAME_RUNTIME = 5 * 3600
"""Maximum assumed duration of a FanDuel game in seconds."""

FANDUEL_LEAGUE_URLS = {
    "nfl": "https://sportsbook.fanduel.com/football/nfl",
    "mlb": "https://sportsbook.fanduel.com/baseball/mlb",
    "nba": "https://sportsbook.fanduel.com/basketball/nba",
}
"""League entry points used to discover FanDuel events."""


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


class EventScraper(BaseEventScraper):
    """Scrape FanDuel league listings to build event snapshots."""

    def __init__(self, output_dir: pathlib.Path | str | None = None) -> None:
        """Initialise league list, logger, and regex helpers."""
        super().__init__(tuple(FANDUEL_LEAGUE_URLS.keys()), output_dir=output_dir)
        self.local_tz = get_localzone()
        self.pattern_event_path = re.compile(
            r"^/(football|baseball)/[a-z]+/[a-z0-9\-@%]+-\d+$",
            re.IGNORECASE,
        )
        self.base_domain = "https://sportsbook.fanduel.com"

    def extract_team_info(self, event_url: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
        """Split a FanDuel slug into short and long team identifiers."""
        slug = urlparse(event_url).path.split("/")[-1]
        try:
            # pattern: new-york-jets-@-cincinnati-bengals-34844525
            away_part, home_part, _ = slug.split("-@-")
        except ValueError:
            return None

        def _parse_team(part: str) -> tuple[str, str]:
            """Normalize a slug fragment into (short, long) team names."""
            tokens = part.strip("-").split("-")
            if len(tokens) >= 2:
                short = tokens[0].lower()
                name = "-".join(tokens[1:]).lower()
                return short, name
            return tokens[0].lower(), ""

        return _parse_team(away_part), _parse_team(home_part)

    async def scrape_today(
        self,
        context: ScrapeContext,
        source_events: Sequence[Event] | None = None,
    ) -> list[Event]:
        """Collect today's FanDuel events for the requested league."""
        league = context.league
        base_url = FANDUEL_LEAGUE_URLS.get(league.lower())
        if not base_url:
            raise ValueError(f"Unsupported league: {league}")

        self.log.info("%s - detected local timezone: %s", self.__class__.__name__, self.local_tz)
        # now = dt.datetime.now(self.local_tz)
        # start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
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
            await browser.close()

        # Filter for valid relative paths like /football/nfl/new-york-jets-@-cincinnati-bengals-34844525
        event_paths = sorted(set(h for h in hrefs if h and self.pattern_event_path.match(h)))
        event_urls = [self.base_domain + path for path in event_paths]

        self.log.info("%s - found %d event URLs on %s", self.__class__.__name__, len(event_urls), base_url)

        for event_url in event_urls:
            try:
                event_id = event_url.split("-")[-1]
                # TODO: FanDuel doesn't show start times on list view; may require secondary fetch
                start_time_utc = dt.datetime.now(dt.timezone.utc)
                away, home = self.extract_team_info(event_url) or (
                    ("?", "?"),
                    ("?", "?"),
                )
                selections = parse_event_markets_fanduel(event_id)
                events.append(
                    Event(
                        event_id,
                        league,
                        event_url,
                        start_time_utc,
                        away,
                        home,
                        selections,
                    )
                )
            except Exception as e:
                self.log.warning("%s - error parsing %s: %s", self.__class__.__name__, event_url, e)

        self.log.info("%s - total %d events for today in %s.", self.__class__.__name__, len(events), league.upper())
        return events


class FanDuelMonitor(BaseMonitor):
    """Placeholder FanDuel monitor that will poll markets once implemented."""

    def __init__(
        self,
        input_dir: pathlib.Path,
        concurrency: int = 10,
        *,
        leagues: Sequence[str] | None = None,
    ) -> None:
        """Initialise paths and bookkeeping for future polling tasks."""
        # FanDuel monitor is not yet implemented
        # pylint: disable=super-init-not-called
        raise NotImplementedError("TODO: Implement FanDuel live polling monitor")

    async def run_once(self) -> None:
        """Raise NotImplementedError until the live monitor is built."""
        raise NotImplementedError("TODO: Implement FanDuel live polling monitor")


async def run_scrape(
    output_dir: pathlib.Path,
    *,
    leagues: Sequence[str] | None = None,
    overwrite: bool = False,
    sink: SelectionSink | None = None,
) -> None:
    """Invoke the FanDuel scraper with the provided destination."""

    scraper = EventScraper(output_dir)
    await scraper.scrape_and_save_all(
        output_dir,
        leagues=leagues,
        overwrite=overwrite,
        sink=sink,
        provider="fanduel",
    )


async def run_monitor(
    input_dir: pathlib.Path,
    *,
    leagues: Sequence[str] | None = None,
) -> None:
    """Execute the placeholder FanDuel monitor."""

    _ = leagues
    monitor = FanDuelMonitor(input_dir)
    await monitor.run_once()
