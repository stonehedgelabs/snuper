from __future__ import annotations
import asyncio
import argparse
import datetime as dt
import json
import logging
import os
import re
import zoneinfo
import time
import types
import pathlib
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import httpx
from playwright.async_api import async_playwright
from tzlocal import get_localzone

from event_monitor.t import Event
from event_monitor.utils import load_events, decimal_to_american

CYAN = "\033[96m"
RED = "\033[91m"
RESET = "\033[0m"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

DEFAULT_LOOP_INTERVAL = 1  # 1 second for live monitoring
GAME_RUNTIME = 5 * 3600  # 5 hours

MGM_LEAGUE_URLS = {
    "nfl": "https://www.co.betmgm.com/en/sports/football-11/betting/usa-9/nfl-35",
    "mlb": "https://www.co.betmgm.com/en/sports/baseball-23/betting/usa-9/mlb-75",
    "nba": "https://www.co.betmgm.com/en/sports/basketball-7/today",
}

NBA_TEAMS = {
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


class EventScraper:
    def __init__(self) -> None:
        self.leagues = list(MGM_LEAGUE_URLS.keys())
        self.log = logging.getLogger(self.__class__.__name__)
        self.local_tz = get_localzone()
        self.pattern_event_path = re.compile(
            r"^/en/sports/events/[a-z0-9\-@%]+-\d+$",
            re.IGNORECASE,
        )
        self.base_domain = "https://www.co.betmgm.com"

    async def extract_start_time(self, page):
        """Extract start time from MGM event header (span.date + span.time) and convert to UTC."""
        from datetime import datetime, timedelta
        from zoneinfo import ZoneInfo
        from tzlocal import get_localzone

        try:
            # Wait for both elements to appear
            await page.wait_for_selector(
                "#main-view > ms-event-details-main > ds-card > ms-header > ms-header-content > ms-scoreboard > ms-prematch-scoreboard > div > div.header > div > span.time",
                timeout=15000,
            )
            await page.wait_for_selector(
                "#main-view > ms-event-details-main > ds-card > ms-header > ms-header-content > ms-scoreboard > ms-prematch-scoreboard > div > div.header > div > span.date",
                timeout=15000,
            )
            # await page.wait_for_selector("span.time")
            # await page.wait_for_selector("span.date")

            # Extract text contents via JS
            date_time_texts = await page.evaluate(
                """
                () => {
                    const dateEl = document.querySelector(
                        '#main-view > ms-event-details-main > ds-card > ms-header > ms-header-content > ms-scoreboard > ms-prematch-scoreboard > div > div.header > div > span.date'
                    );
                    const timeEl = document.querySelector(
                        '#main-view > ms-event-details-main > ds-card > ms-header > ms-header-content > ms-scoreboard > ms-prematch-scoreboard > div > div.header > div > span.time'
                    );
                    return {
                        date: dateEl ? dateEl.textContent.trim() : null,
                        time: timeEl ? timeEl.textContent.trim() : null
                    };
                }
                """
            )

            date_text = date_time_texts.get("date")
            time_text = date_time_texts.get("time")

            self.log.info(f"MGM extracted date='{date_text}', time='{time_text}'")

            if not date_text or not time_text:
                raise ValueError("Missing date or time text")

            local_tz = get_localzone()
            now_local = datetime.now(local_tz)

            # Interpret the date string
            if date_text.lower() == "today":
                event_date = now_local.date()
            elif date_text.lower() == "tomorrow":
                event_date = (now_local + timedelta(days=1)).date()
            elif "/" in date_text:
                try:
                    event_date = datetime.strptime(date_text, "%m/%d/%y").date()
                except ValueError:
                    event_date = datetime.strptime(date_text, "%m/%d/%Y").date()
            else:
                for fmt in ("%a %d %b", "%b %d", "%a, %b %d"):
                    try:
                        event_date = datetime.strptime(date_text, fmt).replace(year=now_local.year).date()
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError(f"Unrecognized date format: {date_text}")

            # Parse time (e.g. "7:30 PM")
            event_time = datetime.strptime(time_text, "%I:%M %p").time()

            # Combine into local datetime and convert to UTC
            local_dt = datetime.combine(event_date, event_time, tzinfo=local_tz)
            utc_dt = local_dt.astimezone(ZoneInfo("UTC"))

            self.log.info(f"Converted MGM time to UTC: {utc_dt}")
            return utc_dt

        except Exception as e:
            self.log.warning(f"Could not extract MGM start time: {e}")
            return None

    def _is_nba_game(self, event_url: str) -> bool:
        """Check if the event URL contains NBA team names."""
        url_lower = event_url.lower()
        return any(team in url_lower for team in NBA_TEAMS)

    def extract_team_info(self, event_url: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
        """Extract ('away', 'home') from MGM slug like 'brooklyn-nets-at-san-antonio-spurs-18269269'."""
        slug = urlparse(event_url).path.split("/")[-1]

        parts = slug.rsplit("-", 1)
        if len(parts) == 2 and parts[1].isdigit():
            slug_without_id = parts[0]
        else:
            slug_without_id = slug

        if "-at-" not in slug_without_id:
            return None

        try:
            away_part, home_part = slug_without_id.split("-at-")
        except ValueError:
            return None

        def _parse_team(part: str) -> tuple[str, str]:
            tokens = part.strip("-").split("-")
            if len(tokens) >= 2:
                city = tokens[0].lower()
                mascot = "-".join(tokens[1:]).lower()
                return city, mascot
            return tokens[0].lower(), ""

        return _parse_team(away_part), _parse_team(home_part)

    async def scrape_today(self, league: str) -> list[Event]:
        """Scrape MGM league page and gather event URLs."""
        base_url = MGM_LEAGUE_URLS.get(league.lower())
        if not base_url:
            raise ValueError(f"Unsupported league: {league}")

        self.log.info(f"Detected local timezone: {self.local_tz}")
        events: list[Event] = []

        headers = {
            "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "accept-language": "en-US,en;q=0.7",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/140.0.0.0 Safari/537.36"
            ),
            "referer": "https://www.co.betmgm.com/",
        }

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(locale="en-US", extra_http_headers=headers)
            page = await context.new_page()

            self.log.info(f"Navigating to {base_url}")
            await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(4000)
            hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")

            event_paths = sorted(set(h for h in hrefs if h and self.pattern_event_path.match(h)))
            event_urls = [self.base_domain + path for path in event_paths]
            if league == "nba":
                event_urls = list(filter(lambda x: self._is_nba_game(x), event_urls))
            self.log.info(f"Found {len(event_urls)} event URLs on {base_url}")

            # Get today's date range in local timezone
            now_local = dt.datetime.now(self.local_tz)
            today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = today_start + dt.timedelta(days=1)

            for event_url in event_urls:
                try:
                    self.log.info(f"Navigating to event: {event_url}")
                    await page.goto(event_url, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(2000)

                    start_time = await self.extract_start_time(page)

                    # Skip if we couldn't get start time
                    if not start_time:
                        self.log.warning(f"Skipping event {event_url} - no start time")
                        continue

                    # Convert to local timezone for comparison
                    start_time_local = start_time.astimezone(self.local_tz)

                    # Filter: only include games starting today (in local timezone)
                    if not (today_start <= start_time_local < today_end):
                        local_str = start_time_local.strftime("%Y-%m-%d %I:%M %p %Z")
                        self.log.info(f"Skipping event {event_url} - not starting today (starts {local_str})")
                        continue

                    local_str = start_time_local.strftime("%Y-%m-%d %I:%M %p %Z")
                    self.log.info(f"Extracted start time for {event_url}: {start_time} ({local_str})")

                    event_id = event_url.split("-")[-1]
                    away, home = self.extract_team_info(event_url) or (("?", "?"), ("?", "?"))
                    selections = []
                    events.append(Event(event_id, league, event_url, start_time, away, home, selections))

                except Exception as e:
                    self.log.warning(f"Error scraping {event_url}: {e}")
                    continue

            await browser.close()

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


def transform_markets_to_records(event_id: str, league: str, timestamp: str, markets: list[dict]) -> list[dict]:
    """Transform scraped markets into flattened per-selection records."""
    records = []
    for m in markets:
        odds_dec = m.get("odds")
        odds_am = decimal_to_american(odds_dec)
        market_type = m.get("market_type")

        # Parse line/spread numerically if possible
        line_val = None
        line_raw = m.get("line")
        if line_raw:
            # e.g. "+1.5" or "O 44.5" or "U 44.5"
            import re

            nums = re.findall(r"[-+]?\d*\.?\d+", line_raw)
            if nums:
                try:
                    line_val = float(nums[0])
                    if "U" in line_raw:
                        line_val *= -1
                except ValueError:
                    line_val = None

        data = {
            "event_id": event_id,
            "selection_id": m.get("option_id"),
            "market_id": None,
            "market_name": market_type,
            "marketType": market_type,
            "label": m.get("team"),
            "odds_american": odds_am,
            "odds_decimal": odds_dec,
            "spread_or_line": line_val,
            "bet_type": market_type,
            "participant": m.get("team"),
            "match": f"exact:selection:{market_type}",
        }

        record = {
            "created_at": timestamp,
            "data": data,
        }
        records.append(record)
    return records


class Monitor:
    def __init__(self, input_dir: pathlib.Path, concurrency: int = 10) -> None:
        self.input_dir = pathlib.Path(input_dir)
        self.output_dir = self.input_dir
        self.concurrency = concurrency
        self.log = logging.getLogger(self.__class__.__name__)
        self.active_tasks: dict[str, asyncio.Task] = {}
        self.running = False

    async def extract_markets(self, page) -> list[dict[str, Any]]:
        """Extract all betting markets (spread, total, moneyline) from rendered DOM."""
        try:
            markets = await page.evaluate(
                r"""
            () => {
                const result = [];
                
                // Find all option rows (teams)
                const rows = document.querySelectorAll('.option-row');
                
                for (const row of rows) {
                    // Get team name
                    const teamEl = row.querySelector('.six-pack-player-name span');
                    const teamName = teamEl ? teamEl.textContent.trim() : null;
                    
                    // Find all options for this team
                    const options = row.querySelectorAll('ms-option');
                    
                    for (const option of options) {
                        const nameEl = option.querySelector('.name');
                        const oddsEl = option.querySelector('.custom-odds-value-style');
                        const optionId = option.getAttribute('data-test-option-id');
                        
                        if (nameEl && oddsEl) {
                            const name = nameEl.textContent.trim();
                            const odds = oddsEl.textContent.trim();
                            
                            // Determine market type
                            let marketType = null;
                            let line = null;
                            
                            if (/^[+-]\d+\.?\d*$/.test(name)) {
                                marketType = 'spread';
                                line = name;
                            } else if (name.startsWith('O ') || name.startsWith('U ')) {
                                marketType = 'total';
                                line = name;
                            } else if (name === '' || /^\s*$/.test(name)) {
                                marketType = 'moneyline';
                                line = null;
                            }
                            
                            if (marketType) {
                                result.push({
                                    team: teamName,
                                    market_type: marketType,
                                    line: line,
                                    odds: odds,
                                    option_id: optionId
                                });
                            }
                        }
                    }
                }
                
                return result;
            }
            """
            )

            return markets

        except Exception as e:
            self.log.warning(f"Could not extract markets: {e}")
            return []

    async def monitor_event(self, event: Event):
        """Monitor a single event and stream odds changes to file, with heartbeat logs."""
        event_id = event.event_id
        league = event.league
        self.log.info(f"Starting monitor for {event}")

        output_file = odds_filepath(self.output_dir, league, event_id)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        headers = {}
        start_time_wall = time.time()
        last_event_time = time.time()
        event_count = 0
        event_log_interval = 10  # log every 10 iterations
        max_idle_seconds = 120  # stop if 2 minutes with no updates

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(locale="en-US", extra_http_headers=headers)
            page = await context.new_page()
            await page.goto(event.url, wait_until="domcontentloaded", timeout=60000)

            self.log.info(f"Monitoring loop started for {event_id}...")

            with open(output_file, "a") as f:
                while self.running:
                    try:
                        # cancellation support
                        if asyncio.current_task() and asyncio.current_task().cancelled():
                            self.log.info(f"Runner for {event} cancelled, shutting down.")
                            break

                        await page.reload(wait_until="domcontentloaded", timeout=30000)
                        markets = await self.extract_markets(page)
                        timestamp = dt.datetime.now(dt.timezone.utc).isoformat()

                        if markets:
                            records = transform_markets_to_records(event_id, league, timestamp, markets)
                            for rec in records:
                                f.write(json.dumps(rec) + "\n")
                            f.flush()
                            last_event_time = time.time()
                            event_count += 1

                            # DK-style heartbeat
                            if event_count % event_log_interval == 0:
                                now_utc = dt.datetime.now(dt.timezone.utc)
                                worker_runtime_min = (time.time() - start_time_wall) / 60.0
                                event_runtime_min = max((now_utc - event.start_time).total_seconds() / 60.0, 0.1)
                                per_min = event_count / event_runtime_min
                                per_sec = event_count / max((time.time() - start_time_wall), 0.1)
                                self.log.info(
                                    f"{CYAN}{event}\t{worker_runtime_min:,.2f} worker mins."
                                    f"\t{event_runtime_min:,.0f} event mins."
                                    f"\t{per_min:,.2f} rec/min."
                                    f"\t{event_count:,.0f} recs"
                                    f"\t{per_sec:.1f} rec/sec.{RESET}"
                                )

                        # idle timeout
                        if time.time() - last_event_time > max_idle_seconds:
                            self.log.info(f"No updates for 2 minutes; assuming {event} has ended.")
                            break

                        await asyncio.sleep(DEFAULT_LOOP_INTERVAL)

                    except Exception as e:
                        self.log.warning(f"Error monitoring {event_id}: {e}")
                        await asyncio.sleep(DEFAULT_LOOP_INTERVAL)

            await browser.close()
            self.log.info(f"Stopped monitor for {event}")

    def _get_today_files(self) -> list[pathlib.Path]:
        today = dt.datetime.now().strftime("%Y%m%d")
        return sorted(self.input_dir.glob(f"*-{today}.json"))

    async def run_once(self) -> None:
        self.running = True
        semaphore = asyncio.Semaphore(self.concurrency)
        files = self._get_today_files()

        if not files:
            self.log.warning("No event files found for today.")
            return

        for file_path in files:
            league, games = load_events(file_path)
            started = [g for g in games if g.has_started() and not g.is_finished()]

            # Normalize keys for active task map
            existing_keys = set(self.active_tasks.keys())

            async def monitor_with_semaphore(event):
                async with semaphore:
                    await self.monitor_event(event)

            for game in started:
                key = f"{game.league}:{game.event_id}"
                if key in existing_keys:
                    # Prevent double-starts even if run_once() re-enters quickly
                    self.log.debug(f"Skipping already active event {key}")
                    continue

                self.log.info(f"Starting monitor for {game}")
                task = asyncio.create_task(monitor_with_semaphore(game))
                self.active_tasks[key] = task
                existing_keys.add(key)


async def main() -> None:
    parser = argparse.ArgumentParser(description="MGM Event Monitor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    scrape_p = sub.add_parser("scrape", help="Scrape today's events from MGM")
    scrape_p.add_argument("-o", "--output-dir", required=True, type=pathlib.Path)

    monitor_p = sub.add_parser("monitor", help="Monitor live MGM events")
    monitor_p.add_argument("--input-dir", required=True, type=pathlib.Path)

    args = parser.parse_args()

    if args.cmd == "scrape":
        scraper = EventScraper()
        await scraper.scrape_and_save_all(args.output_dir)

    elif args.cmd == "monitor":
        monitor = Monitor(args.input_dir)
        logger.info("Starting monitors for today's events...")
        await monitor.run_once()  # start all event runners
        logger.info("All monitors started. Running indefinitely...")

        # Keep process alive (monitors handle their own loop)
        while True:
            await asyncio.sleep(3600)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully.")
