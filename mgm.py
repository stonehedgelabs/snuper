from __future__ import annotations
import asyncio
import argparse
import datetime as dt
import json
import logging
import os
import re
import time
import pathlib
from typing import Any
from urllib.parse import urlparse, unquote

import httpx
from playwright.async_api import async_playwright
from tzlocal import get_localzone

from event_monitor.constants import (
    CYAN,
    RED,
    RESET,
    MGM_DEFAULT_MONITOR_INTERVAL,
    MGM_EVENT_LOG_INTERVAL,
    MGM_LEAGUE_URLS,
    MGM_MAX_IDLE_SECONDS,
    MGM_HEARTBEAT_SECONDS,
    MGM_NBA_TEAMS,
)
from event_monitor.runner import BaseMonitor, BaseRunner
from event_monitor.scraper import BaseEventScraper, odds_filepath
from event_monitor.t import Event
from event_monitor.utils import decimal_to_american

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


class EventScraper(BaseEventScraper):
    """Scrape BetMGM pages to assemble daily event snapshots."""

    def __init__(self) -> None:
        """Initialise timezone context, patterns, and league list."""
        super().__init__(list(MGM_LEAGUE_URLS.keys()))
        self.local_tz = get_localzone()
        self.pattern_event_path = re.compile(
            r"^/en/sports/events/[a-z0-9\-@%]+-\d+$",
            re.IGNORECASE,
        )
        self.base_domain = "https://www.co.betmgm.com"

    async def extract_start_time(self, page):
        """Parse the BetMGM DOM to determine the event's UTC start time."""
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
        """Return True when the event URL references an NBA matchup."""
        url_lower = event_url.lower()
        return any(team in url_lower for team in MGM_NBA_TEAMS)

    def extract_team_info(self, event_url: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
        """Split the MGM slug into (away, home) team tokens."""
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
            """Normalise a single team slug into short and full names."""
            tokens = part.strip("-").split("-")
            if len(tokens) >= 2:
                city = tokens[0].lower()
                mascot = "-".join(tokens[1:]).lower()
                return city, mascot
            return tokens[0].lower(), ""

        return _parse_team(away_part), _parse_team(home_part)

    async def scrape_today(self, league: str) -> list[Event]:
        """Return today's BetMGM events for the requested league."""
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
                    away, home = self.extract_team_info(event_url) or (
                        ("?", "?"),
                        ("?", "?"),
                    )
                    selections = []
                    events.append(
                        Event(
                            event_id,
                            league,
                            event_url,
                            start_time,
                            away,
                            home,
                            selections,
                        )
                    )

                except Exception as e:
                    self.log.warning(f"Error scraping {event_url}: {e}")
                    continue

            await browser.close()

        self.log.info(f"Total {len(events)} events for today in {league.upper()}.")
        return events


def transform_markets_to_records(event_id: str, league: str, timestamp: str, markets: list[dict]) -> list[dict]:
    """Flatten scraped market rows into JSONL selection records."""
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


class PollingRunner(BaseRunner):
    """Poll BetMGM event pages for odds updates on an interval."""

    def __init__(self) -> None:
        """Set defaults for polling cadence and logging intervals."""
        super().__init__()
        self.loop_interval = MGM_DEFAULT_MONITOR_INTERVAL
        self.event_log_interval = MGM_EVENT_LOG_INTERVAL
        self.max_idle_seconds = MGM_MAX_IDLE_SECONDS

    async def extract_markets(self, page) -> list[dict[str, Any]]:
        """Pull structured market rows from the rendered BetMGM DOM."""
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

    async def run(self, event: Event, output_dir: pathlib.Path, league: str) -> None:
        """Poll the BetMGM page for ``event`` and append odds snapshots."""

        event_id = event.event_id
        self.log.info(f"Starting monitor for {event}")

        output_file = odds_filepath(output_dir, league, event_id)
        output_file.parent.mkdir(parents=True, exist_ok=True)

        headers = {}
        start_time_wall = time.time()
        last_event_time = time.time()
        event_count = 0
        hits = 0
        self.reset_heartbeat()

        def current_stats() -> dict[str, Any]:
            return {
                "start_wall": start_time_wall,
                "event_count": event_count,
                "hits": hits,
                "event_start": event.start_time,
            }

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(locale="en-US", extra_http_headers=headers)
            page = await context.new_page()
            await page.goto(event.url, wait_until="domcontentloaded", timeout=60000)

            self.log.info(f"Monitoring loop started for {event}...")

            with open(output_file, "a") as f:
                while True:
                    try:
                        if asyncio.current_task() and asyncio.current_task().cancelled():
                            self.log.info(f"Runner for {event} cancelled, shutting down.")
                            break

                        await page.reload(wait_until="domcontentloaded", timeout=30000)
                        markets = await self.extract_markets(page)
                        timestamp = dt.datetime.now(dt.timezone.utc).isoformat()

                        if markets:
                            records = transform_markets_to_records(event_id, league, timestamp, markets)
                            for rec in records:
                                hits += 1
                                f.write(json.dumps(rec) + "\n")
                            f.flush()
                            last_event_time = time.time()
                            event_count += 1

                        stats = current_stats()
                        if event_count % self.event_log_interval == 0 and event_count > 0:
                            self.maybe_emit_heartbeat(event, force=True, stats=stats)

                        if time.time() - last_event_time > self.max_idle_seconds:
                            self.log.info(f"No updates for 2 minutes; assuming {event} has ended.")
                            self.maybe_emit_heartbeat(event, force=True, stats=stats)
                            break

                        self.maybe_emit_heartbeat(event, stats=stats)
                        await asyncio.sleep(self.loop_interval)

                    except Exception as e:
                        self.log.warning(f"Error monitoring {event}: {e}")
                        self.maybe_emit_heartbeat(event, stats=current_stats())
                        await asyncio.sleep(self.loop_interval)

            await browser.close()
            self.log.info(f"Stopped monitor for {event}")

    def emit_heartbeat(self, event: Event, *, stats: dict[str, Any]) -> None:
        """Log polling throughput using shared formatting."""

        start_wall = stats.get("start_wall", time.time())
        event_count = stats.get("event_count", 0)
        event_start: dt.datetime = stats.get("event_start", event.start_time)

        now = time.time()
        now_utc = dt.datetime.now(dt.timezone.utc)
        worker_runtime = (now - start_wall) / 60.0
        event_runtime = max((now_utc - event_start).total_seconds() / 60.0, 0.1)
        per_min = event_count / event_runtime if event_runtime else 0.0
        per_sec = event_count / max((now - start_wall), 0.1)
        hits = stats["hits"]
        self.log.info(
            f"{CYAN}{event}\t\t{worker_runtime:,.2f} worker mins."
            f"\t{event_runtime:,.0f} event mins."
            f"\t{per_min:,.2f} msgs/min."
            f"\t{hits} hits"
            f"\t{event_count:,.0f} msgs"
            f"\t{per_sec:.1f} msgs/sec.{RESET}"
        )


class Monitor(BaseMonitor):
    """Coordinate BetMGM polling runners based on saved snapshots."""

    def __init__(self, input_dir: pathlib.Path, concurrency: int = 10) -> None:
        """Initialise monitor state with snapshot directory and policy."""
        super().__init__(input_dir, PollingRunner(), concurrency=concurrency)
        self.log.info("Monitor using input directory at %s", self.input_dir)


async def main() -> None:
    """CLI entrypoint for scraping or monitoring BetMGM markets."""
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
