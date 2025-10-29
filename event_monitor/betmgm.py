from __future__ import annotations
import asyncio
import datetime as dt
import json
import logging
import os
import zoneinfo
import re
import time
import pathlib
from typing import Any, Sequence
from urllib.parse import urlparse, unquote

import httpx
from playwright.async_api import async_playwright
from tzlocal import get_localzone

from event_monitor.constants import (
    YELLOW,
    RED,
    RESET,
    MGM_DEFAULT_MONITOR_INTERVAL,
    MGM_MONITOR_SWEEP_INTERVAL,
    MGM_EVENT_LOG_INTERVAL,
    MGM_LEAGUE_URLS,
    MAX_IDLE_SECONDS,
    MGM_HEARTBEAT_SECONDS,
    MGM_NBA_TEAMS,
    MGM_NFL_TEAMS,
    MGM_MLB_TEAMS,
    MAX_RUNNER_ERRORS,
)
from event_monitor.runner import BaseMonitor, BaseRunner
from event_monitor.scraper import BaseEventScraper, ScrapeContext
from event_monitor.t import Event
from event_monitor.utils import configure_colored_logger, decimal_to_american, odds_filepath

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s]: %(message)s",
)
logger = configure_colored_logger(__name__, YELLOW)


class BetMGMEventScraper(BaseEventScraper):
    """Scrape BetMGM pages to assemble daily event snapshots."""

    def __init__(self, output_dir: pathlib.Path | str | None = None) -> None:
        """Initialise timezone context, patterns, and league list."""
        super().__init__(list(MGM_LEAGUE_URLS.keys()), output_dir=output_dir)
        self.local_tz = get_localzone()
        self.pattern_event_path = re.compile(
            r"^/en/sports/events/[a-z0-9\-@%]+-\d+$",
            re.IGNORECASE,
        )
        self.base_domain = "https://www.co.betmgm.com"

    async def extract_start_time(self, page):
        """Parse the BetMGM DOM to determine the event's UTC start time."""
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

            self.log.info("%s - MGM extracted date='%s', time='%s'", self.__class__.__name__, date_text, time_text)

            if not date_text or not time_text:
                raise ValueError("Missing date or time text")

            # BetMGM shows times in UTC to Playwright, so we interpret as UTC
            utc_tz = zoneinfo.ZoneInfo("UTC")
            now_utc = dt.datetime.now(utc_tz)

            # Interpret the date string (relative to UTC "now")
            event_date = None
            if date_text.lower() == "today":
                event_date = now_utc.date()
            elif date_text.lower() == "tomorrow":
                event_date = (now_utc + dt.timedelta(days=1)).date()
            elif "/" in date_text:
                try:
                    event_date = dt.datetime.strptime(date_text, "%m/%d/%y").date()
                except ValueError:
                    event_date = dt.datetime.strptime(date_text, "%m/%d/%Y").date()
            else:
                for fmt in ("%a %d %b", "%b %d", "%a, %b %d"):
                    try:
                        event_date = dt.datetime.strptime(date_text, fmt).replace(year=now_utc.year).date()
                        break
                    except ValueError:
                        continue
                else:
                    raise ValueError(f"Unrecognized date format: {date_text}")

            # Parse time (e.g. "12:08 AM")
            event_time = dt.datetime.strptime(time_text, "%I:%M %p").time()

            # Combine into UTC datetime (since page shows UTC times)
            utc_dt = dt.datetime.combine(event_date, event_time, tzinfo=utc_tz)

            self.log.info("%s - converted MGM time to UTC: %s", self.__class__.__name__, utc_dt)
            return utc_dt

        except Exception as e:
            self.log.warning("%s - could not extract MGM start time: %s", self.__class__.__name__, e)
            return None

    def _is_nba_game(self, event_url: str) -> bool:
        """Return True when the event URL references an NBA matchup."""
        url_lower = event_url.lower()
        return any(team in url_lower for team in MGM_NBA_TEAMS)

    def _is_nfl_game(self, event_url: str) -> bool:
        """Return True when the event URL references an NFL matchup."""
        url_lower = event_url.lower()
        return any(team in url_lower for team in MGM_NFL_TEAMS)

    def _is_mlb_game(self, event_url: str) -> bool:
        """Return True when the event URL references an MLB matchup."""
        url_lower = event_url.lower()
        return any(team in url_lower for team in MGM_MLB_TEAMS)

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

    async def scrape_today(
        self,
        context: ScrapeContext,
        source_events: Sequence[Event] | None = None,
    ) -> list[Event]:
        """Return today's BetMGM events for the requested league."""
        league = context.league
        base_url = MGM_LEAGUE_URLS.get(league.lower())
        if not base_url:
            raise ValueError(f"{self.__class__.__name__} - unsupported league: {league}")

        self.log.info(
            "%s - scraping league '%s', detected local timezone: %s", self.__class__.__name__, league, self.local_tz
        )
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
            context = await browser.new_context(
                locale="en-US",
                timezone_id="America/Los_Angeles",
                extra_http_headers=headers,
            )
            page = await context.new_page()

            self.log.info("%s - navigating to %s", self.__class__.__name__, base_url)
            await page.goto(base_url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(10_000)
            # hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
            hrefs = await page.evaluate(
                """
() => {
  const links = new Set();

  // Normal <a href="..."> elements (resolved)
  document.querySelectorAll('a[href]').forEach(e => {
    if (e.href) links.add(e.href);
  });

  // Raw DOM attributes (some React links might set only data-href)
  document.querySelectorAll('a').forEach(e => {
    const raw = e.getAttribute('href');
    if (raw && !raw.startsWith('javascript')) links.add(new URL(raw, document.baseURI).href);
  });

  // Elements with data-href or role=link (common in lazy-hydrated UIs)
  document.querySelectorAll('[data-href], [role="link"]').forEach(e => {
    const raw = e.getAttribute('data-href');
    if (raw) links.add(new URL(raw, document.baseURI).href);
  });

  return Array.from(links);
}
"""
            )
            event_paths = sorted(set(h for h in hrefs if h and self.pattern_event_path.match(h)))
            event_urls = [self.base_domain + path for path in event_paths]
            if league == "nba":
                event_urls = list(filter(lambda x: self._is_nba_game(x), event_urls))
            elif league == "mlb":
                event_urls = list(filter(lambda x: self._is_mlb_game(x), event_urls))
            elif league == "nfl":
                event_urls = list(filter(lambda x: self._is_nfl_game(x), event_urls))
            else:
                raise ValueError(f"{self.__class__.__name__} - unsupported league: {league}")

            self.log.info("%s - found %d event URLs on %s", self.__class__.__name__, len(event_urls), base_url)

            # Get today's date range in local timezone
            now_local = dt.datetime.now(self.local_tz)
            today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = today_start + dt.timedelta(days=1)
            self.log.info(
                "%s - MGM date window: Now(%s), Start(%s), End(%s)",
                self.__class__.__name__,
                now_local,
                today_start,
                today_end,
            )

            for event_url in event_urls:
                try:
                    self.log.info("%s - navigating to event: %s", self.__class__.__name__, event_url)
                    await page.goto(event_url, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(2000)

                    start_time = await self.extract_start_time(page)

                    # Skip if we couldn't get start time
                    if not start_time:
                        self.log.warning("%s - skipping event %s - no start time", self.__class__.__name__, event_url)
                        continue

                    # Convert to local timezone for comparison
                    start_time_local = start_time.astimezone(self.local_tz)
                    self.log.info(
                        "%s - MGM start time conversion: UTC(%s) -> Local(%s)",
                        self.__class__.__name__,
                        start_time,
                        start_time_local,
                    )

                    # Filter: only include games starting today (in local timezone)
                    if not (today_start <= start_time_local < today_end):
                        local_str = start_time_local.strftime("%Y-%m-%d %I:%M %p %Z")
                        self.log.info(
                            "%s - skipping event %s - not starting today (starts %s)",
                            self.__class__.__name__,
                            event_url,
                            local_str,
                        )
                        continue

                    local_str = start_time_local.strftime("%Y-%m-%d %I:%M %p %Z")
                    self.log.info(
                        "%s - extracted start time for %s: %s (%s)",
                        self.__class__.__name__,
                        event_url,
                        start_time,
                        local_str,
                    )

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
                    self.log.warning("%s - error scraping %s: %s", self.__class__.__name__, event_url, e)
                    continue

            await browser.close()

        self.log.info("%s - total %d events for today in %s.", self.__class__.__name__, len(events), league.upper())
        return events


def transform_markets_to_records(event: Event, markets: list[dict]) -> list[dict]:
    """Flatten scraped market rows into JSONL selection records."""

    timestamp = dt.datetime.now(dt.timezone.utc).isoformat()
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
            "event_id": event.event_id,
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
            "label": event.game_label(),
            "data": data,
        }
        records.append(record)
    return records


class PollingRunner(BaseRunner):
    """Poll BetMGM event pages for odds updates on an interval."""

    def __init__(self) -> None:
        """Set defaults for polling cadence and logging intervals."""
        super().__init__(
            heartbeat_interval=MGM_HEARTBEAT_SECONDS,
            log_color=YELLOW,
        )
        self.loop_interval = MGM_DEFAULT_MONITOR_INTERVAL
        self.event_log_interval = MGM_EVENT_LOG_INTERVAL
        self.max_idle_seconds = MAX_IDLE_SECONDS

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
            self.log.warning("%s - could not extract markets: %s", self.__class__.__name__, e)
            return []

    async def run(self, event: Event, output_dir: pathlib.Path, league: str) -> None:
        """Poll the BetMGM page for ``event`` and append odds snapshots."""

        event_id = event.event_id
        self.log.info("%s - starting monitor for %s", self.__class__.__name__, event)

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
            context = await browser.new_context(
                locale="en-US",
                extra_http_headers=headers,
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/118.0.5993.117 Safari/537.36"
                ),
                viewport={"width": 1366, "height": 768},
            )
            page = await context.new_page()
            await page.goto(event.url, wait_until="domcontentloaded", timeout=60000)

            self.log.info("%s - monitoring loop started for %s...", self.__class__.__name__, event)

            with open(output_file, "a") as f:
                error_count = 0
                while True:
                    try:
                        if asyncio.current_task() and asyncio.current_task().cancelled():
                            self.log.info(
                                "%s - runner for %s cancelled, shutting down.", self.__class__.__name__, event
                            )
                            break

                        await page.reload(wait_until="domcontentloaded", timeout=30000)
                        markets = await self.extract_markets(page)

                        if markets:
                            records = transform_markets_to_records(event, markets)
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
                            self.log.info(
                                "%s - no updates for %d seconds; assuming %s has ended.",
                                self.__class__.__name__,
                                MAX_IDLE_SECONDS,
                                event,
                            )
                            self.maybe_emit_heartbeat(event, force=True, stats=stats)
                            break

                        self.maybe_emit_heartbeat(event, stats=stats)
                        await asyncio.sleep(self.loop_interval)
                        error_count = 0

                    except Exception as e:
                        error_count += 1
                        self.log.warning("%s - error monitoring %s: %s", self.__class__.__name__, event, e)
                        current = current_stats()
                        self.maybe_emit_heartbeat(event, stats=current)
                        if error_count > MAX_RUNNER_ERRORS:
                            self.log.error(
                                "%s - assuming game ended for %s after %d consecutive errors.",
                                self.__class__.__name__,
                                event,
                                error_count,
                            )
                            self.maybe_emit_heartbeat(event, force=True, stats=current)
                            break
                        await asyncio.sleep(self.loop_interval)

                        if time.time() - last_event_time > self.max_idle_seconds:
                            self.log.info(
                                "%s - no updates for 2 minutes; assuming %s has ended.", self.__class__.__name__, event
                            )
                            self.maybe_emit_heartbeat(event, force=True, stats=current)
                            break

            await browser.close()
            self.log.info("%s - stopped monitor for %s", self.__class__.__name__, event)

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
            "%s%s\t%.2f worker mins.\t%.0f event mins.\t%.2f msgs/event min.\t%d hits\t%.0f msgs\t\t%.1f msgs/sec.%s",
            YELLOW,
            event,
            worker_runtime,
            event_runtime,
            per_min,
            hits,
            event_count,
            per_sec,
            RESET,
        )


class BetMGMMonitor(BaseMonitor):
    """Coordinate BetMGM polling runners based on saved snapshots."""

    def __init__(
        self,
        input_dir: pathlib.Path,
        concurrency: int = 10,
        *,
        leagues: Sequence[str] | None = None,
    ) -> None:
        """Initialise monitor state with snapshot directory and policy."""
        super().__init__(
            input_dir,
            PollingRunner(),
            concurrency=concurrency,
            log_color=YELLOW,
            leagues=leagues,
        )
        self.log.info("%s - monitor using input directory at %s", self.__class__.__name__, self.input_dir)


async def run_scrape(
    output_dir: pathlib.Path,
    *,
    leagues: Sequence[str] | None = None,
    overwrite: bool = False,
) -> None:
    """Invoke the BetMGM scraper with the supplied destination."""

    scraper = BetMGMEventScraper(output_dir)
    await scraper.scrape_and_save_all(output_dir, leagues=leagues, overwrite=overwrite)


async def run_monitor(
    input_dir: pathlib.Path,
    *,
    interval: int = MGM_MONITOR_SWEEP_INTERVAL,
    leagues: Sequence[str] | None = None,
) -> None:
    """Start BetMGM monitors and continue refreshing snapshot awareness."""

    monitor = BetMGMMonitor(input_dir, leagues=leagues)
    logger.info("starting BetMGM monitor loop (interval=%ss)...", interval)

    while True:
        try:
            await monitor.run_once()
        except Exception as exc:  # pragma: no cover - guard for runtime errors
            logger.error("monitor sweep failed: %s", exc)
        logger.info("sleeping %ss before next sweep...", interval)
        await asyncio.sleep(interval)
