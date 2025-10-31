from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import pathlib
import re
import time
import zoneinfo
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse

from playwright.async_api import async_playwright
from playwright_stealth import Stealth
from tzlocal import get_localzone

from snuper.constants import (
    YELLOW,
    MGM_DEFAULT_MONITOR_INTERVAL,
    MGM_MONITOR_SWEEP_INTERVAL,
    MGM_EVENT_LOG_INTERVAL,
    MGM_LEAGUE_URLS,
    MAX_IDLE_SECONDS,
    MGM_HEARTBEAT_SECONDS,
    MGM_NBA_TEAMS,
    MGM_NFL_TEAMS,
    MGM_MLB_TEAMS,
    MGM_PAGE_LOAD_TIME,
    MAX_RUNNER_ERRORS,
    League,
)
from snuper.runner import BaseMonitor, BaseRunner
from snuper.scraper import BaseEventScraper, ScrapeContext
from snuper.t import Event, Selection, SelectionChange
from snuper.utils import (
    configure_colored_logger,
    decimal_to_american,
    format_bytes,
    format_duration,
    format_rate_per_sec,
    odds_filepath,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s]: %(message)s",
)
logger = configure_colored_logger(__name__, YELLOW)


CDS_API_BASE = "https://www.co.betmgm.com/cds-api/bettingoffer/fixture-view"
CDS_ACCESS_ID = "OTU4NDk3MzEtOTAyNS00MjQzLWIxNWEtNTI2MjdhNWM3Zjk3"  # static token from site


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
            await page.wait_for_load_state("domcontentloaded")
            await page.wait_for_timeout(5000)
            await page.wait_for_selector(".event-time span.date", timeout=15000)
            await page.wait_for_selector(".event-time span.time", timeout=15000)

            # Extract text contents via JS
            date_time_texts = await page.evaluate(
                """
                () => {
                    const dateEl = document.querySelector('.event-time span.date');
                    const timeEl = document.querySelector('.event-time span.time');
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
        """Return today's BetMGM events for the requested league (robust interception version)."""
        league = context.league
        base_url = MGM_LEAGUE_URLS.get(league)
        if not base_url:
            raise ValueError(f"{self.__class__.__name__} - unsupported league: {league}")

        self.log.info(
            "%s - scraping league '%s', detected local timezone: %s",
            self.__class__.__name__,
            league,
            self.local_tz,
        )

        events: list[Event] = []

        async with Stealth().use_async(async_playwright()) as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(
                locale="en-US",
                user_agent=(
                    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                timezone_id=getattr(self.local_tz, "key", str(self.local_tz)),
            )
            page = await context.new_page()

            self.log.info("%s - navigating to %s", self.__class__.__name__, base_url)
            await page.goto(base_url, wait_until="domcontentloaded", timeout=MGM_PAGE_LOAD_TIME)
            await page.wait_for_timeout(MGM_PAGE_LOAD_TIME)

            # Collect all event URLs
            hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.getAttribute('href'))")
            event_paths = sorted(set(h for h in hrefs if h and self.pattern_event_path.match(h)))
            event_urls = [self.base_domain + path for path in event_paths]

            # Filter per-league
            if league == League.NBA.value:
                event_urls = [x for x in event_urls if self._is_nba_game(x)]
            elif league == League.MLB.value:
                event_urls = [x for x in event_urls if self._is_mlb_game(x)]
            elif league == League.NFL.value:
                event_urls = [x for x in event_urls if self._is_nfl_game(x)]
            else:
                raise ValueError(f"{self.__class__.__name__} - unsupported league: {league}")

            self.log.info("%s - found %d event URLs", self.__class__.__name__, len(event_urls))

            # Define date window
            now_local = dt.datetime.now(self.local_tz)
            today_start = now_local.replace(hour=0, minute=0, second=0, microsecond=0)
            today_end = today_start + dt.timedelta(days=1)

            # Iterate events and intercept their fixture JSONs
            for event_url in event_urls:
                event_id = event_url.split("-")[-1]
                fixture_data = {}

                async def capture_fixture_response(
                    response,
                    *,
                    expected_event_id: str = event_id,
                    target_fixture: dict[str, Any] = None,
                ) -> None:
                    url = response.url
                    if "fixture-view" in url and f"fixtureIds={expected_event_id}" in url:
                        try:
                            data = await response.json()
                            target_fixture.update(data)
                            self.log.info(
                                "%s - captured fixture JSON for %s",
                                self.__class__.__name__,
                                expected_event_id,
                            )
                        except Exception as e:
                            self.log.warning(
                                "%s - failed to decode fixture %s: %s",
                                self.__class__.__name__,
                                expected_event_id,
                                e,
                            )

                page.on("response", capture_fixture_response)

                try:
                    self.log.info("%s - navigating to event: %s", self.__class__.__name__, event_url)
                    await page.goto(event_url, wait_until="domcontentloaded", timeout=45000)
                    await page.wait_for_timeout(4000)  # allow lazy requests

                    # Wait for capture or timeout
                    for _ in range(10):
                        if fixture_data:
                            break
                        await asyncio.sleep(1)

                    if not fixture_data:
                        self.log.warning("%s - no fixture JSON captured for %s", self.__class__.__name__, event_id)
                        continue

                    fixture = fixture_data.get("fixtures", [{}])[0]
                    start_str = fixture.get("startDate")
                    if not start_str:
                        self.log.warning("%s - fixture %s missing startDate", self.__class__.__name__, event_id)
                        continue

                    start_time = dt.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
                    start_local = start_time.astimezone(self.local_tz)
                    if not today_start <= start_local < today_end:
                        self.log.info("%s - skipping %s (starts %s)", self.__class__.__name__, event_id, start_local)
                        continue

                    participants = fixture.get("participants", [])
                    if len(participants) >= 2:
                        away_name = participants[0]["name"]
                        home_name = participants[1]["name"]
                    else:
                        away_name, home_name = "?", "?"

                    # Extract selections
                    selections = []
                    for market in fixture.get("optionMarkets", []):
                        mname = market.get("name", {}).get("value")
                        for opt in market.get("options", []):
                            price = opt.get("price", {})
                            selections.append(
                                {
                                    "selection_id": opt.get("id"),
                                    "market_id": market.get("id"),
                                    "event_id": event_id,
                                    "market_name": mname,
                                    "marketType": mname,
                                    "label": opt.get("name", {}).get("value"),
                                    "odds_american": price.get("americanOdds"),
                                    "odds_decimal": price.get("odds"),
                                    "participant": opt.get("name", {}).get("value"),
                                }
                            )

                    events.append(
                        Event(
                            event_id,
                            league,
                            event_url,
                            start_time,
                            (away_name, ""),
                            (home_name, ""),
                            selections,
                        )
                    )
                    self.log.info("%s - added %s (%d selections)", self.__class__.__name__, event_id, len(selections))

                except Exception as e:
                    self.log.warning("%s - error processing %s: %s", self.__class__.__name__, event_url, e)
                    continue

            await browser.close()

        self.log.info("%s - total %d events for today in %s.", self.__class__.__name__, len(events), league.upper())
        return events


def transform_markets_to_records(event: Event, markets: list[dict]) -> list[SelectionChange]:
    """Flatten scraped market rows into typed selection change records."""

    changes: list[SelectionChange] = []
    for market in markets:
        odds_dec = market.get("odds")
        odds_am = decimal_to_american(odds_dec)
        market_type = market.get("market_type")

        # Parse line/spread numerically if possible
        line_val = None
        line_raw = market.get("line")
        if line_raw:
            # e.g. "+1.5" or "O 44.5" or "U 44.5"
            nums = re.findall(r"[-+]?\d*\.?\d+", line_raw)
            if nums:
                try:
                    line_val = float(nums[0])
                    if "U" in line_raw:
                        line_val *= -1
                except ValueError:
                    line_val = None

        selection_data = {
            "selection_id": market.get("option_id"),
            "market_id": None,
            "market_name": market_type,
            "marketType": market_type,
            "label": market.get("team"),
            "odds_american": odds_am,
            "odds_decimal": odds_dec,
            "spread_or_line": line_val,
            "bet_type": market_type,
            "participant": market.get("team"),
            "match": f"exact:selection:{market_type}",
        }

        selection = Selection(event.event_id, selection_data)
        changes.append(SelectionChange(event.game_label(), selection))

    return changes


class BetMGMRunner(BaseRunner):
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
        total_bytes = 0
        self.reset_heartbeat()

        def current_stats() -> dict[str, Any]:
            return {
                "start_wall": start_time_wall,
                "event_count": event_count,
                "hits": hits,
                "event_start": event.start_time,
                "bytes_received": total_bytes,
            }

        async with Stealth().use_async(async_playwright()) as p:
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

            with open(output_file, "a", encoding="utf-8") as f:
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
                            changes = transform_markets_to_records(event, markets)
                            for change in changes:
                                hits += 1
                                payload = change.to_json()
                                total_bytes += len(payload.encode("utf-8")) + 1
                                f.write(payload + "\n")
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
        """Emit a one-line JSON payload describing polling throughput."""

        start_wall = stats.get("start_wall", time.time())
        event_count = int(stats.get("event_count", 0))
        hits = int(stats.get("hits", 0))
        bytes_received = int(stats.get("bytes_received", 0))
        event_start: dt.datetime = stats.get("event_start", event.start_time)

        now = time.time()
        now_utc = dt.datetime.now(dt.timezone.utc)
        worker_elapsed = max(now - start_wall, 0.0)
        event_elapsed = max((now_utc - event_start).total_seconds(), 0.0)

        payload = {
            "event": str(event),
            "worker_time": format_duration(worker_elapsed),
            "event_runtime": format_duration(event_elapsed),
            "msgs_rcvd": f"{event_count:,d}",
            "msgs_rcvd_per_sec": format_rate_per_sec(event_count, worker_elapsed),
            "total_bytes_rcvd": format_bytes(bytes_received),
            "odds_rcvd": f"{hits:,d}",
            "odds_rcvd_per_sec": format_rate_per_sec(hits, worker_elapsed),
        }

        self.log.info("%s - %s", self.__class__.__name__, json.dumps(payload, separators=(",", ":")))


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
            BetMGMRunner(),
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
