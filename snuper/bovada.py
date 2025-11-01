from __future__ import annotations

import asyncio
import datetime as dt
import json
import logging
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import httpx
import websockets

from snuper.sinks import SelectionSink
from snuper.constants import (
    CYAN,
    SUPPORTED_LEAGUES,
    MGM_NBA_TEAMS,
    MGM_MLB_TEAMS,
    MGM_NFL_TEAMS,
    League,
    MAX_RUNNER_ERRORS,
    BOVADA_EVENT_LOG_INTERVAL,
    BOVADA_HEARTBEAT_SECONDS,
    BOVADA_MAX_TIME_SINCE_LAST_EVENT,
)
from snuper.runner import BaseMonitor, BaseRunner
from snuper.scraper import BaseEventScraper, ScrapeContext
from snuper.t import Event, Selection, SelectionChange
from snuper.utils import (
    configure_colored_logger,
    format_bytes,
    format_duration,
    format_rate_per_sec,
)

__all__ = ["BovadaEventScraper", "BovadaMonitor", "run_scrape", "run_monitor"]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s]: %(message)s",
)
logger = configure_colored_logger(__name__, CYAN)


all_market_period_descrs = set()
all_market_period_abbrvs = set()
all_market_descriptions = set()

event_headers = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br, zstd",
    "accept-language": "en-US,en;q=0.9",
    "authorization": "Bearer 1ce5795a-eda6-4539-ac1e-5cc4d0eca6fb",
    "cookie": (
        "Device-Type=Desktop|false; "
        "randMktUserId=0|Sat Apr 25 2026; "
        "JOINED=true; bvcusttype=true; "
        "CookieInformationConsent=%7B%22website_uuid%22%3A%22933dcbf3-c7fa-4c19-8051-43897cdc44b7%22%2C%22timestamp%22%3A%222025-10-27T23%3A17%3A09.436Z%22%2C%22consent_url%22%3A%22https%3A%2F%2Fwww.bovada.lv%2Faccount%2Fcashier%2Fdeposit%22%2C%22consent_website%22%3A%22Live%20Environment%20-%20BVD%22%2C%22consent_domain%22%3A%22www.bovada.lv%22%2C%22user_uid%22%3A%22f4d5d70d-505f-4113-a00f-85d48d019244%22%2C%22consents_approved%22%3A%5B%22cookie_cat_necessary%22%2C%22cookie_cat_functional%22%2C%22cookie_cat_statistic%22%2C%22cookie_cat_marketing%22%2C%22cookie_cat_unclassified%22%5D%2C%22consents_denied%22%3A%5B%5D%2C%22user_agent%22%3A%22Mozilla%2F5.0%20%28Macintosh%3B%20Intel%20Mac%20OS%20X%2010_15_7%29%20AppleWebKit%2F537.36%20%28KHTML%2C%20like%20Gecko%29%20Chrome%2F141.0.0.0%20Safari%2F537.36%22%7D; "
        "VISITED=true; LANG=en; AB=variant; ln_grp=2; odds_format=AMERICAN; url-prefix=/; "
        "TS01890ddd030=011c38c90bc89f3d9c3466923d2930cbfb682d51eb2ccea104cb3113b09f833838b4f138556f698c05ee243e746cc3279e4380670b; "
        "audiences=9cdc613a070adc3789c0db493623471b; sid=1ce5795a-eda6-4539-ac1e-5cc4d0eca6fb; "
        "variant=v:0|lgn:1|dt:d|os:mac|cntry:US|cur:USD|jn:1|rt:o|pb:0; st=US:California; "
        "_pk_id.7.639d=d521de9ea84a26c5.1761757329.; _pk_ses.7.639d=1; "
        "JSESSIONID=456C6DA4F346D724B1016705B18260C7; "
        "TS01890ddd=014b5d5d073124b904eb1c1ced2c03463982ac7be9c43e8808e561107f7473462f5fffc69770795066b4f75cae03f5365bde554423084a17a451479c61b45c58cd12807b0aaab11ddb74bd2b1011644b241964d7b0608ed02d413f44826ffa78b8072bfc60f21c105a0d70dc3114464ceeb8ad85a421a56fc8e93916fc26b054ad2e589814ca39d4bf736b0c232e49466f5b25c56f"
    ),
    "priority": "u=1, i",
    "referer": "https://www.bovada.lv/",
    "sec-ch-ua": '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"macOS"',
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "sec-gpc": "1",
    "traceparent": "00-6d8aeea3b993e31102dad32c7c4a2c61-054aa0a746cf5605-00",
    "user-agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/141.0.0.0 Safari/537.36"
    ),
    "x-channel": "desktop",
    "x-sport-context": "BASK",
}


def description_filter(league: str) -> str:
    return "Live Game" if league == League.NBA.value else "Game"


def derive_bovada_timestamp(ts: int) -> dt.datetime:
    dt_utc = dt.datetime.fromtimestamp(ts / 1000, tz=dt.timezone.utc)
    # dt_est = dt_utc.astimezone(pytz.timezone("US/Eastern"))
    return dt_utc.astimezone(dt.timezone.utc)


def is_league_matchup(path: str, league: str) -> bool:
    league = league.lower().strip()
    path = path.lower().strip()

    def extract_mascots(items: set) -> list[str]:
        return [item.split("-")[-1] for item in items]

    # Map league identifiers to team sets
    league_teams = {
        "mlb": extract_mascots(MGM_MLB_TEAMS),
        "nfl": extract_mascots(MGM_NFL_TEAMS),
        "nba": extract_mascots(MGM_NBA_TEAMS),
    }.get(league)

    if not league_teams:
        raise ValueError(f"Unsupported league: {league}")

    # Extract the final slug (e.g. "toronto-blue-jays-los-angeles-dodgers-202510292010")
    try:
        slug = path.split("/")[-1]
    except IndexError:
        return False

    # Remove date/time suffix if present
    parts = slug.split("-")
    if parts and parts[-1].isdigit():
        parts = parts[:-1]
    slug = "-".join(parts)

    # Count how many known teams appear in this slug
    matched_teams = [team for team in league_teams if team in slug]
    return len(matched_teams) == 2


def extract_bovada_odds(event_id: str, data: dict, league: str) -> dict:
    results = []
    for market in data.get("markets", []):
        all_market_period_descrs.add(market.get("period", {}).get("abbreviation"))
        all_market_period_abbrvs.add(market.get("period", {}).get("description"))
        all_market_descriptions.add(market.get("description"))
        if (
            market.get("period", {}).get("description") == description_filter(league)
            and market.get("period", {}).get("abbreviation") == "G"
            and market.get("description") in {"Spread", "Point Spread", "Moneyline"}
        ):
            for outcome in market.get("outcomes", []):
                p = outcome.get("price", {})
                results.append(
                    {
                        "selection_id": outcome.get("id"),
                        "market_id": market.get("id"),
                        "event_id": event_id,
                        "market_name": outcome.get("description"),
                        "marketType": outcome.get("description"),
                        "label": outcome.get("description"),
                        "odds_american": p.get("american"),
                        "odds_decimal": p.get("decimal"),
                        "spread_or_line": p.get("handicap"),
                        "bet_type": market.get("description").lower(),
                        "participant": outcome.get("description"),
                        "match": "market_name",
                    }
                )
    return results


class BovadaEventScraper(BaseEventScraper):
    """Placeholder Bovada scraper awaiting future implementation."""

    def __init__(
        self,
        *,
        output_dir: Path | str | None = None,
        input_dir: Path | str | None = None,
        leagues: Sequence[str] | None = None,
    ) -> None:
        super().__init__(
            tuple(leagues or SUPPORTED_LEAGUES),
            output_dir=output_dir,
            input_dir=input_dir,
        )

    async def scrape_today(
        self,
        context: ScrapeContext,
        source_events: Sequence[Event] | None = None,
    ) -> list[Event]:
        """Fetch today's Bovada full-game events for ``league``."""
        # pylint: disable=too-many-nested-blocks

        url = f"https://www.bovada.lv/services/sports/event/coupon/events/A/description/{context.sport}?marketFilterId=def&preMatchOnly=true&eventsLimit=50&lang=en"
        try:
            response = httpx.get(url, headers=event_headers, timeout=20)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            self.log.error("%s - Failed to fetch Bovada data: %s", self.__class__.__name__, e)
            return []

        results: list[Event] = []

        # Define today's range in local timezone
        now = dt.datetime.now(self.local_tz)
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + dt.timedelta(days=1)

        for group in data:
            for ev in group.get("events", []):
                if not is_league_matchup(ev.get("link"), context.league):
                    self.log.warning(
                        "%s - Event %s is not a part of league %s",
                        self.__class__.__name__,
                        ev.get("link"),
                        context.league,
                    )
                    continue

                try:
                    start_time = derive_bovada_timestamp(ev["startTime"])

                    # Skip events not starting today
                    if not start_of_day <= start_time.astimezone(self.local_tz) < end_of_day:
                        self.log.warning(
                            "%s - date condition *NOT* met StartOfDay(%s) <= StartTime(%s) < EndOfDay(%s)",
                            self.__class__.__name__,
                            start_of_day,
                            start_time.astimezone(self.local_tz),
                            end_of_day,
                        )
                        continue

                    self.log.info(
                        "%s - date condition met StartOfDay(%s) <= StartTime(%s) < EndOfDay(%s)",
                        self.__class__.__name__,
                        start_of_day,
                        start_time.astimezone(self.local_tz),
                        end_of_day,
                    )

                    event_id = ev["id"]
                    competitors = ev.get("competitors", [])

                    # Identify home and away teams
                    home_team = next((c for c in competitors if c.get("home")), {})
                    away_team = next((c for c in competitors if not c.get("home")), {})

                    home_tokens = tuple(home_team.get("name", "").lower().split())
                    away_tokens = tuple(away_team.get("name", "").lower().split())

                    selections = []
                    for dg in ev.get("displayGroups", []):
                        if dg.get("alternateType"):
                            continue  # Skip alternate lines

                        for market in dg.get("markets", []):
                            period = market.get("period", {})
                            if (
                                period.get("description") == description_filter(context.league)
                                and period.get("abbreviation") == "G"
                                and period.get("main", False)
                            ):
                                market_id = market["id"]
                                market_name = market.get("description", "")
                                market_type = market.get("descriptionKey", "")
                                for outcome in market.get("outcomes", []):
                                    price = outcome.get("price", {})
                                    selections.append(
                                        {
                                            "selection_id": outcome.get("id"),
                                            "market_id": market_id,
                                            "event_id": event_id,
                                            "market_name": market_name,
                                            "marketType": market_type,
                                            "label": outcome.get("description"),
                                            "odds_american": price.get("american"),
                                            "odds_decimal": price.get("decimal"),
                                            "participant": outcome.get("description"),
                                            "handicap": price.get("handicap"),
                                        }
                                    )

                    if not selections:
                        self.log.warning("%s - no selections found", self.__class__.__name__)

                    # Build event URL
                    event_link = ev.get("link", "")
                    url = f"https://www.bovada.lv{event_link}"

                    league_name = context.league

                    results.append(
                        Event(
                            event_id,
                            league_name,
                            url,
                            start_time,
                            away_tokens,
                            home_tokens,
                            selections,
                        )
                    )

                except Exception as e:
                    self.log.error(
                        "%s - Failed to parse Bovada event %s: %s",
                        self.__class__.__name__,
                        ev.get("id"),
                        e,
                    )
                    continue

        self.log.info("%s - Retrieved %d Bovada events for today", self.__class__.__name__, len(results))
        return results


class BovadaRunner(BaseRunner):
    """Stream Bovada websocket messages and emit raw payload logs."""

    def __init__(self) -> None:
        super().__init__(heartbeat_interval=BOVADA_HEARTBEAT_SECONDS, log_color=CYAN)
        self.url = "wss://services.bovada.lv/services/sports/subscription/57858489-9422-6222-5982-488405165904?X-Atmosphere-tracking-id=0&X-Atmosphere-Framework=3.1.0-javascript&X-Atmosphere-Transport=websocket"
        self.headers = {
            "accept-encoding": "gzip, deflate, br, zstd",
            "accept-language": "en-US,en;q=0.9",
            "cache-control": "no-cache",
            "connection": "Upgrade",
            "host": "services.bovada.lv",
            "origin": "https://www.bovada.lv",
            "pragma": "no-cache",
            "sec-websocket-extensions": "permessage-deflate; client_max_window_bits",
            "sec-websocket-version": "13",
            "upgrade": "websocket",
            "user-agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/141.0.0.0 Safari/537.36"
            ),
        }

    async def run(
        self,
        event: Event,
        output_dir: Path,
        league: str,
        provider: str,
        sink: SelectionSink,
    ) -> None:
        """Consume the Bovada websocket, subscribe to one event_id, and persist frames via ``sink``."""

        destination = sink.describe_destination(
            provider=provider,
            league=league,
            event=event,
            output_dir=output_dir,
        )
        suffix = f" to {destination}" if destination else ""
        self.log.info(
            "%s - logging odds data for %s%s",
            self.__class__.__name__,
            event,
            suffix,
        )

        start = time.time()
        events = 0
        odds_hits = 0
        total_bytes = 0
        last_event_time = time.time()
        error_count = 0
        self.reset_heartbeat()

        subscribe_payload = f"SUBSCRIBE|A|/events/{event.event_id}.1?delta=true"
        unsubscribe_payload = f"UNSUBSCRIBE|{event.event_id}"

        def current_stats() -> dict[str, Any]:
            return {
                "start": start,
                "events": events,
                "hits": odds_hits,
                "bytes_received": total_bytes,
                "event_start": event.start_time,
            }

        while True:
            task = asyncio.current_task()
            if task and task.cancelled():
                self.log.info(
                    "%s - websocket runner for %s cancelled, shutting down.",
                    self.__class__.__name__,
                    event,
                )
                break

            try:
                self.log.info("%s - connecting websocket for %s", self.__class__.__name__, event)
                async with websockets.connect(
                    self.url,
                    extra_headers=self.headers,
                    max_size=None,
                ) as ws:
                    error_count = 0
                    self.log.info(
                        "%s - connected to Bovada stream for %s",
                        self.__class__.__name__,
                        event,
                    )

                    # Send subscription message for this single event
                    await ws.send(subscribe_payload)
                    self.log.info(
                        "%s - sent subscribe payload: %s",
                        self.__class__.__name__,
                        subscribe_payload,
                    )

                    label = event.game_label()
                    try:
                        while True:
                            if time.time() - last_event_time > BOVADA_MAX_TIME_SINCE_LAST_EVENT:
                                self.log.info(
                                    "%s - no updates for %d seconds; assuming %s has ended.",
                                    self.__class__.__name__,
                                    BOVADA_MAX_TIME_SINCE_LAST_EVENT,
                                    event,
                                )
                                await ws.send(unsubscribe_payload)
                                return

                            task = asyncio.current_task()
                            if task and task.cancelled():
                                self.log.warning(
                                    "%s - websocket runner for %s cancelled mid-loop.",
                                    self.__class__.__name__,
                                    event,
                                )
                                await ws.send(unsubscribe_payload)
                                return

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                            except asyncio.TimeoutError:
                                self.maybe_emit_heartbeat(event, stats=current_stats())
                                continue

                            payload_bytes, payload_text = (
                                (msg, msg.decode("utf-8", errors="replace"))
                                if isinstance(msg, bytes)
                                else (msg.encode("utf-8"), msg)
                            )

                            if "|" not in payload_text:
                                preview = payload_text.strip()
                                preview = f"{preview[:200]}..." if len(preview) > 200 else preview
                                self.log.warning(
                                    "%s - no header in payload: %s",
                                    self.__class__.__name__,
                                    preview or "<empty>",
                                )
                                continue

                            _header, body = payload_text.split("|", 1)
                            msg_data = json.loads(body)

                            hits = extract_bovada_odds(event.event_id, msg_data, league)
                            if hits:
                                odds_hits += len(hits)
                                last_event_time = time.time()
                                for hit in hits:
                                    selection = Selection(event.event_id, hit)
                                    change = SelectionChange(label, selection)
                                    change_payload = json.loads(change.to_json())
                                    await sink.save(
                                        provider=provider,
                                        league=league,
                                        event=event,
                                        raw_event=payload_text,
                                        selection_update=change_payload,
                                        output_dir=output_dir,
                                    )

                            total_bytes += len(payload_bytes)
                            events += 1
                            stats = current_stats()
                            if events % BOVADA_EVENT_LOG_INTERVAL == 0 and events > 0:
                                self.maybe_emit_heartbeat(event, force=True, stats=stats)

                            self.maybe_emit_heartbeat(event, stats=stats)
                    finally:
                        try:
                            await ws.send(unsubscribe_payload)
                            self.log.info(
                                "%s - sent unsubscribe payload: %s",
                                self.__class__.__name__,
                                unsubscribe_payload,
                            )
                        except Exception as exc:
                            self.log.warning(
                                "%s - failed to send unsubscribe for %s: %s",
                                self.__class__.__name__,
                                event,
                                exc,
                            )

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionResetError,
            ) as exc:
                error_count += 1
                self.log.warning(
                    "%s - websocket disconnected (%s): %s. Reconnecting in 3s...",
                    self.__class__.__name__,
                    event,
                    exc,
                )
                if error_count > MAX_RUNNER_ERRORS:
                    stats = current_stats()
                    self.log.error(
                        "%s - assuming game ended for %s after %d consecutive websocket errors.",
                        self.__class__.__name__,
                        event,
                        error_count,
                    )
                    self.maybe_emit_heartbeat(event, force=True, stats=stats)
                    break
                await asyncio.sleep(3)
                continue
            except Exception as exc:  # pragma: no cover - defensive runtime guard
                error_count += 1
                self.log.error(
                    "%s - unexpected websocket error (%s): %s",
                    self.__class__.__name__,
                    event,
                    exc,
                )
                if error_count > MAX_RUNNER_ERRORS:
                    stats = current_stats()
                    self.log.error(
                        "%s - assuming game ended for %s after %d consecutive runner errors.",
                        self.__class__.__name__,
                        event,
                        error_count,
                    )
                    self.maybe_emit_heartbeat(event, force=True, stats=stats)
                    break
                await asyncio.sleep(5)

    def emit_heartbeat(self, event: Event, *, stats: dict[str, Any]) -> None:
        """Emit heartbeat JSON describing websocket throughput."""

        start = stats.get("start", time.time())
        events = int(stats.get("events", 0))
        hits = int(stats.get("hits", 0))
        bytes_received = int(stats.get("bytes_received", 0))
        event_start: dt.datetime = stats.get("event_start", event.start_time)

        now = time.time()
        now_utc = dt.datetime.now(dt.timezone.utc)
        worker_elapsed = max(now - start, 0.0)
        event_elapsed = max((now_utc - event_start).total_seconds(), 0.0)

        payload = {
            "event": str(event),
            "worker_time": format_duration(worker_elapsed),
            "event_runtime": format_duration(event_elapsed),
            "msgs_rcvd": f"{events:,d}",
            "msgs_rcvd_per_sec": format_rate_per_sec(events, worker_elapsed),
            "total_bytes_rcvd": format_bytes(bytes_received),
            "odds_rcvd": f"{hits:,d}",
            "odds_rcvd_per_sec": format_rate_per_sec(hits, worker_elapsed),
        }

        self.log.info("%s - %s", self.__class__.__name__, json.dumps(payload, separators=(",", ":")))


class BovadaMonitor(BaseMonitor):
    """Coordinate Bovada websocket runners for live events."""

    def __init__(
        self,
        input_dir: Path,
        concurrency: int = 0,
        *,
        leagues: Sequence[str] | None = None,
        sink: SelectionSink,
        provider: str,
        output_dir: Path | None = None,
    ) -> None:
        super().__init__(
            input_dir,
            BovadaRunner(),
            concurrency=concurrency,
            log_color=CYAN,
            leagues=leagues,
            sink=sink,
            provider=provider,
            output_dir=output_dir,
        )
        self.log.info("%s - using input directory at %s", self.__class__.__name__, self.input_dir)


async def run_scrape(
    output_dir: Path,
    *,
    leagues: Sequence[str] | None = None,
    overwrite: bool = False,
    sink: SelectionSink | None = None,
) -> None:
    """Invoke the Bovada scraper with the destination directory."""

    scraper = BovadaEventScraper(output_dir=output_dir, leagues=leagues)
    await scraper.scrape_and_save_all(
        output_dir,
        leagues=leagues,
        overwrite=overwrite,
        sink=sink,
        provider="bovada",
    )


async def run_monitor(
    input_dir: Path,
    *,
    leagues: Sequence[str] | None = None,
    interval: int = BOVADA_HEARTBEAT_SECONDS,
    sink: SelectionSink,
    provider: str,
    output_dir: Path | None = None,
) -> None:
    """Run the Bovada monitor in a persistent sweep loop."""

    monitor = BovadaMonitor(
        input_dir,
        leagues=leagues,
        sink=sink,
        provider=provider,
        output_dir=output_dir,
    )
    logger.info("starting Bovada monitor loop (interval=%ss)...", interval)

    while True:
        try:
            await monitor.run_once()
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.error("Bovada monitor sweep failed: %s", exc)
        await asyncio.sleep(interval)
