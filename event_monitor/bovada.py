from __future__ import annotations

import logging
import datetime as dt
from pathlib import Path
from typing import Sequence

import httpx

from event_monitor.constants import (
    CYAN,
    SUPPORTED_LEAGUES,
    MGM_NBA_TEAMS,
    MGM_MLB_TEAMS,
    MGM_NFL_TEAMS,
    League,
    SPORTS,
    BOVADA_COMPEITION_IDS,
)
from event_monitor.runner import BaseMonitor
from event_monitor.scraper import BaseEventScraper, ScrapeContext
from event_monitor.t import Event
from event_monitor.utils import configure_colored_logger, load_events, event_filepath

__all__ = ["BovadaEventScraper", "BovadaMonitor", "run_scrape", "run_monitor"]


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s]: %(message)s",
)
logger = configure_colored_logger(__name__, CYAN)

event_headers = {
    # ":authority": "www.bovada.lv",
    # ":method": "GET",
    # ":path": "/services/sports/event/coupon/events/A/description/basketball?marketFilterId=def&liveOnly=true&eventsLimit=50&lang=en",
    # ":scheme": "https",
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


# def build_bovada_url(event: Event) -> str:
#     def slugify(team: tuple[str, str]) -> str:
#         tokens = [part for part in team if part]
#         if not tokens:
#             return ""
#         return "-".join(tokens).lower().replace(" ", "-")
#
#     def resolve_team_slug(team: tuple[str, str]) -> str:
#         tokens = [part for part in team if part]
#         slug = slugify(team)
#         for pool in (MGM_NBA_TEAMS, MGM_NFL_TEAMS, MGM_MLB_TEAMS):
#             for candidate in pool:
#                 if all(token in candidate for token in tokens):
#                     return candidate
#         return slug
#
#     def format_date(start_time: dt.datetime) -> str:
#         if start_time.tzinfo is None:
#             start_time = start_time.replace(tzinfo=dt.timezone.utc)
#         est = pytz.timezone("US/Eastern")
#         localized = start_time.astimezone(est)
#         return localized.strftime("%Y%m%d%H")
#
#     sport = SPORTS[event.league]
#     away = resolve_team_slug(event.away)
#     home = resolve_team_slug(event.home)
#     date_str = format_date(event.start_time)
#     return (
#         f"https://www.bovada.lv/sports/{sport}/{event.league}/{away}-{home}-{date_str}?lang=en"
#     )


def derive_bovada_timestamp(ts: int) -> dt.datetime:
    dt_utc = dt.datetime.fromtimestamp(ts / 1000, tz=dt.timezone.utc)
    # dt_est = dt_utc.astimezone(pytz.timezone("US/Eastern"))
    return dt_utc.astimezone(dt.timezone.utc)


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

    # def load_draftkings_events(self, context: ScrapeContext) -> list[Event]:
    #     filepath = event_filepath(
    #         context.input_dir,
    #         context.league,
    #         timestamp=context.stamp,
    #     )
    #     league_name, events = load_events(filepath)
    #     if league_name != context.league:
    #         self.log.warning(
    #             "%s - snapshot league %s mismatched requested %s",
    #             self.__class__.__name__,
    #             league_name,
    #             context.league,
    #         )
    #     return events

    # async def load_source_events(self, context: ScrapeContext) -> Sequence[Event] | None:
    #     filepath = event_filepath(
    #         context.input_dir,
    #         context.league,
    #         timestamp=context.stamp,
    #     )
    #     if not filepath.exists():
    #         self.log.warning("%s - missing DraftKings snapshot %s", self.__class__.__name__, filepath)
    #         return []
    #     try:
    #         return self.load_draftkings_events(context)
    #     except Exception as exc:
    #         self.log.error(
    #             "%s - failed to load DraftKings events from %s: %s",
    #             self.__class__.__name__,
    #             filepath,
    #             exc,
    #         )
    #         return []

    async def scrape_today(
        self,
        context: ScrapeContext,
        source_events: Sequence[Event] | None = None,
    ) -> list[Event]:
        """Fetch today's Bovada full-game events for ``league``."""

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
        competition_id = BOVADA_COMPEITION_IDS[context.league]

        for group in data:
            for ev in group.get("events", []):
                self.log.info("%s - expected competition id: %s, actual competition id: %s", self.__class__.__name__, competition_id, ev.get("competitionId"))
                if ev.get("competitionId") != competition_id:
                    self.log.warning(
                        "%s - skipping invalid competition event: %s", self.__class__.__name__, ev.get("description")
                    )
                    continue

                try:
                    start_time = derive_bovada_timestamp(ev["startTime"])

                    # Skip events not starting today
                    if not (start_of_day <= start_time.astimezone(self.local_tz) < end_of_day):
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

                    description_filter = "Live Game" if context.league == League.NBA else "Game"

                    selections = []
                    for dg in ev.get("displayGroups", []):
                        if dg.get("alternateType"):
                            continue  # Skip alternate lines

                        for market in dg.get("markets", []):
                            period = market.get("period", {})
                            if (
                                period.get("description") == description_filter
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
                        continue

                    # Build event URL
                    event_link = ev.get("link", "")
                    url = f"https://www.bovada.lv{event_link}"

                    # Detect league dynamically from group.path (e.g. NBA, NFL)
                    league_name = context.league
                    for path_entry in group.get("path", []):
                        if path_entry.get("type") == "LEAGUE":
                            league_name = path_entry.get("description", context.league)

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


class BovadaMonitor(BaseMonitor):
    """Placeholder Bovada monitor that will stream odds once built."""

    def __init__(
        self,
        input_dir: Path,
        concurrency: int = 10,
        *,
        leagues: Sequence[str] | None = None,
    ) -> None:
        raise NotImplementedError("TODO: Implement Bovada live polling monitor")

    async def run_once(self) -> None:
        raise NotImplementedError("TODO: Implement Bovada live polling monitor")


async def run_scrape(
    output_dir: Path,
    *,
    leagues: Sequence[str] | None = None,
    overwrite: bool = False,
) -> None:
    """Invoke the Bovada scraper with the destination directory."""

    dk_root = output_dir.parent / "draftkings"
    scraper = BovadaEventScraper(output_dir=output_dir, input_dir=dk_root, leagues=leagues)
    await scraper.scrape_and_save_all(output_dir, leagues=leagues, overwrite=overwrite)


async def run_monitor(
    input_dir: Path,
    *,
    leagues: Sequence[str] | None = None,
) -> None:
    """Execute the Bovada monitor against ``input_dir`` when available."""

    monitor = BovadaMonitor(input_dir)
    await monitor.run_once()
