#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import datetime as dt
import json
import logging
import pathlib
import re
import time
import types
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse, unquote

import httpx
import msgpack
import websockets
from playwright.async_api import async_playwright


from snuper.sinks import SelectionSink
from snuper.constants import (
    CYAN,
    MAX_IDLE_SECONDS,
    MAX_RUNNER_ERRORS,
    DRAFTKINGS_DEFAULT_MONITOR_INTERVAL,
    DRAFTKINGS_EVENT_LOG_INTERVAL,
    DRAFTKINGS_HEARTBEAT_SECONDS,
    DRAFTKINGS_LEAGUE_URLS,
    DRAFTKINGS_MAX_TIME_SINCE_LAST_EVENT,
    DRAFTKINGS_SPREAD_MARKET_TYPE,
    DRAFTKINGS_WEBSOCKET_URL,
    SUPPORTED_LEAGUES,
    Provider,
    League,
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s]: %(message)s",
)
logger = configure_colored_logger(__name__, CYAN)

DRAFTKINGS_WS_JWT = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImN0eSI6IkpXVCJ9.eyJJc0Fub255bW91cyI6IlRydWUiLCJTaXRlR3JvdXBJZCI6IjM5IiwiU2l0ZUlkIjoiMTUwNjUiLCJEb21haW5JZCI6IjQ0OSIsIkNvdW50cnlDb2RlIjoiVVMiLCJuYmYiOjE3NjEwMDc4NDEsImV4cCI6MTc2MTAwNzk2MSwiaWF0IjoxNzYxMDA3ODQxLCJpc3MiOiJ1cm46ZGsiLCJhdWQiOiJ1cm46ZGsifQ.iD1DWwc0ToslumMKFQn8Wuh8onjoMa_ZlcNgyjNiJz5NgG-nm8_eWfB8oQ3L_FdW-OUcFwCIKgb5ouN7tnmI2RDqGThmRcF4YF7sOHm3Jzbp3aD5oy4_NMyiNMVozhfpn5QHE0KRKcik1OWbYUG9kKT71RFk22H5UMt-cnkoy_DG6C4gvXfUKvSyFqZVwysHrHR6KyXm8k4FggocMrRIXb7XzN-D9pxxKpeX57MBjEPE4Ru1Dr4Ab_LLMafsVX38CwohKuKSaoMMk2783U47gUywDt2bW4W__C0cZQFM1OKlrvIUTYez3TOd-PdTutBSKXpb-qQid0m8s_MX4nR2uG6HfwERL5_QjWtIUhPogOADlDvfPIRuJf1EZCHaRydUqWOBxvkh4JD73N1hRjtkP9HCU7b_pNqNci4p47p-1wmuD8zqGTh29DIUdt4SC3Bfxu_cmzMXnAokgFm_RDvMzLap93LC-BpQQtnmVmkbGh_yJUEBfPEbydYI7YgP1FtgpnmSTCDKZT6UieP1pEyaqvBwat5ZnLRELJUDw0hQjmmoEKK1HIY0AJv4vU47N0kQLpb5pGa5jsWWF6rpYU8an-30HNGuOx-JaYv6erU575ZakrIofbWXqOlugi8ToKtbgof9wiMjdlwX3poJEpAGMQWZqoP3UiWDLaxIvcM6yP4"


def sanitize_american_odds(value: Any) -> str | None:
    """Return ASCII-only American odds without leading plus signs."""

    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    replacements = {
        "−": "-",
        "–": "-",
        "—": "-",
        "﹣": "-",
        "－": "-",
        "＋": "+",
        "⁺": "+",
        "﹢": "+",
    }
    for src, dest in replacements.items():
        text = text.replace(src, dest)

    normalized = text.replace(" ", "")
    lowered = normalized.lower()
    if lowered == "even":
        return "100"
    if lowered in {"pk", "pick", "pick'em", "pickem"}:
        return "0"

    sign = ""
    if normalized.startswith("-"):
        sign = "-"
        normalized = normalized[1:]
    elif normalized.startswith("+"):
        normalized = normalized[1:]

    digits = "".join(ch for ch in normalized if ch.isdigit())
    if not digits:
        return None

    return f"{sign}{digits}" if sign else digits


def flatten_markets(data: dict) -> list:
    """Flatten DraftKings market payloads into per-selection dicts."""

    markets = data.get("markets", [])
    selections = data.get("selections", [])
    if not markets or not selections:
        logger.warning("No markets or selections found")
        return []

    market_map = {m["id"]: m for m in markets if "id" in m}
    output = []

    for s in selections:
        mid = s.get("marketId")
        market = market_map.get(mid, {})
        market_type = (market.get("marketType") or {}).get("name")
        display_odds = s.get("displayOdds", {})
        participant_name = None
        if s.get("participants"):
            participant_name = s["participants"][0].get("name")

        output.append(
            {
                "selection_id": s.get("id"),
                "market_id": mid,
                "event_id": market.get("eventId"),
                "market_name": market.get("name"),
                "marketType": market_type,
                "label": s.get("label"),
                "odds_american": sanitize_american_odds(display_odds.get("american")),
                "odds_decimal": display_odds.get("decimal"),
                "participant": participant_name,
            }
        )

    return output


def parse_event_categories(event_id) -> list:
    """Fetch and flatten markets for the DraftKings event id."""
    cookie = """site=US-DK; ASP.NET_SessionId=zcp2ugw2fdo4mbksvkraxn41; VIDN=87710433171; SN=1223548523; LID=1; SINFN=PID=&AOID=&PUID=0&SSEG=&GLI=0&LID=1&site=US-DK; _csrf=4e41fa98-1694-4b4a-bd73-a5dea01b92f6; notice_behavior=implied,us; gpcishonored=true; notice_preferences=0:; notice_gdpr_prefs=0::implied,us; cmapi_gtm_bl=ga-ms-ua-ta-asp-bzi-sp-awct-cts-csm-img-flc-fls-mpm-mpr-m6d-tc-tdc; cmapi_cookie_privacy=permit 1 required; userLocale=en; SIDN=121791485289; SSIDN=125937432060; STIDN=eyJDIjoxMjIzNTQ4NTIzLCJTIjoxMjE3OTE0ODUyODksIlNTIjoxMjU5Mzc0MzIwNjAsIlYiOjg3NzEwNDMzMTcxLCJMIjoxLCJFIjoiMjAyNS0xMC0yMVQxOToyOToyOS42NTQ1Nzk1WiIsIlNFIjoiVVMtREsiLCJVQSI6IkU4eEZEWWMwL01uRFkzd2pzZlRLY3dsNXdOdk9HXHUwMDJCWGZOTm1sUHQ4ZGJtVT0iLCJESyI6ImIwMDM0MjBkLTRjY2EtNDZhZC05MjBiLWMzNmQ1N2ZlNmQ3YyIsIkRJIjoiZWQxNmY2YzUtY2ZjNS00ZWRiLWJmMDQtYTg4NGNlYTlhOTkyIiwiREQiOjg4MDQ0MzMxNjMxfQ==; STH=e0d5879e16be9d3c3f7d90e957f081bbc960512ccc19a6171de5022e8b2a0df3; TAsessionID=16c96a22-51e9-4004-a0af-27199441938b|EXISTING; _abck=95A531F756A6A1542E9CD86A7A3D78F0~0~YAAQzvEPF+4EUdyZAQAAYJAkCA7/wDsW9fgkaw6MgoK9u6nGXHjOB6slxYwOVo6tEwBfSx0d4DUpvOiw4jEVDr7lnT/2t6jMAZktX14yeBbl7bqihCsl0G/bA89rglKygKhq2RIySVV7QrvTR4xmef4DJxOuJ+HoDDD6/CEs+duhDb5WYvqm45gDgLJNYE0jwMb0c/moZXMbDuhLZsCno5KVJLs2TTa1IizRWMXS0nPm5Q1At5eF8whpye2Y1wfrCb9Va/g2AGo9LCzcTbF6t+MXGEy/ZR0q7ZYwL6ixNPCs95X0vMHKAU9t2UJzBv3450x8F4oQPk+i2Dj484ppP5Q9r1HnVoLHeRLcYQTmeyIy0TMKEmUTV6anO+CBtAnEuvJBvqVKk17w+nuPd6jzSrm9RQS34Kegwz1Jt5D5vWL3KDIi0nXg0a31GtGs5eudVd+VZWWk7hLkUzgg+0f9noEq3Gl22PFwlFNAgSb1Ypj/bvMCNZl1GMLDTxIs4fcf8l+vQOi5gsk33kjHZjVIF21599ln7D8ut7VrCH/5w45bVI6CukTmVPLeUdb9O8132RchOiRYgVOqqpBq+pEl8jFwBfYJOdlprRowkQcny/jIGritYWlraA4tHdL2fkWCe/ffW4/rua64lBrj6a3WXBKhmIVk267FTi6XSUqQN9E=~-1~-1~-1~AAQAAAAE%2f%2f%2f%2f%2f9RLW8TzocBIYdfW4NRNy2WdHEqgk04K29KSdip+dRwwyR%2fjDaK5dTWFrGjgOs+jALXCqgd1HjKXOCYbIZWDCPsnc7g70WW+kU+zBufG2ENt2RpYN7mHFNptP%2fv4HfSKZVgA1RQ%3d~-1; ak_bmsc=AA687E63EC9CE0DFE7FDB54E8BE93C39~000000000000000000000000000000~YAAQzvEPFwsHUdyZAQAA1EElCB3EgWTE8cawJ7TOqBvJlW8DlTXWQXOfJ6PSeLaUq8wwWn7JWgtZslBo7qls7O7Zh3ybp6WwH7SR3cBQuowSyH8SrRUngo3q99VyvsWoGc8wBRN1SxU78GCvk/mqQvvf5nSvhx0/gpMy4Rh2qKjQhYdvt0DMPPrZ3ONVOaF1JVRDr70D+6S3CXW4chCEWxhMoZ9wjfkanbhOz1cJgEw7zT2tTmOS8o2h5mgkjRTKztGPAS4FZ8wTt+93hfVa0FehJqhy6I8mLfp7rpAuOkhZdeMBvdx8nZ2Y8UzJ/mJyDABOF48Zmr8Sx+5qo9heP55GWGqFUyTntNPY+MDdAXpUd0vjfvwwOrhvMVTojElS0iF9+C3Ka7gwSJ+b7909kesq6jfDvI328rsUCdaJ2w0E/42IXQj39hH1BppAjQkybRQtqFBVneb6nj0=; hgg=eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ2aWQiOiI4NzcxMDQzMzE3MSIsImRrZS0xMjYiOiIzNzQiLCJka2UtMjA0IjoiNzEwIiwiZGtlLTI4OCI6IjExMjgiLCJka2UtMzE4IjoiMTI2MCIsImRrZS0zNDUiOiIxMzUzIiwiZGtlLTM0NiI6IjEzNTYiLCJka2UtNDI5IjoiMTcwNSIsImRrZS03MDAiOiIyOTkyIiwiZGtlLTczOSI6IjMxNDAiLCJka2UtNzU3IjoiMzIxMiIsImRrZS04MDYiOiIzNDI2IiwiZGtlLTgwNyI6IjM0MzciLCJka2UtODI0IjoiMzUxMSIsImRrZS04MjUiOiIzNTE0IiwiZGtlLTgzNiI6IjM1NzAiLCJka2gtODk1IjoiOGVTdlpEbzAiLCJka2UtODk1IjoiMCIsImRrZS05MDMiOiIzODQ4IiwiZGtlLTkxNyI6IjM5MTMiLCJka2UtOTQ3IjoiNDA0MiIsImRrZS05NzYiOiI0MTcxIiwiZGtoLTE2NDEiOiJSMGtfbG1rRyIsImRrZS0xNjQxIjoiMCIsImRrZS0xNjUzIjoiNzEzMSIsImRrZS0xNjg2IjoiNzI3MSIsImRrZS0xNjg5IjoiNzI4NyIsImRrZS0xNzU0IjoiNzYwNSIsImRrZS0xNzYwIjoiNzY0OSIsImRrZS0xNzc0IjoiNzcwOSIsImRrZS0xNzk0IjoiNzgwMSIsImRraC0xODA1IjoiT0drYmxrSHgiLCJka2UtMTgwNSI6IjAiLCJka2UtMTgyOCI6Ijc5NTYiLCJka2UtMTg2MSI6IjgxNTciLCJka2UtMTg2OCI6IjgxODgiLCJka2UtMTg5OCI6IjgzMTUiLCJka2gtMTk1MiI6ImFVZ2VEWGJRIiwiZGtlLTE5NTIiOiIwIiwiZGtlLTIwOTciOiI5MjA1IiwiZGtlLTIxMDAiOiI5MjIzIiwiZGtlLTIxMzUiOiI5MzkzIiwiZGtoLTIxNTAiOiJOa2JhU0Y4ZiIsImRrZS0yMTUwIjoiMCIsImRrZS0yMTk1IjoiOTY2NSIsImRrZS0yMjI0IjoiOTc4MyIsImRrZS0yMjI2IjoiOTc5MCIsImRrZS0yMjM3IjoiOTgzNSIsImRrZS0yMjM4IjoiOTgzNyIsImRrZS0yMjQwIjoiOTg1NyIsImRrZS0yMjQ2IjoiOTg4NyIsImRrZS0yMzI0IjoiMTAzMjMiLCJka2UtMjMyOCI6IjEwMzM4IiwiZGtlLTIzMzMiOiIxMDM3MSIsImRrZS0yMzM5IjoiMTAzOTYiLCJka2UtMjM0MiI6IjEwNDExIiwiZGtlLTIzNTIiOiIxMDQ4MyIsImRrZS0yMzU5IjoiMTA1MTgiLCJka2UtMjM2NSI6IjEwNTQ1IiwiZGtlLTIzNjYiOiIxMDU0OSIsImRrZS0yMzg4IjoiMTA2NTEiLCJka2UtMjQwMyI6IjEwNzY0IiwiZGtlLTI0MTgiOiIxMDg1MyIsImRrZS0yNDIwIjoiMTA4NjEiLCJka2UtMjQyMSI6IjEwODY0IiwiZGtlLTI0MjMiOiIxMDg3MSIsImRrZS0yNDI3IjoiMTA4OTUiLCJka2UtMjQyOSI6IjEwOTAzIiwiZGtlLTI0MzEiOiIxMDkwOSIsImRrZS0yNDM0IjoiMTA5MTgiLCJka2UtMjQzNSI6IjEwOTIyIiwiZGtlLTI0MzYiOiIxMDkzMiIsImRraC0yNDM3IjoiQ1IyTTdOR28iLCJka2UtMjQzNyI6IjAiLCJka2UtMjQ0MCI6IjEwOTUwIiwiZGtoLTI0NDciOiI5X0V0RkZRVCIsImRrZS0yNDQ3IjoiMCIsImRrZS0yNDUxIjoiMTA5OTMiLCJka2UtMjQ1OCI6IjExMDIwIiwiZGtlLTI0NTkiOiIxMTAyMiIsImRrZS0yNDYwIjoiMTEwMjUiLCJka2UtMjQ2MSI6IjExMDI4IiwiZGtlLTI0NjYiOiIxMTAzNCIsImRrZS0yNDY5IjoiMTEwNDAiLCJka2UtMjQ3NCI6IjExMDU5IiwiZGtlLTI0NzUiOiIxMTA2MiIsImRrZS0yNDc2IjoiMTEwNjUiLCJka2UtMjQ3NyI6IjExMDcxIiwiZGtlLTI0NzgiOiIxMTA3NSIsImRrZS0yNDg2IjoiMTExMDkiLCJka2UtMjQ4NyI6IjExMTExIiwiZGtlLTI0OTEiOiIxMTEyNiIsImRrZS0yNDkyIjoiMTExMzAiLCJka2UtMjQ5MyI6IjExMTM0IiwiZGtlLTI0OTUiOiIxMTE0MSIsImRrZS0yNDk2IjoiMTExNDciLCJka2gtMjQ5OCI6IjhEY1J4dzVZIiwiZGtlLTI0OTgiOiIwIiwiZGtoLTI0OTkiOiJqYjBpdGttZCIsImRrZS0yNDk5IjoiMCIsImRrZS0yNTA5IjoiMTExODgiLCJuYmYiOjE3NjEwNzQwOTYsImV4cCI6MTc2MTA3NDM5NiwiaWF0IjoxNzYxMDc0MDk2LCJpc3MiOiJkayJ9.tdVkE6nFA4I0-l-Oct5WiVktDk7N27t9eDVWv45P78E; bm_sz=76268343F598B72C070FE88EA7AF7026~YAAQr/EPF8HTY92ZAQAAxWk0CB3a/GbQ20NPOhxHqrjQ+c5kGQiwu9dmksbqDdLAvEfQaK/0So3CSrzSTTkRdKslpget9Z+TLC67PADbjiuAhhfOtYiFVQk5zm9YlrGCSJqeVXEh1o5NO7S2Ncs1Sx33LmwxTOjOT37clGtaBGvttHX3tYkiKgN0Et8saDL9IVVQ2S1/5whJRMywOA2wo0ElPrWjg4KKGY60+64F+Z8Ken6+wdZe8ENXBoO8MgHEYdWqdO5GNM+KkW33BaxOlB5aLpOGJGRmltmOH2xtw55985mSf4TEqSfu2nSTU8YNcjwhOvfVMJpmsizOZ2a3UpV3Mgs6ZEEPSDu0njlX/80pXFzwu+H/NJeBg8J5QQzzSgnOiJjPp4K70CYzlG9dx+aaAFQDeuZ3bwCfD7r0XmZeX5DLUxpvn5sLXpK8aHI80ZTeSW7GP0gA5SA5rABVCv+PCWKFnMHNPCRvnT8Uv3BsRvxQz5uqF39n5ymxw88/iM3C8CEoCyZFfg==~3229252~4599858; STE="2025-10-21T19:49:22.6543698Z"; bm_sv=1B302B2673C06CD4415B7EA07E377372~YAAQr/EPF43YY92ZAQAAUDk2CB1OcY8EYjI+Uefrrl1FtBMGe2UqdcS/ibJwYq/GhK5gZe/vAsL4WvmonUZMjfRVYdatTwu0d9tdbVIxyzs870VjW0r3zeR46r58zQhqlOJ+Z4sUfvqG6J430OkWSHEeCSJZNRNyB92YHd7Sok3usy5J3x8PrnNKhqzsRNI49sqRNUkX39st/3F8lFXNZTeIqziPgPNze0XrIGfbD/J6ExksF/pi0cL5lMKcrNCpYtPycl4=~1"""
    headers = {
        "origin": "https://sportsbook.draftkings.com",
        "priority": "u=1, i",
        "referer": "https://sportsbook.draftkings.com/",
        "sec-ch-ua": '"Chromium";v="140", "Not=A?Brand";v="24", "Brave";v="140"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "sec-gpc": "1",
        "user-agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/140.0.0.0 Safari/537.36"
        ),
        "x-client-feature": "event-page",
        "x-client-name": "web",
        "x-client-page": "Event",
        "x-client-version": "2542.2.1.9",
        "x-client-widget-name": "EventPageWidget",
        "x-client-widget-version": "2.4.0",
        "accept": "*/*",
        "accept-encoding": "gzip, deflate, br, zstd",
        "accept-language": "en-US,en;q=0.7",
        "content-type": "application/json; charset=utf-8",
        "cookie": cookie,
    }
    categories_url = f"https://sportsbook-nash.draftkings.com/api/sportscontent/dkusnj/v1/events/{event_id}/categories"
    resp = httpx.get(categories_url, headers=headers, timeout=20)
    data = resp.json()
    return flatten_markets(data)


def parse_spread_markets(event_id):
    """Filter markets down to spread selections for monitoring."""
    data = parse_event_categories(event_id)
    spread_markets = [x for x in data if x["marketType"] == DRAFTKINGS_SPREAD_MARKET_TYPE]
    return spread_markets


def process_dk_frame(msg_bytes, event, state, selection_ids, market_ids):

    if not hasattr(state, "sel"):
        state.sel, state.hashes = {}, {}

    SCHEMA = {
        24: [
            "selection_id",
            "label",
            "odds_array",
            "odds_decimal",
            "spread_or_line",
            "market_id",
            "market_tags",
            "linked_selection_id",
        ],
        35: [
            "selection_id",
            "market_code",
            "label",
            "odds_array",
            "odds_decimal",
            "spread_or_line",
            "side",
            "participants",
            "market_id",
            "market_tags",
            "linked_selection_id",
            "unknown",
            "meta",
        ],
    }

    watched_sids_exact = {s.lower() for s in selection_ids}
    watched_mids_exact = {str(m).lower() for m in market_ids}

    watched_sid_patterns, watched_mid_patterns = set(), set()
    for sid in selection_ids:
        watched_sid_patterns.add(sid.lower())
        for num in re.findall(r"\d+", sid):
            if len(num) >= 7:
                watched_sid_patterns.add(num.lower())
    for mid in market_ids:
        mid_str = str(mid).lower()
        watched_mid_patterns.add(mid_str)
        for num in re.findall(r"\d+", mid_str):
            if len(num) >= 7:
                watched_mid_patterns.add(num.lower())

    try:
        decoded = msgpack.unpackb(msg_bytes, raw=False, timestamp=3)
    except Exception as e:
        logger.error("Failed to decode msg: %s", e)
        return []

    def iter_nodes(root):
        stack = [root]
        while stack:
            cur = stack.pop()
            if isinstance(cur, list):
                if len(cur) >= 2 and isinstance(cur[0], int) and isinstance(cur[1], list):
                    yield cur
                for item in reversed(cur):
                    if isinstance(item, (list, dict)):
                        stack.append(item)
            elif isinstance(cur, dict):
                stack.extend(v for v in cur.values() if isinstance(v, (list, dict)))

    def matches_watched_selection(sid):
        if not sid:
            return False
        sid_lower = sid.lower()
        if sid_lower in watched_sids_exact:
            return True
        for pattern in watched_sid_patterns:
            if pattern in sid_lower:
                return True
        return False

    def matches_watched_market(mid):
        if not mid:
            return False
        mid_str = str(mid).lower()
        if mid_str in watched_mids_exact:
            return True
        for pattern in watched_mid_patterns:
            if pattern in mid_str:
                return True
        return False

    hits = []
    for node in iter_nodes(decoded):
        tcode, payload = node[0], node[1]
        hdr = SCHEMA.get(tcode)
        if not hdr or len(payload) != len(hdr):
            continue

        item = dict(zip(hdr, payload))
        sid = item.get("selection_id")
        if not sid:
            continue

        mid = item.get("market_id")

        prefix = sid[:3] if len(sid) >= 3 else ""
        bet_type = None
        if prefix == "0HC":
            bet_type = "spread"
        elif prefix == "0OU":
            bet_type = "total"
        elif prefix == "0ML":
            bet_type = "moneyline"

        match_type = None
        if matches_watched_selection(sid):
            match_type = f"exact:selection:{bet_type}" if bet_type else "exact:selection"
        elif matches_watched_market(mid):
            match_type = "exact:market"
        elif bet_type:
            match_type = f"prefix:selection:{bet_type}"

        if not match_type:
            continue

        prev = state.sel.get(sid, {})
        merged = {k: item[k] if item.get(k) is not None else prev.get(k) for k in hdr}
        state.sel[sid] = merged

        oa = merged.get("odds_array") or []
        odds_raw = oa[0] if oa else None
        odds_ascii = sanitize_american_odds(odds_raw)

        fp = (
            odds_raw,
            oa[1] if len(oa) > 1 else None,
            merged.get("odds_decimal"),
            merged.get("spread_or_line"),
        )

        if state.hashes.get(sid) == fp:
            continue

        state.hashes[sid] = fp

        mtags = merged.get("market_tags") or []
        market_type = mtags[0] if mtags else None

        # Manual hacky skip of non-game total lines. Drafkings doesn't have a way of doing this?
        if bet_type == "total" and event.league == League.NBA.value and merged.get("spread_or_line", 0) < 180:
            continue

        hits.append(
            {
                "selection_id": merged.get("selection_id"),
                "market_id": merged.get("market_id"),
                "event_id": event.event_id,
                "market_name": market_type,
                "marketType": market_type,
                "label": merged.get("label"),
                "odds_american": odds_ascii,
                "odds_decimal": (oa[1] if len(oa) > 1 else None) or merged.get("odds_decimal"),
                "spread_or_line": merged.get("spread_or_line"),
                "bet_type": bet_type,
                "participant": merged.get("label"),
                "match": match_type,
            }
        )

    return hits


class DraftkingsEventScraper(BaseEventScraper):
    """Scrape DraftKings league pages for daily event metadata."""

    def __init__(self, output_dir: pathlib.Path | str | None = None) -> None:
        """Initialise helper patterns and timezone context."""
        super().__init__(SUPPORTED_LEAGUES, output_dir=output_dir)
        self.pattern_event_url = re.compile(
            r"^https://sportsbook\.draftkings\.com/event/[a-z0-9\-@%]+/\d+$",
            re.IGNORECASE,
        )
        self.pattern_date = re.compile(r'"startEventDate":"([^"]+)"')

    def extract_team_info(self, event_url: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
        """Derive away/home team tokens from a DraftKings event slug."""
        slug = urlparse(event_url).path.split("/event/")[-1].split("/")[0]
        # Handle double URL encoding (e.g., %2540 -> %40 -> @)
        # Keep decoding until the string stops changing
        prev_slug = None
        while prev_slug != slug:
            prev_slug = slug
            slug = unquote(slug)
        try:
            away_part, home_part = slug.split("@")
        except ValueError:
            return None

        def _parse_team(part: str) -> tuple[str, str]:
            """Split a slug fragment into short and long team names."""
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
        """Collect today's events for the league and attach spread markets."""
        league = context.league
        url = DRAFTKINGS_LEAGUE_URLS[league]
        self.log.info(
            "%s - scraping league '%s', detected local timezone: %s", self.__class__.__name__, league, self.local_tz
        )
        now = dt.datetime.now(self.local_tz)
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + dt.timedelta(days=1)
        self.log.info(
            "%s - using date info: Now(%s), StartOfDay(%s), EndOfDay(%s)",
            self.__class__.__name__,
            now,
            start_of_day,
            end_of_day,
        )
        events: list[Event] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(locale="en-US")
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)

            # Wait for event links to stabilize instead of fixed timeout
            # DraftKings loads events asynchronously, so we need to wait for the count to stabilize
            prev_count = 0
            stable_count = 0
            max_attempts = 10

            for attempt in range(max_attempts):
                await page.wait_for_timeout(1000)  # Wait 1 second between checks
                hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
                event_urls = [h for h in hrefs if self.pattern_event_url.match(h)]
                current_count = len(set(event_urls))

                if current_count == prev_count:
                    stable_count += 1
                    if stable_count >= 3:  # Count stable for 3 consecutive checks (3 seconds)
                        self.log.info(
                            "%s - event count stabilized at %d after %d attempts",
                            self.__class__.__name__,
                            current_count,
                            attempt + 1,
                        )
                        break
                else:
                    stable_count = 0
                    prev_count = current_count
                    self.log.debug(
                        "%s - found %d event URLs (attempt %d/%d), waiting for more...",
                        self.__class__.__name__,
                        current_count,
                        attempt + 1,
                        max_attempts,
                    )

            # Final scrape after stabilization
            hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            await browser.close()

        event_urls = sorted(set(h for h in hrefs if self.pattern_event_url.match(h)))
        self.log.info("%s - found %d event URLs. Fetching metadata...", self.__class__.__name__, len(event_urls))

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        for event_url in event_urls:
            try:
                r = httpx.get(event_url, headers=headers, timeout=10)
                match = self.pattern_date.search(r.text)
                if not match:
                    # Fallback: use Playwright to fetch with JavaScript execution
                    self.log.info(
                        "%s - date not in HTML for %s, fetching with browser (JavaScript)",
                        self.__class__.__name__,
                        event_url,
                    )
                    async with async_playwright() as p:
                        browser = await p.chromium.launch(headless=True)
                        ctx = await browser.new_context(locale="en-US")
                        page = await ctx.new_page()
                        await page.goto(event_url, wait_until="domcontentloaded", timeout=30000)
                        await page.wait_for_timeout(2000)  # Give JavaScript time to render
                        html = await page.content()
                        await browser.close()
                        match = self.pattern_date.search(html)
                        if not match:
                            # Log a sample of the HTML to debug what's actually in the page
                            self.log.warning(
                                "%s - no date found even with browser for %s. HTML sample (first 1000 chars): %s",
                                self.__class__.__name__,
                                event_url,
                                html[:1000],
                            )
                            continue
                utc_str = match.group(1)
                dt_utc = dt.datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
                dt_local = dt_utc.astimezone(self.local_tz)

                if not start_of_day <= dt_local < end_of_day:
                    self.log.debug(
                        "%s - date condition *NOT* met - StartOfDay(%s) <= Local(%s) < EndOfDay(%s)",
                        self.__class__.__name__,
                        start_of_day,
                        dt_local,
                        end_of_day,
                    )
                    continue

                self.log.debug(
                    "%s - date condition met - StartOfDay(%s) <= Local(%s) < EndOfDay(%s)",
                    self.__class__.__name__,
                    start_of_day,
                    dt_local,
                    end_of_day,
                )
                if start_of_day <= dt_local < end_of_day:
                    event_id = event_url.split("/")[-1]
                    away, home = self.extract_team_info(event_url) or (
                        ("?", "?"),
                        ("?", "?"),
                    )
                    selections = parse_spread_markets(event_id)
                    events.append(
                        Event(
                            event_id,
                            league,
                            event_url,
                            dt_utc,
                            away,
                            home,
                            selections,
                        )
                    )
            except Exception as e:
                self.log.warning("%s - error fetching %s: %s", self.__class__.__name__, event_url, e)
            await asyncio.sleep(0.5)

        events.sort(key=lambda g: g.start_time)
        self.log.info("%s - scraped %d total %s events for today.", self.__class__.__name__, len(events), league)
        return events


class DraftKingsRunner(BaseRunner):
    """Stream DraftKings websocket messages and persist odds."""

    def __init__(self) -> None:
        """Prepare the websocket endpoint, JWT, and logger metadata."""

        super().__init__(
            heartbeat_interval=DRAFTKINGS_HEARTBEAT_SECONDS,
            log_color=CYAN,
        )
        self.jwt = DRAFTKINGS_WS_JWT
        self.url = DRAFTKINGS_WEBSOCKET_URL

    async def run(
        self,
        event: Event,
        output_dir: pathlib.Path,
        league: str,
        provider: str,
        sink: SelectionSink,
    ) -> None:
        """Consume the websocket for ``event`` and persist odds updates via ``sink``."""

        state = types.SimpleNamespace()
        self.log.info("%s - using selections %s for %s", self.__class__.__name__, event.selections, event)
        selection_ids = [x["selection_id"] for x in event.selections]
        market_ids = [x["market_id"] for x in event.selections]

        self.log.info(
            "%s - starting websocket runner for game %s in league %s",
            self.__class__.__name__,
            event,
            league,
        )
        subscribe_payload = {
            "jsonrpc": "2.0",
            "params": {
                "entity": "events",
                "queryParams": {
                    "query": f"$filter=id eq '{event.event_id}'",
                    "initialData": False,
                    "includeMarkets": "$filter=tags/all(t: t ne 'SportcastBetBuilder')",
                    "projection": "sportsbook",
                    "locale": "en",
                },
                "forwardedHeaders": {},
                "clientMetadata": {
                    "feature": "event",
                    "X-Client-Name": "web",
                    "X-Client-Version": "2542.2.1.9",
                },
                "jwt": self.jwt,
                "siteName": "dkusnj",
            },
            "method": "subscribe",
            "id": "dk-ws-client",
        }
        destination = sink.describe_destination(
            provider=provider,
            league=league,
            event=event,
            output_dir=output_dir,
        )
        if destination:
            self.log.info(
                "%s - logging odds data for %s to %s",
                self.__class__.__name__,
                event,
                destination,
            )
        else:
            self.log.info("%s - logging odds data for %s", self.__class__.__name__, event)
        start = time.time()
        events = 0
        nhits = 0
        total_bytes = 0
        last_event_time = time.time()
        self.reset_heartbeat()
        error_count = 0

        def current_stats() -> dict[str, Any]:
            return {
                "start": start,
                "events": events,
                "event_start": event.start_time,
                "hits": nhits,
                "bytes_received": total_bytes,
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
                async with websockets.connect(self.url, max_size=None) as ws:
                    error_count = 0
                    await ws.send(msgpack.packb(subscribe_payload))
                    self.log.info("%s - subscribed to %s", self.__class__.__name__, event)

                    while True:
                        if time.time() - last_event_time > DRAFTKINGS_MAX_TIME_SINCE_LAST_EVENT:
                            self.log.info(
                                "%s - no updates for %d seconds; assuming %s is experiencing a pause in play (e.g., halftime, timeout, etc).",
                                self.__class__.__name__,
                                MAX_IDLE_SECONDS,
                                event,
                            )
                            return

                        task = asyncio.current_task()
                        if task and task.cancelled():
                            self.log.warning(
                                "%s - websocket runner for %s cancelled mid-loop.",
                                self.__class__.__name__,
                                event,
                            )
                            return

                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=10)
                        except asyncio.TimeoutError:
                            self.maybe_emit_heartbeat(event, stats=current_stats())
                            continue

                        try:
                            if isinstance(msg, str):
                                msg_bytes = msg.encode("utf-8")
                            else:
                                msg_bytes = msg

                            total_bytes += len(msg_bytes)

                            hits = process_dk_frame(
                                msg_bytes,
                                event,
                                state,
                                selection_ids,
                                market_ids,
                            )
                            if hits:
                                nhits += len(hits)
                                last_event_time = time.time()

                            events += 1
                            stats = current_stats()
                            if events % DRAFTKINGS_EVENT_LOG_INTERVAL == 0 and events > 0:
                                self.maybe_emit_heartbeat(event, force=True, stats=stats)

                            for hit in hits:
                                sel = Selection(event.event_id, hit)
                                change = SelectionChange(event.game_label(), sel)
                                change_payload = json.loads(change.to_json())
                                await sink.save(
                                    provider=provider,
                                    league=league,
                                    event=event,
                                    raw_event=msg_bytes,
                                    selection_update=change_payload,
                                    output_dir=output_dir,
                                )

                            self.maybe_emit_heartbeat(event, stats=stats)
                        except Exception as exc:
                            self.log.warning(
                                "%s - decode error (%s): %s",
                                self.__class__.__name__,
                                event,
                                exc,
                            )

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionResetError,
            ) as e:
                error_count += 1
                self.log.warning(
                    "%s - websocket disconnected (%s): %s. Reconnecting in 3s...", self.__class__.__name__, event, e
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
            except Exception as e:
                error_count += 1
                self.log.error("%s - unexpected websocket error (%s): %s", self.__class__.__name__, event, e)
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
        """Emit a one-line JSON payload describing websocket throughput."""

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


class DraftKingsMonitor(BaseMonitor):
    """Coordinate DraftKings websocket runners for live events."""

    def __init__(
        self,
        input_dir: pathlib.Path,
        concurrency: int = 0,
        *,
        leagues: Sequence[str] | None = None,
        sink: SelectionSink,
        provider: str,
        output_dir: pathlib.Path | None = None,
        monitor_interval: int | None = None,
        early_exit: bool = False,
    ) -> None:
        """Initialise the monitor with the target snapshot directory."""
        super().__init__(
            input_dir,
            DraftKingsRunner(),
            concurrency=concurrency,
            log_color=CYAN,
            leagues=leagues,
            provider=provider,
            sink=sink,
            output_dir=output_dir,
            monitor_interval=monitor_interval,
            early_exit=early_exit,
        )
        self.log.info("%s - using input directory at %s", self.__class__.__name__, self.input_dir)


async def run_scrape(
    output_dir: pathlib.Path,
    *,
    leagues: Sequence[str] | None = None,
    overwrite: bool = False,
    sink: SelectionSink | None = None,
    merge_sportdata_games: bool = False,
    merge_rollinginsights_games: bool = False,
) -> None:
    """Invoke the DraftKings scraper with the provided destination."""

    scraper = DraftkingsEventScraper(output_dir)
    await scraper.scrape_and_save_all(
        output_dir,
        leagues=leagues,
        overwrite=overwrite,
        sink=sink,
        provider=Provider.DraftKings.value,
        merge_sportdata_games=merge_sportdata_games,
        merge_rollinginsights_games=merge_rollinginsights_games,
    )


async def run_monitor(
    input_dir: pathlib.Path,
    *,
    interval: int = DRAFTKINGS_DEFAULT_MONITOR_INTERVAL,
    leagues: Sequence[str] | None = None,
    sink: SelectionSink,
    provider: str,
    output_dir: pathlib.Path | None = None,
    early_exit: bool = False,
) -> None:
    """Execute the DraftKings monitor loop with the configured interval."""

    monitor = DraftKingsMonitor(
        input_dir,
        leagues=leagues,
        sink=sink,
        provider=provider,
        output_dir=output_dir,
        monitor_interval=interval,
        early_exit=early_exit,
    )
    logger.info("Starting persistent monitor (interval=%ss)...", interval)
    while True:
        try:
            await monitor.run_once()
            if monitor.should_terminate_eod():
                break
        except Exception as exc:
            logger.error("Cycle error: %s", exc)
        logger.info("Sleeping %ss before next cycle...", interval)
        await asyncio.sleep(interval)
