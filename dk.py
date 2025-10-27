#!/usr/bin/env python3
from __future__ import annotations
import asyncio
import argparse
import datetime as dt
import json
import logging
import os
import time
import re
import types
import pathlib
from typing import Any, Optional
from urllib.parse import urlparse, unquote

import httpx
import msgpack
import websockets
from dotenv import load_dotenv
from playwright.async_api import async_playwright
from tzlocal import get_localzone

from event_monitor.t import Event
from event_monitor.utils import load_events

CYAN = "\033[96m"
RED = "\033[91m"
RESET = "\033[0m"

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(lineno)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

url = "wss://sportsbook-ws-us-nj.draftkings.com/websocket?format=msgpack&locale=en"
spread_market_type = "Spread"
# spread_market_id = "2_80876023"
default_loop_interval = 30
max_time_since_last_event = 60 * 10  # 10 mins
game_runtime = 5 * 3600  # 5 hours
event_log_interval = 500


DRAFTKINGS_LEAGUE_URLS = {
    "nfl": "https://sportsbook.draftkings.com/leagues/football/nfl",
    "mlb": "https://sportsbook.draftkings.com/leagues/baseball/mlb",
    "nba": "https://sportsbook.draftkings.com/leagues/basketball/nba",
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


def flatten_markets(data: dict) -> list:
    markets = data.get("markets", [])
    selections = data.get("selections", [])
    if not markets or not selections:
        print("No markets or selections found")
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
                "odds_american": display_odds.get("american"),
                "odds_decimal": display_odds.get("decimal"),
                "participant": participant_name,
            }
        )

    return output


def parse_event_categories(event_id) -> list:
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
    data = parse_event_categories(event_id)
    spread_markets = [x for x in data if x["marketType"] == spread_market_type]
    return spread_markets


def process_dk_frame(msg_bytes, event_id, state, selection_ids, market_ids):

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

    all_patterns = watched_sid_patterns | watched_mid_patterns
    if all_patterns:
        sample = msg_bytes[:2048] + (msg_bytes[-2048:] if len(msg_bytes) > 2048 else b"")
        sample_str = sample.decode("utf-8", errors="ignore").lower()
        if not any(pattern in sample_str for pattern in all_patterns):
            return []

    try:
        decoded = msgpack.unpackb(msg_bytes, raw=False, timestamp=3)
    except Exception:
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

        # Infer bet type from selection_id prefix
        prefix = sid[:3] if len(sid) >= 3 else ""
        bet_type = None
        if prefix == "0HC":
            bet_type = "spread"
        elif prefix == "0OU":
            bet_type = "total"
        elif prefix == "0ML":
            bet_type = "moneyline"

        # Determine match type (priority: exact selection > exact market > prefix)
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
        merged = {k: (item[k] if item.get(k) is not None else prev.get(k)) for k in hdr}
        state.sel[sid] = merged

        oa = merged.get("odds_array") or []
        fp = (
            oa[0] if oa else None,
            oa[1] if len(oa) > 1 else None,
            merged.get("odds_decimal"),
            merged.get("spread_or_line"),
        )

        if state.hashes.get(sid) == fp:
            continue
        state.hashes[sid] = fp

        mtags = merged.get("market_tags") or []
        market_type = mtags[0] if mtags else None

        hits.append(
            {
                "selection_id": merged.get("selection_id"),
                "market_id": merged.get("market_id"),
                "event_id": event_id,
                "market_name": market_type,
                "marketType": market_type,
                "label": merged.get("label"),
                "odds_american": oa[0] if oa else None,
                "odds_decimal": (oa[1] if len(oa) > 1 else None) or merged.get("odds_decimal"),
                "spread_or_line": merged.get("spread_or_line"),
                "bet_type": bet_type,
                "participant": merged.get("label"),
                "match": match_type,
            }
        )

    return hits


class EventScraper:
    def __init__(self) -> None:
        self.leagues = ["nba", "nfl", "mlb"]
        self.log = logging.getLogger(self.__class__.__name__)
        self.local_tz = get_localzone()
        self.pattern_event_url = re.compile(
            r"^https://sportsbook\.draftkings\.com/event/[a-z0-9\-@%]+/\d+$",
            re.IGNORECASE,
        )
        self.pattern_date = re.compile(r'"startEventDate":"([^"]+)"')

    def extract_team_info(self, event_url: str) -> tuple[tuple[str, str], tuple[str, str]] | None:
        slug = urlparse(event_url).path.split("/event/")[-1].split("/")[0]
        slug = unquote(slug)
        try:
            away_part, home_part = slug.split("@")
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
        url = DRAFTKINGS_LEAGUE_URLS[league]
        self.log.info(f"Detected local timezone: {self.local_tz}")
        now = dt.datetime.now(self.local_tz)
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        end_of_day = start_of_day + dt.timedelta(days=1)
        events: list[Event] = []

        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=True)
            context = await browser.new_context(locale="en-US")
            page = await context.new_page()
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(4000)
            hrefs = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            await browser.close()

        event_urls = sorted(set(h for h in hrefs if self.pattern_event_url.match(h)))
        self.log.info(f"Found {len(event_urls)} event URLs. Fetching metadata...")

        headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*"}
        for event_url in event_urls:
            try:
                r = httpx.get(event_url, headers=headers, timeout=10)
                match = self.pattern_date.search(r.text)
                if not match:
                    continue
                utc_str = match.group(1)
                dt_utc = dt.datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
                dt_local = dt_utc.astimezone(self.local_tz)
                if start_of_day <= dt_local < end_of_day:
                    event_id = event_url.split("/")[-1]
                    away, home = self.extract_team_info(event_url) or (("?", "?"), ("?", "?"))
                    selections = parse_spread_markets(event_id)
                    events.append(Event(event_id, league, event_url, dt_utc, away, home, selections))
            except Exception as e:
                self.log.warning(f"Error fetching {event_url}: {e}")
            await asyncio.sleep(0.5)

        events.sort(key=lambda g: g.start_time)
        self.log.info(f"Total {len(events)} events for today.")
        return events

    def save(self, games: list[Event], league: str, output_dir: pathlib.Path) -> Optional[pathlib.Path]:
        if not games:
            self.log.warning(f"No games to save for {league} today.")
            return

        path = event_filepath(output_dir, league)
        with open(path, "w", encoding="utf-8") as fp:
            json.dump([g.to_dict() for g in games], fp, indent=2)
        self.log.info(f"Saved {len(games)} events to {path}")
        return path

    async def scrape_and_save_all(self, output_dir: pathlib.Path) -> list[pathlib.Path]:
        """Scrape and save each league"""
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


class Selection:
    def __init__(self, event_id: str, data: dict[str, Any]) -> None:
        self.event_id = event_id
        self.data = data

    def to_dict(self) -> dict[str, Any]:
        d = {"event_id": self.event_id}
        d.update(self.data)
        return d


class SelectionChange:
    def __init__(self, selection: Selection) -> None:
        self.created_at = dt.datetime.now(dt.timezone.utc).isoformat()
        self.selection = selection

    def to_json(self) -> str:
        return json.dumps({"created_at": self.created_at, "data": self.selection.to_dict()})


class WebsocketRunner:
    def __init__(self) -> None:
        self.log = logging.getLogger(self.__class__.__name__)
        self.jwt = os.environ["DRAFTKINGS_WS_JWT"]
        self.url = "wss://sportsbook-ws-us-nj.draftkings.com/websocket?format=msgpack&locale=en"
        self.local_tz = get_localzone()

    async def run(self, game: Event, output_dir: pathlib.Path, league: str) -> None:
        state = types.SimpleNamespace()
        self.log.info("Using selections %s for %s", game.selections, game)
        selection_ids = [x["selection_id"] for x in game.selections]
        market_ids = [x["market_id"] for x in game.selections]

        self.log.info("Starting websocket runner for game %s in league %s", game, league)
        subscribe_payload = {
            "jsonrpc": "2.0",
            "params": {
                "entity": "events",
                "queryParams": {
                    "query": f"$filter=id eq '{game.event_id}'",
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
        path = odds_filepath(output_dir, league, game.event_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        self.log.info("Logging odds data for %s to %s", game, path)
        start = time.time()
        events = 0
        last_event_time = time.time()

        while True:
            if asyncio.current_task() and asyncio.current_task().cancelled():
                self.log.info(f"Websocket runner for {game} cancelled, shutting down.")
                break

            try:
                self.log.info(f"Connecting websocket for {game}")
                async with websockets.connect(self.url, max_size=None) as ws:
                    await ws.send(msgpack.packb(subscribe_payload))
                    self.log.info(f"Subscribed to {game}")

                    with open(path, "a", encoding="utf-8") as writer:
                        while True:
                            # Timeout if no events seen
                            now_utc = dt.datetime.now(dt.timezone.utc)
                            runtime = (now_utc - game.start_time).total_seconds() / 60
                            events_per_game_min = events / runtime
                            if time.time() - last_event_time > max_time_since_last_event:
                                self.log.info(f"No events for 2 minutes; assuming {game} has ended.")
                                return

                            if asyncio.current_task() and asyncio.current_task().cancelled():
                                self.log.warning(f"Websocket runner for {game} cancelled mid-loop.")
                                return

                            try:
                                msg = await asyncio.wait_for(ws.recv(), timeout=10)
                            except asyncio.TimeoutError:
                                continue

                            self.log.debug(f"Received bytes: {len(msg)}")

                            try:
                                hits = process_dk_frame(msg, game.event_id, state, selection_ids, market_ids)
                                if hits:
                                    last_event_time = time.time()  # reset idle timer

                                events_per_second = events / (time.time() - start)
                                worker_runtime = (time.time() - start) / 60.0
                                if events % event_log_interval == 0:
                                    logging.info(
                                        f"{CYAN}{game}\t{worker_runtime:,.2f} worker mins.\t{runtime:,.0f} event mins.\t{events_per_game_min:,.2f} msgs/event min.\t\t{events:,.0f} msgs\t\t{events_per_second:.1f} msgs/sec.{RESET}"
                                    )
                                events += 1

                                for hit in hits:
                                    sel = Selection(game.event_id, hit)
                                    change = SelectionChange(sel)
                                    writer.write(change.to_json() + "\n")
                                    writer.flush()
                            except Exception as e:
                                self.log.warning(f"Decode error ({game}): {e}")

            except (
                websockets.exceptions.ConnectionClosedError,
                websockets.exceptions.ConnectionClosedOK,
                ConnectionResetError,
            ) as e:
                self.log.warning(f"Websocket disconnected ({game}): {e}. Reconnecting in 3s...")
                await asyncio.sleep(3)
                continue
            except Exception as e:
                self.log.error(f"Unexpected Websocket error ({game}): {e}")
                await asyncio.sleep(5)


class Monitor:
    def __init__(self, input_dir: pathlib.Path, concurrency: int = 10) -> None:
        self.input_dir = pathlib.Path(input_dir)
        self.output_dir = self.input_dir
        self.concurrency = concurrency
        self.runner = WebsocketRunner()
        self.log = logging.getLogger(self.__class__.__name__)
        self.active_tasks: dict[str, asyncio.Task] = {}
        self.log.info("Monitor using input director at %s", input_dir)

    def _get_today_files(self) -> list[pathlib.Path]:
        today = dt.datetime.now().strftime("%Y%m%d")
        return sorted(self.input_dir.glob(f"*-{today}.json"))

    async def run_once(self) -> None:
        files = self._get_today_files()
        if not files:
            self.log.warning("No event files found for today.")
            return
        for file_path in files:
            league, games = load_events(file_path)
            self.log.info(f"Found {len(games)} scraped games for league {league}.")
            started = [g for g in games if g.has_started() and not g.is_finished()]

            self.log.info(f"Found {len(started)} live games for league {league}.")

            # cancel finished or outdated tasks
            for key in list(self.active_tasks):
                task = self.active_tasks[key]
                league_key, event_id = key.split(":")
                if league_key == league and (task.done() or all(x.event_id != event_id for x in started)):
                    self.log.info(f"Cleaning up finished task for {key}")
                    if not task.done():
                        task.cancel()
                        try:
                            await task
                        except asyncio.CancelledError:
                            self.log.info(f"Cancelled Websocket task for {key}")
                    del self.active_tasks[key]

            # start new ones
            for game in started:
                key = game.get_key()
                if key not in self.active_tasks:
                    self.log.info(f"Starting monitor for {game}")
                    task = asyncio.create_task(self.runner.run(game, self.output_dir, league))
                    self.active_tasks[key] = task
                else:
                    self.log.info(f"Already running task for {game}")


async def main() -> None:
    parser = argparse.ArgumentParser(description="DraftKings Event Monitor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    scrape_p = sub.add_parser("scrape", help="Scrape today's events")
    scrape_p.add_argument(
        "-o", "--output-dir", required=True, type=pathlib.Path, help="Output directory for the JSON file"
    )

    monitor_p = sub.add_parser("monitor", help="Monitor live events")
    monitor_p.add_argument(
        "--input-dir", required=True, type=pathlib.Path, help="Directory containing event JSON files"
    )
    monitor_p.add_argument("--interval", type=int, default=default_loop_interval, help="Refresh interval in seconds")

    args = parser.parse_args()

    if args.cmd == "scrape":
        scraper = EventScraper()
        await scraper.scrape_and_save_all(args.output_dir)
    elif args.cmd == "monitor":
        monitor = Monitor(args.input_dir)
        logger.info(f"Starting persistent monitor (interval={args.interval}s)...")
        while True:
            try:
                await monitor.run_once()
            except Exception as e:
                logger.error(f"Cycle error: {e}")
            logger.info(f"Sleeping {args.interval}s before next cycle...")
            await asyncio.sleep(args.interval)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Shutting down gracefully.")
