"""Backfill Rolling Insights game_ID on snuper_events rows with a NULL game_ID.

Queries snuper_events for rows on a given date/league/provider whose
data -> 'event' -> 'rollinginsight_game' -> 'game_ID' is NULL, fetches today's
RS schedule via the arb API, fuzzy-matches home+away teams, and updates the
JSONB column in place.
"""

from __future__ import annotations

import argparse
import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import httpx
import psycopg2
import psycopg2.extras

from snuper.constants import RED, YELLOW, CYAN, RESET
from snuper.utils import _fuzzy_match_team_name

logger = logging.getLogger("snuper.merge_rs_games")


DEFAULT_ARB_URL = "https://api.arbi.gg"


@dataclass(frozen=True)
class PendingEvent:
    """A snuper_events row that needs an RS game_ID populated."""

    row_id: int
    event_id: str
    home: tuple[str, ...]
    away: tuple[str, ...]


@dataclass(frozen=True)
class ScheduleGame:
    """A game from /api/v2/schedule with just what we need for matching."""

    game_id: str
    home_team: str
    away_team: str


def _to_tokens(raw: Any) -> tuple[str, ...]:
    """Coerce a JSON list (or None) into a lowercase token tuple."""
    if not raw:
        return ()
    if isinstance(raw, str):
        raw = json.loads(raw)
    return tuple(str(t).lower() for t in raw if t)


def match_event_to_game(event: PendingEvent, games: Iterable[ScheduleGame]) -> ScheduleGame | None:
    """Return the schedule game matching both teams of `event`, or None.

    Uses snuper's existing `_fuzzy_match_team_name` on both sides. Requires
    that home and away align on the same side (does NOT accept a swapped
    match — the DB row's home/away are authoritative here).

    If multiple games match, returns None (ambiguous) and logs a warning.
    """
    if not event.home or not event.away:
        return None

    matches: list[ScheduleGame] = []
    for game in games:
        if _fuzzy_match_team_name(event.home, game.home_team) and _fuzzy_match_team_name(event.away, game.away_team):
            matches.append(game)

    if not matches:
        return None
    if len(matches) > 1:
        logger.warning(
            "ambiguous match for event_id=%s: %s",
            event.event_id,
            [m.game_id for m in matches],
        )
        return None
    return matches[0]


def fetch_pending_events(conn, provider: str, league: str, date: str) -> list[PendingEvent]:
    """Fetch snuper_events rows for (provider, league, date) with null RS game_ID."""
    sql = """
        SELECT id,
               event_id,
               data->'event'->'home' AS home,
               data->'event'->'away' AS away
        FROM snuper_events
        WHERE provider = %s
          AND league = %s
          AND created_at::date = %s
          AND (
              data->'event'->'rollinginsight_game' IS NULL
              OR data->'event'->'rollinginsight_game' = 'null'::jsonb
              OR data->'event'->'rollinginsight_game'->>'game_ID' IS NULL
          )
        ORDER BY id
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, (provider, league, date))
        rows = cur.fetchall()

    return [
        PendingEvent(
            row_id=r["id"],
            event_id=r["event_id"],
            home=_to_tokens(r["home"]),
            away=_to_tokens(r["away"]),
        )
        for r in rows
    ]


def fetch_schedule_games(arb_url: str, league: str, date: str, timeout: float = 30.0) -> list[ScheduleGame]:
    """GET /api/v2/schedule and flatten to ScheduleGame list."""
    url = f"{arb_url.rstrip('/')}/api/v2/schedule"
    resp = httpx.get(url, params={"league": league, "date": date}, timeout=timeout)
    resp.raise_for_status()
    payload = resp.json()
    raw = payload.get("data") or []
    return [
        ScheduleGame(
            game_id=str(g["game_ID"]),
            home_team=g.get("home_team") or "",
            away_team=g.get("away_team") or "",
        )
        for g in raw
        if g.get("game_ID")
    ]


def update_event_rs_game_id(conn, row_id: int, game_id: str) -> None:
    """Set data->'event'->'rollinginsight_game'->>'game_ID' for a given row."""
    sql = """
        UPDATE snuper_events
        SET data = jsonb_set(
            CASE
                WHEN jsonb_typeof(data->'event'->'rollinginsight_game') = 'object'
                    THEN data
                ELSE jsonb_set(
                    data, '{event,rollinginsight_game}', '{}'::jsonb, true
                )
            END,
            '{event,rollinginsight_game,game_ID}',
            to_jsonb(%s::text),
            true
        )
        WHERE id = %s
    """
    with conn.cursor() as cur:
        cur.execute(sql, (game_id, row_id))


def run_backfill(args: argparse.Namespace) -> int:
    """Top-level entry invoked from cli.py when -t backfill-rs-games."""
    provider = args.provider[0] if isinstance(args.provider, list) else args.provider
    league = args.league[0] if isinstance(args.league, list) else args.league
    date = args.date
    arb_url = getattr(args, "arb_url", None) or DEFAULT_ARB_URL
    dry_run = bool(getattr(args, "dry_run", False))

    print(
        f"{CYAN}backfill-rs-games{RESET} "
        f"provider={provider} league={league} date={date} "
        f"dry_run={dry_run} arb_url={arb_url}"
    )

    conn = psycopg2.connect(args.rds_uri)
    try:
        pending = fetch_pending_events(conn, provider, league, date)
        print(f"{YELLOW}found {len(pending)} pending event(s){RESET}")
        if not pending:
            return 0

        games = fetch_schedule_games(arb_url, league, date)
        print(f"{YELLOW}fetched {len(games)} schedule game(s){RESET}")
        if not games:
            print(f"{RED}no schedule games returned — aborting{RESET}")
            return 1

        matched = 0
        unmatched = 0
        for ev in pending:
            game = match_event_to_game(ev, games)
            if not game:
                unmatched += 1
                print(f"  {RED}[MISS]{RESET} event_id={ev.event_id} " f"home={list(ev.home)} away={list(ev.away)}")
                continue
            matched += 1
            print(
                f"  {CYAN}[MATCH]{RESET} event_id={ev.event_id} "
                f"-> game_ID={game.game_id} "
                f"({game.away_team} @ {game.home_team})"
            )
            if not dry_run:
                update_event_rs_game_id(conn, ev.row_id, game.game_id)

        if not dry_run:
            conn.commit()
            print(f"{YELLOW}committed {matched} update(s){RESET}")
        else:
            conn.rollback()
            print(f"{YELLOW}dry-run: rolled back, no changes written{RESET}")

        print(f"summary: pending={len(pending)} matched={matched} " f"unmatched={unmatched}")
        return 0 if unmatched == 0 else 2
    finally:
        conn.close()
