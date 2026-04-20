"""Unit tests for snuper.merge_rs_games team-matching logic.

These tests exercise the pure matching function only — no DB, no HTTP.
They verify that real-world abbreviated-token pairs from snuper_events
match the correct full team names returned by the arb /schedule endpoint.
"""

from __future__ import annotations

import pytest

from snuper.merge_rs_games import (
    PendingEvent,
    ScheduleGame,
    _to_tokens,
    match_event_to_game,
)


def _ev(event_id: str, home: tuple[str, ...], away: tuple[str, ...]) -> PendingEvent:
    return PendingEvent(row_id=1, event_id=event_id, home=home, away=away)


def _g(game_id: str, home_team: str, away_team: str) -> ScheduleGame:
    return ScheduleGame(game_id=game_id, home_team=home_team, away_team=away_team)


NBA_SCHEDULE = [
    _g("20260419001", "Detroit Pistons", "Orlando Magic"),
    _g("20260419002", "Golden State Warriors", "Los Angeles Lakers"),
    _g("20260419003", "San Antonio Spurs", "Portland Trail Blazers"),
    _g("20260419004", "LA Clippers", "New Orleans Pelicans"),
    _g("20260419005", "Philadelphia 76ers", "Brooklyn Nets"),
    _g("20260419006", "Oklahoma City Thunder", "Memphis Grizzlies"),
]


class TestMatchEventToGame:
    def test_det_pistons_vs_orl_magic(self):
        event = _ev("34000182", home=("det", "pistons"), away=("orl", "magic"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is not None
        assert result.game_id == "20260419001"

    def test_por_trail_blazers_at_sa_spurs(self):
        event = _ev("34000183", home=("sa", "spurs"), away=("por", "trail-blazers"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is not None
        assert result.game_id == "20260419003"

    def test_la_lakers_vs_gs_warriors_not_clippers(self):
        event = _ev("34000184", home=("gs", "warriors"), away=("la", "lakers"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is not None
        assert result.game_id == "20260419002"
        assert result.home_team == "Golden State Warriors"

    def test_la_clippers_picks_clippers_not_lakers(self):
        event = _ev("34000185", home=("la", "clippers"), away=("no", "pelicans"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is not None
        assert result.game_id == "20260419004"
        assert result.home_team == "LA Clippers"

    def test_okc_thunder_vs_mem_grizzlies(self):
        event = _ev("34000186", home=("okc", "thunder"), away=("mem", "grizzlies"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is not None
        assert result.game_id == "20260419006"

    def test_phi_76ers_vs_bkn_nets(self):
        event = _ev("34000187", home=("phi", "76ers"), away=("bkn", "nets"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is not None
        assert result.game_id == "20260419005"


class TestMismatches:
    def test_home_matches_but_away_does_not(self):
        event = _ev("X", home=("det", "pistons"), away=("bos", "celtics"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is None

    def test_teams_not_in_schedule(self):
        event = _ev("X", home=("mia", "heat"), away=("chi", "bulls"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is None

    def test_empty_schedule(self):
        event = _ev("X", home=("det", "pistons"), away=("orl", "magic"))
        result = match_event_to_game(event, [])
        assert result is None

    def test_empty_home_tokens(self):
        event = _ev("X", home=(), away=("orl", "magic"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is None

    def test_empty_away_tokens(self):
        event = _ev("X", home=("det", "pistons"), away=())
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is None

    def test_swapped_sides_does_not_match(self):
        """If the DB row has home/away flipped vs the schedule, we do NOT match.

        The DB's home/away are authoritative for this backfill path; a swapped
        match would silently write the wrong game alignment.
        """
        event = _ev("X", home=("orl", "magic"), away=("det", "pistons"))
        result = match_event_to_game(event, NBA_SCHEDULE)
        assert result is None


class TestNFL:
    def test_ne_patriots_at_buf_bills(self):
        schedule = [
            _g("NFL001", "Buffalo Bills", "New England Patriots"),
            _g("NFL002", "New York Jets", "Miami Dolphins"),
        ]
        event = _ev("NFL-X", home=("buf", "bills"), away=("ne", "patriots"))
        result = match_event_to_game(event, schedule)
        assert result is not None
        assert result.game_id == "NFL001"

    def test_nyj_not_nyg(self):
        schedule = [
            _g("NFL010", "New York Jets", "Miami Dolphins"),
            _g("NFL011", "New York Giants", "Dallas Cowboys"),
        ]
        event = _ev("NFL-Y", home=("nyj", "jets"), away=("mia", "dolphins"))
        result = match_event_to_game(event, schedule)
        assert result is not None
        assert result.game_id == "NFL010"


class TestMLB:
    def test_nyy_yankees_not_nym_mets(self):
        schedule = [
            _g("MLB001", "New York Yankees", "Boston Red Sox"),
            _g("MLB002", "New York Mets", "Atlanta Braves"),
        ]
        event = _ev("MLB-X", home=("ny", "yankees"), away=("bos", "red-sox"))
        result = match_event_to_game(event, schedule)
        assert result is not None
        assert result.game_id == "MLB001"

    def test_nym_mets_not_nyy_yankees(self):
        schedule = [
            _g("MLB001", "New York Yankees", "Boston Red Sox"),
            _g("MLB002", "New York Mets", "Atlanta Braves"),
        ]
        event = _ev("MLB-Y", home=("ny", "mets"), away=("atl", "braves"))
        result = match_event_to_game(event, schedule)
        assert result is not None
        assert result.game_id == "MLB002"

    def test_sf_giants_not_ny_giants_wrong_league_mix(self):
        schedule = [
            _g("MLB010", "San Francisco Giants", "Los Angeles Dodgers"),
        ]
        event = _ev("MLB-Z", home=("sf", "giants"), away=("la", "dodgers"))
        result = match_event_to_game(event, schedule)
        assert result is not None
        assert result.game_id == "MLB010"


class TestToTokens:
    def test_list_input(self):
        assert _to_tokens(["DET", "Pistons"]) == ("det", "pistons")

    def test_json_string_input(self):
        assert _to_tokens('["det", "pistons"]') == ("det", "pistons")

    def test_none_input(self):
        assert _to_tokens(None) == ()

    def test_empty_list(self):
        assert _to_tokens([]) == ()

    def test_filters_empty_strings(self):
        assert _to_tokens(["det", "", "pistons"]) == ("det", "pistons")
