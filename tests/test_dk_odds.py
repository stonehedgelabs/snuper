from __future__ import annotations

import pytest

from dk import sanitize_american_odds


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("+105", "105"),
        ("−110", "-110"),
        ("  ＋210 ", "210"),
        ("EVEN", "100"),
        ("pick", "0"),
    ],
)
def test_sanitize_american_odds_normalises_symbols(raw: str, expected: str) -> None:
    assert sanitize_american_odds(raw) == expected


def test_sanitize_american_odds_returns_none_on_missing_digits() -> None:
    assert sanitize_american_odds("abc") is None
    assert sanitize_american_odds(None) is None
