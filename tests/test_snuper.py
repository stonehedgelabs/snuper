import argparse
import datetime as dt

import pytest
from tzlocal import get_localzone

from snuper.cli import _next_scrape_run, scrape_interval_argument


def test_scrape_interval_argument_parses_am_time() -> None:
    result = scrape_interval_argument("6am")
    assert result == dt.time(6, 0)


def test_scrape_interval_argument_parses_24_hour_time() -> None:
    result = scrape_interval_argument("18:15")
    assert result == dt.time(18, 15)


def test_scrape_interval_argument_rejects_invalid_value() -> None:
    with pytest.raises(argparse.ArgumentTypeError):
        scrape_interval_argument("not-a-time")


def test_next_scrape_run_returns_same_day_when_future() -> None:
    tz = get_localzone()
    now = dt.datetime(2024, 3, 2, 5, 0, tzinfo=tz)
    target = dt.time(6, 0)
    next_run = _next_scrape_run(target, now=now)
    assert next_run.date() == now.date()
    assert next_run.time() == dt.time(6, 0)


def test_next_scrape_run_rolls_forward_when_past() -> None:
    tz = get_localzone()
    now = dt.datetime(2024, 3, 2, 8, 0, tzinfo=tz)
    target = dt.time(6, 0)
    next_run = _next_scrape_run(target, now=now)
    assert next_run.date() == (now + dt.timedelta(days=1)).date()
    assert next_run.time() == dt.time(6, 0)
