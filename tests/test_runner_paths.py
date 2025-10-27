from __future__ import annotations

from pathlib import Path

import asyncio

import pytest

from event_monitor.runner import BaseMonitor, BaseRunner


class StubRunner(BaseRunner):
    """Minimal runner implementation used for monitor path tests."""

    async def run(self, event, output_dir: Path, league: str) -> None:  # pragma: no cover - unused in path tests
        await asyncio.sleep(0)

    def emit_heartbeat(self, event, *, stats: dict[str, object]) -> None:  # pragma: no cover - unused in path tests
        return None


@pytest.fixture
def runner() -> StubRunner:
    return StubRunner()


def test_monitor_defaults_to_parent_of_events_dir(tmp_path: Path, runner: StubRunner) -> None:
    events_dir = tmp_path / "draftkings" / "events"
    events_dir.mkdir(parents=True)

    monitor = BaseMonitor(events_dir, runner)

    assert monitor.output_dir == events_dir


def test_monitor_keeps_input_dir_when_not_events(tmp_path: Path, runner: StubRunner) -> None:
    input_dir = tmp_path / "snapshots"
    input_dir.mkdir()

    monitor = BaseMonitor(input_dir, runner)

    assert monitor.output_dir == input_dir


def test_monitor_respects_explicit_output_dir(tmp_path: Path, runner: StubRunner) -> None:
    input_dir = tmp_path / "draftkings" / "events"
    input_dir.mkdir(parents=True)
    explicit_output = tmp_path / "draftkings"

    monitor = BaseMonitor(input_dir, runner, output_dir=explicit_output)

    assert monitor.output_dir == explicit_output
