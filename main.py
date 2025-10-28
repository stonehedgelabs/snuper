from __future__ import annotations

import argparse
import asyncio
import pathlib
from collections.abc import Awaitable, Callable
from typing import Protocol

from event_monitor import draftkings, fanduel, betmgm


class ScrapeRunner(Protocol):
    async def __call__(self, output_dir: pathlib.Path) -> None: ...


PROVIDER_ALIASES: dict[str, str] = {
    "draftkings": "draftkings",
    "betmgm": "betmgm",
    # "fanduel": "fanduel",
}

PROVIDER_SCRAPE: dict[str, ScrapeRunner] = {
    "draftkings": draftkings.run_scrape,
    "betmgm": betmgm.run_scrape,
    # "fanduel": fanduel.run_scrape,
}

PROVIDER_MONITOR: dict[str, Callable[..., Awaitable[None]]] = {
    "draftkings": draftkings.run_monitor,
    "betmgm": betmgm.run_monitor,
    # "fanduel": fanduel.run_monitor,
}


def canonical_provider(value: str) -> str:
    try:
        return PROVIDER_ALIASES[value.lower()]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Unknown provider alias: {value}") from exc


def provider_argument(value: str) -> str:
    try:
        return canonical_provider(value)
    except ValueError as exc:  # pragma: no cover - defensive guard
        raise argparse.ArgumentTypeError(str(exc)) from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified Event Monitor CLI")
    parser.add_argument(
        "-p",
        "--provider",
        type=provider_argument,
        choices=sorted(PROVIDER_SCRAPE.keys()),
        help="Target sportsbook provider (omit to run all)",
    )
    parser.add_argument(
        "-t",
        "-task",
        "--task",
        choices=["scrape", "monitor"],
        required=True,
        help="Operation to perform",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        type=pathlib.Path,
        help="Base directory where provider artifacts are written",
    )
    parser.add_argument(
        "-i",
        "--interval",
        type=int,
        help="Refresh interval in seconds (DraftKings monitor only)",
    )
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.output_dir is None:
        parser.error("--output-dir is required for all tasks")


async def dispatch(args: argparse.Namespace) -> None:
    providers: list[str]
    if args.provider:
        providers = [args.provider]
    else:
        providers = list(PROVIDER_SCRAPE.keys())

    task = args.task

    if task == "scrape":
        for provider in providers:
            runner = PROVIDER_SCRAPE[provider]
            provider_dir = args.output_dir / provider
            await runner(provider_dir)
        return

    monitor_tasks: list[Awaitable[None]] = []
    for provider in providers:
        runner = PROVIDER_MONITOR[provider]
        events_dir = args.output_dir / provider / "events"
        if provider == "draftkings":
            interval = args.interval or draftkings.DRAFTKINGS_DEFAULT_MONITOR_INTERVAL
            monitor_tasks.append(runner(events_dir, interval=interval))
        else:
            monitor_tasks.append(runner(events_dir))

    await asyncio.gather(*monitor_tasks)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)
    try:
        asyncio.run(dispatch(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
