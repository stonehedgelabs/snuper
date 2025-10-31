from __future__ import annotations

import argparse
import asyncio
import pathlib
import logging
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from dotenv import load_dotenv

from snuper import betmgm, bovada, draftkings, fanduel
from snuper.constants import Provider, SUPPORTED_LEAGUES

load_dotenv()

logger = logging.getLogger("snuper")


class ScrapeRunner(Protocol):
    async def __call__(
        self,
        output_dir: pathlib.Path,
        *,
        leagues: Sequence[str] | None = None,
        overwrite: bool = False,
    ) -> None: ...


PROVIDER_ALIASES: dict[str, str] = {
    "draftkings": "draftkings",
    "betmgm": "betmgm",
    "fanduel": "fanduel",
    "bovada": "bovada",
}

PROVIDER_SCRAPE: dict[str, ScrapeRunner] = {
    "draftkings": draftkings.run_scrape,
    "betmgm": betmgm.run_scrape,
    "fanduel": fanduel.run_scrape,
    "bovada": bovada.run_scrape,
}

PROVIDER_MONITOR: dict[str, Callable[..., Awaitable[None]]] = {
    "draftkings": draftkings.run_monitor,
    "betmgm": betmgm.run_monitor,
    "fanduel": fanduel.run_monitor,
    "bovada": bovada.run_monitor,
}


def canonical_provider(value: str) -> str:
    try:
        return PROVIDER_ALIASES[value.lower()]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Unknown provider alias: {value}") from exc


def provider_argument(value: str) -> list[str]:
    items = [chunk.strip() for chunk in value.split(",")]
    providers: list[str] = []

    for item in items:
        if not item:
            continue
        try:
            canonical = canonical_provider(item)
        except ValueError as exc:  # pragma: no cover - defensive guard
            raise argparse.ArgumentTypeError(str(exc)) from exc
        if canonical not in providers:
            providers.append(canonical)

    if not providers:
        raise argparse.ArgumentTypeError("No providers supplied")

    return providers


def league_argument(value: str) -> list[str]:
    items = [chunk.strip().lower() for chunk in value.split(",")]
    leagues: list[str] = []

    for item in items:
        if not item:
            continue
        if item not in SUPPORTED_LEAGUES:
            raise argparse.ArgumentTypeError(f"Unsupported league: {item}")
        if item not in leagues:
            leagues.append(item)

    if not leagues:
        raise argparse.ArgumentTypeError("No leagues supplied")

    return leagues


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Unified Event Monitor CLI")
    parser.add_argument(
        "-p",
        "--provider",
        type=provider_argument,
        help="Comma-separated list of sportsbook providers (omit to run all)",
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
        "-l",
        "--league",
        type=league_argument,
        help="Comma-separated list of leagues to limit (omit for all)",
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
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing snapshots instead of skipping",
    )
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    if args.output_dir is None:
        parser.error("--output-dir is required for all tasks")


async def dispatch(args: argparse.Namespace) -> None:
    if args.provider:
        providers = list(args.provider)
    else:
        providers = list(PROVIDER_SCRAPE.keys())

    task = args.task
    leagues = list(args.league) if args.league else None

    if task == "scrape":
        for provider in providers:
            if provider == Provider.FanDuel.value:
                logger.info("Skipping unsupported provider %s", provider)
                continue
            runner = PROVIDER_SCRAPE[provider]
            provider_dir = args.output_dir / provider
            await runner(provider_dir, leagues=leagues, overwrite=args.overwrite)
        return

    monitor_tasks: list[Awaitable[None]] = []
    for provider in providers:
        runner = PROVIDER_MONITOR[provider]
        events_dir = args.output_dir / provider / "events"
        if provider == Provider.DraftKings.value:
            interval = args.interval or draftkings.DRAFTKINGS_DEFAULT_MONITOR_INTERVAL
            monitor_tasks.append(runner(events_dir, interval=interval, leagues=leagues))
        elif provider == Provider.FanDuel.value:
            logger.info("Skipping unsupported provider %s", provider)
        else:
            monitor_tasks.append(runner(events_dir, leagues=leagues))

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
