from __future__ import annotations

import argparse
import asyncio
import pathlib
import logging
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from dotenv import load_dotenv

from snuper import betmgm, bovada, draftkings, fanduel
from snuper.sinks import SinkType, build_sink
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
        "--fs-sink-dir",
        type=pathlib.Path,
        help="Base directory for filesystem snapshots and odds logs",
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
    parser.add_argument(
        "--sink",
        choices=[sink.value for sink in SinkType],
        default=SinkType.FS.value,
        help="Destination sink for selection updates (default: fs)",
    )
    parser.add_argument(
        "--rds-uri",
        help="Database connection URI when using the rds sink",
    )
    parser.add_argument(
        "--rds-table",
        help="Table name used by the rds sink",
    )
    parser.add_argument(
        "--cache-uri",
        help="Cache connection URI when using the cache sink",
    )
    parser.add_argument(
        "--cache-ttl",
        type=int,
        help="Expiration window in seconds for cache sink entries",
    )
    parser.add_argument(
        "--cache-max-items",
        type=int,
        help="Maximum list length per event stored in the cache sink",
    )
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    sink_type = SinkType(args.sink)
    if sink_type is SinkType.FS and args.fs_sink_dir is None:
        parser.error("--fs-sink-dir is required when --sink=fs")
    if sink_type is SinkType.RDS:
        if not args.rds_uri or not args.rds_table:
            parser.error("--rds-uri and --rds-table are required when --sink=rds")
    if sink_type is SinkType.CACHE:
        if not args.cache_uri:
            parser.error("--cache-uri is required when --sink=cache")
        if args.cache_ttl is None or args.cache_ttl <= 0:
            parser.error("--cache-ttl must be provided as a positive integer when --sink=cache")
        if args.cache_max_items is None or args.cache_max_items <= 0:
            parser.error("--cache-max-items must be provided as a positive integer when --sink=cache")


async def dispatch(args: argparse.Namespace) -> None:
    if args.provider:
        providers = list(args.provider)
    else:
        providers = list(PROVIDER_SCRAPE.keys())

    task = args.task
    leagues = list(args.league) if args.league else None
    sink_type = SinkType(args.sink)
    fs_sink_dir: pathlib.Path | None = pathlib.Path(args.fs_sink_dir) if args.fs_sink_dir else None
    temp_fs_sink_dir: pathlib.Path | None = None
    if fs_sink_dir is None:
        temp_fs_sink_dir = pathlib.Path(tempfile.mkdtemp(prefix="snuper-staging-"))
        fs_sink_dir = temp_fs_sink_dir
        logger.debug("No --fs-sink-dir supplied; using temporary staging directory %s", fs_sink_dir)

    rds_selection_table: str | None = None
    if sink_type is SinkType.RDS and task == "monitor":
        if args.rds_table is None:
            raise ValueError("--rds-table is required when --task monitor uses the rds sink")
        rds_selection_table = f"{args.rds_table}_selection_changes"

    sink = build_sink(
        sink_type=sink_type,
        fs_sink_dir=fs_sink_dir if sink_type is SinkType.FS else None,
        rds_uri=args.rds_uri,
        rds_table=args.rds_table,
        rds_selection_table=rds_selection_table,
        cache_uri=args.cache_uri,
        cache_ttl=args.cache_ttl,
        cache_max_items=args.cache_max_items,
    )

    if task == "scrape":
        for provider in providers:
            if provider == Provider.FanDuel.value:
                logger.info("Skipping unsupported provider %s", provider)
                continue
            runner = PROVIDER_SCRAPE[provider]
            provider_dir = fs_sink_dir / provider
            await runner(
                provider_dir,
                leagues=leagues,
                overwrite=args.overwrite,
                sink=sink,
            )
        await sink.close()
        if temp_fs_sink_dir:
            logger.debug("Temporary staging directory %s will remain for this run", temp_fs_sink_dir)
        return

    monitor_tasks: list[Awaitable[None]] = []
    for provider in providers:
        runner = PROVIDER_MONITOR[provider]
        events_dir = fs_sink_dir / provider
        if provider == Provider.DraftKings.value:
            interval = args.interval or draftkings.DRAFTKINGS_DEFAULT_MONITOR_INTERVAL
            monitor_tasks.append(
                runner(
                    events_dir,
                    interval=interval,
                    leagues=leagues,
                    sink=sink,
                    provider=provider,
                    output_dir=events_dir,
                )
            )
        elif provider == Provider.FanDuel.value:
            logger.info("Skipping unsupported provider %s", provider)
        else:
            monitor_tasks.append(
                runner(
                    events_dir,
                    leagues=leagues,
                    sink=sink,
                    provider=provider,
                    output_dir=events_dir,
                )
            )

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
