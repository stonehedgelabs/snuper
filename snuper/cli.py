"""Command-line interface for the snuper sports odds monitoring tool.

This module provides the main CLI entry point with commands for:
- Scraping event schedules from sportsbooks
- Monitoring live odds via websockets or polling
- Configuring output sinks (filesystem, RDS, cache)
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import pathlib
import tempfile
from collections.abc import Awaitable, Callable, Sequence
from typing import Protocol

from dotenv import load_dotenv

from snuper import betmgm, bovada, draftkings, fanduel
from snuper.config import load_config
from snuper.sinks import SelectionSink, SinkType, build_sink
from snuper.constants import Provider, SUPPORTED_LEAGUES

load_dotenv()

logger = logging.getLogger("snuper")


class ScrapeRunner(Protocol):
    """Protocol defining the interface for provider-specific scraping functions."""

    async def __call__(
        self,
        output_dir: pathlib.Path,
        *,
        leagues: Sequence[str] | None = None,
        overwrite: bool = False,
    ) -> None: ...


# Mapping of provider names to their canonical identifiers.
PROVIDER_ALIASES: dict[str, str] = {
    Provider.DraftKings.value: Provider.DraftKings.value,
    # Provider.BetMGM.value: Provider.BetMGM.value,
    # Provider.FanDuel.value: Provider.FanDuel.value,
    Provider.Bovada.value: Provider.Bovada.value,
}

# Mapping of providers to their scraping implementations.
PROVIDER_SCRAPE: dict[str, ScrapeRunner] = {
    Provider.DraftKings.value: draftkings.run_scrape,
    Provider.BetMGM.value: betmgm.run_scrape,
    Provider.FanDuel.value: fanduel.run_scrape,
    Provider.Bovada.value: bovada.run_scrape,
}

# Mapping of providers to their live monitoring implementations.
PROVIDER_MONITOR: dict[str, Callable[..., Awaitable[None]]] = {
    Provider.DraftKings.value: draftkings.run_monitor,
    Provider.BetMGM.value: betmgm.run_monitor,
    Provider.FanDuel.value: fanduel.run_monitor,
    Provider.Bovada.value: bovada.run_monitor,
}


def canonical_provider(value: str) -> str:
    """Resolve a provider string to its canonical name.

    Raises:
        ValueError: If the provider is not recognized.
    """
    try:
        return PROVIDER_ALIASES[value.lower()]
    except KeyError as exc:  # pragma: no cover - defensive guard
        raise ValueError(f"Unknown provider alias: {value}") from exc


def provider_argument(value: str) -> list[str]:
    """Parse a comma-separated list of provider names for argparse."""
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
    """Parse a comma-separated list of league identifiers for argparse."""
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


async def _run_scrape_task(
    *,
    providers: Sequence[str],
    leagues: Sequence[str] | None,
    fs_sink_dir: pathlib.Path,
    overwrite: bool,
    sink: SelectionSink,
    merge_sportdata_games: bool = False,
    merge_rollinginsights_games: bool = False,
) -> None:
    for provider in providers:
        if provider == Provider.FanDuel.value or provider == Provider.BetMGM.value:
            logger.info("Skipping scrape for unsupported provider %s", provider)
            continue
        runner = PROVIDER_SCRAPE[provider]
        provider_dir = fs_sink_dir / provider
        await runner(
            provider_dir,
            leagues=leagues,
            overwrite=overwrite,
            sink=sink,
            merge_sportdata_games=merge_sportdata_games,
            merge_rollinginsights_games=merge_rollinginsights_games,
        )


async def _run_monitor_task(
    *,
    providers: Sequence[str],
    leagues: Sequence[str] | None,
    fs_sink_dir: pathlib.Path,
    sink: SelectionSink,
    monitor_interval: int | None,
) -> None:
    monitor_tasks: list[Awaitable[None]] = []
    for provider in providers:
        if provider == Provider.FanDuel.value or provider == Provider.BetMGM.value:
            logger.info("Skipping scrape for unsupported provider %s", provider)
            continue
        runner = PROVIDER_MONITOR[provider]
        events_dir = fs_sink_dir / provider
        if provider == Provider.DraftKings.value:
            interval = monitor_interval or draftkings.DRAFTKINGS_DEFAULT_MONITOR_INTERVAL
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

    if monitor_tasks:
        await asyncio.gather(*monitor_tasks)


def build_parser() -> argparse.ArgumentParser:
    """Construct the argument parser for the snuper CLI."""
    parser = argparse.ArgumentParser(description="Unified Event Monitor CLI")
    parser.add_argument(
        "-p",
        "--provider",
        type=provider_argument,
        help="Comma-separated list of sportsbook providers (omit to run all)",
    )
    parser.add_argument(
        "-t",
        "--task",
        choices=["scrape", "monitor"],
        required=True,
        help="Operation to perform",
    )
    parser.add_argument(
        "-c",
        "--config",
        type=pathlib.Path,
        help="Path to the TOML configuration file",
    )
    parser.add_argument(
        "-l",
        "--league",
        type=league_argument,
        help="Comma-separated list of leagues to limit (omit for all)",
    )
    parser.add_argument(
        "--fs-sink-dir",
        type=pathlib.Path,
        help="Base directory for filesystem snapshots and odds logs",
    )
    parser.add_argument(
        "--monitor-interval",
        dest="monitor_interval",
        type=int,
        help="Refresh interval in seconds for the DraftKings monitor",
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
    parser.add_argument(
        "--merge-sportdata-games",
        action="store_true",
        help="Match and merge Sportdata games with scraped events before saving (requires --task scrape)",
    )
    parser.add_argument(
        "--merge-rollinginsights-games",
        action="store_true",
        help="Match and merge Rolling Insights games with scraped events before saving (requires --task scrape)",
    )
    parser.add_argument(
        "--merge-all-games",
        action="store_true",
        help="Match and merge both Sportdata and Rolling Insights games (equivalent to using both --merge-sportdata-games and --merge-rollinginsights-games)",
    )
    return parser


def validate_args(parser: argparse.ArgumentParser, args: argparse.Namespace) -> None:
    """Validate parsed command-line arguments and enforce constraints."""
    if args.config is not None and not args.config.is_file():
        parser.error("--config must point to an existing configuration file")
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
    if args.merge_all_games and (args.merge_sportdata_games or args.merge_rollinginsights_games):
        parser.error("--merge-all-games cannot be used with --merge-sportdata-games or --merge-rollinginsights-games")
    if (
        args.merge_sportdata_games or args.merge_rollinginsights_games or args.merge_all_games
    ) and args.task != "scrape":
        parser.error(
            "--merge-sportdata-games, --merge-rollinginsights-games, and --merge-all-games require --task scrape"
        )
    if (args.merge_sportdata_games or args.merge_all_games) and args.config is None:
        parser.error("--merge-sportdata-games and --merge-all-games require --config to be specified")


async def dispatch(args: argparse.Namespace) -> None:
    """Execute the requested CLI task (scrape or monitor)."""
    if args.config is not None:
        load_config(args.config)
        logger.info("Loaded configuration from %s", args.config)

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
            raise ValueError("--rds-table is required when monitoring uses the rds sink")
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

    merge_sportdata_games = args.merge_sportdata_games or args.merge_all_games
    merge_rollinginsights_games = args.merge_rollinginsights_games or args.merge_all_games

    if task == "scrape":
        await _run_scrape_task(
            providers=providers,
            leagues=leagues,
            fs_sink_dir=fs_sink_dir,
            overwrite=args.overwrite,
            sink=sink,
            merge_sportdata_games=merge_sportdata_games,
            merge_rollinginsights_games=merge_rollinginsights_games,
        )
        await sink.close()
        if temp_fs_sink_dir:
            logger.debug("Temporary staging directory %s will remain for this run", temp_fs_sink_dir)
        return

    await _run_monitor_task(
        providers=providers,
        leagues=leagues,
        fs_sink_dir=fs_sink_dir,
        sink=sink,
        monitor_interval=args.monitor_interval,
    )


def main() -> None:
    """Main entry point for the snuper CLI application."""
    parser = build_parser()
    args = parser.parse_args()
    validate_args(parser, args)
    try:
        asyncio.run(dispatch(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
