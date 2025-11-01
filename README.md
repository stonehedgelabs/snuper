# snuper
![CI](https://github.com/stonehedgelabs/snuper/actions/workflows/ci.yaml/badge.svg)

<div display="flex" align-items="center">
  <img src="https://img.shields.io/badge/OpenAI-412991?style=for-the-badge&logo=openai&logoColor=white" />
  <img src="https://img.shields.io/badge/Python-3776AB?style=for-the-badge&logo=python&logoColor=white" />
</div>

- [Overview](#overview)
- [Usage](#usage)
- [Coverage](#coverage)
- [Workflows](#workflows)
  - [Scrape](#scrape)
  - [Monitor](#monitor)
- [Output](#output)
- [Development](#development)
- [Glossary](#glossary)

Asynchronous tooling for collecting sportsbook events, persisting immutable
snapshots, and streaming live odds updates.

## Overview

- Asynchronous CLI for gathering sportsbook events and live odds updates.
- Integrates DraftKings, BetMGM, Bovada, and FanDuel (monitoring in progress).
- Targets NBA, NFL, and MLB leagues with an extensible storage interface.

## Usage

```text
usage: snuper [-h] [-p PROVIDER] -t {scrape,monitor} [-l LEAGUE] [-o FS_SINK_DIR] [-i INTERVAL] [--overwrite] [--sink {fs,rds,cache}] [--rds-uri RDS_URI] [--rds-table RDS_TABLE] [--cache-uri CACHE_URI]
              [--cache-ttl CACHE_TTL] [--cache-max-items CACHE_MAX_ITEMS]

Unified Event Monitor CLI

options:
  -h, --help            show this help message and exit
  -p PROVIDER, --provider PROVIDER
                        Comma-separated list of sportsbook providers (omit to run all)
  -t {scrape,monitor}, -task {scrape,monitor}, --task {scrape,monitor}
                        Operation to perform
  -l LEAGUE, --league LEAGUE
                        Comma-separated list of leagues to limit (omit for all)
  -o FS_SINK_DIR, --fs-sink-dir FS_SINK_DIR
                        Base directory for filesystem snapshots and odds logs
  -i INTERVAL, --interval INTERVAL
                        Refresh interval in seconds (DraftKings monitor only)
  --overwrite           Overwrite existing snapshots instead of skipping
  --sink {fs,rds,cache}
                        Destination sink for selection updates (default: fs)
  --rds-uri RDS_URI     Database connection URI when using the rds sink
  --rds-table RDS_TABLE
                        Table name used by the rds sink
  --cache-uri CACHE_URI
                        Cache connection URI when using the cache sink
  --cache-ttl CACHE_TTL
                        Expiration window in seconds for cache sink entries
  --cache-max-items CACHE_MAX_ITEMS
                        Maximum list length per event stored in the cache sink
```

- Providers must be supplied using their full names (e.g., `draftkings`,
  `betmgm`, `bovada`, `fanduel`). Omit `--provider` to run every available
  scraper or monitor.
- `--fs-sink-dir` is required when `--sink=fs`; for other sinks a temporary
  staging directory is created automatically if you omit the flag.
- Select a destination with `--sink {fs,rds,cache}` and supply the matching
  connection flags (e.g., `--rds-uri`, `--cache-uri`).
- When using `--sink=rds`, pass a SQLAlchemy-compatible URI via `--rds-uri` (for example
  `postgresql+psycopg://user:pass@host:5432/snuper`) and the destination table name via
  `--rds-table`. The sink expects the primary table to provide `id`, `provider`, `league`,
  `event_id`, `data` (JSON/JSONB), and `created_at` columns and manages a
  `<table>_snapshots` companion with `provider`, `league`, `snapshot_date`, `payload`,
  and `received_at`.
- Restrict execution with `--league nba,mlb` for targeted runs.
- Use `--overwrite` to replace existing daily snapshots during a rescrape.
- DraftKings monitors honor `--interval`; other providers pace themselves.

Examples:

```sh
$ poetry run snuper --task scrape --fs-sink-dir data
```
- Run every supported scraper for all providers and leagues, writing fresh daily snapshots.

```sh
$ poetry run snuper -p draftkings,betmgm --task monitor --fs-sink-dir data
```
- Start DraftKings and BetMGM monitors using their latest snapshots and default pacing.

```sh
$ poetry run snuper \
  --task monitor \
  --provider bovada \
  --sink rds \
  --rds-uri postgresql+psycopg://snuper:secret@db.example.com:5432/snuper \
  --rds-table selection_updates
```
- Stream Bovada odds into PostgreSQL; the sink creates `selection_updates` and
  `selection_updates_snapshots` tables on first run.

```sh
$ poetry run snuper \
  --task monitor \
  --provider draftkings \
  --interval 45 \
  --fs-sink-dir data
```
- Monitor DraftKings only, overriding the websocket refresh cadence to 45 seconds.

## Coverage

| League | DraftKings | BetMGM | Bovada | FanDuel   |
| --- | --- | --- | --- |-----------|
| NBA | ✅ | ✅ | ✅ | ❌         |
| NFL | ✅ | ✅ | ✅ | ❌ |
| MLB | ✅ | ✅ | ✅ | ❌ |

## Workflows

### `scrape`

The `scrape` workflow launches provider-specific collectors that enumerate the
day’s playable events, normalize metadata (teams, start times, selections), and
write snapshots to `<fs_sink_dir>/<provider>/events/YYYYMMDD-<league>.json` when `--sink=fs`. When `--sink=rds` or `--sink=cache`,
the same payload is persisted to the selected backend instead (see Output),
allowing monitors to bootstrap without local files. Each run emits INFO logs
describing how many events are queued for persistence and subsequently saved.
Snapshots are timestamped and never overwritten; reruns append a new record for
comparison.

### `monitor`

The `monitor` workflows read the latest `scrape` snapshot for each provider and league, reuse
the stored selection IDs, and stream live odds into JSONL files under
`<fs_sink_dir>/<provider>/odds/`. Runners emit heartbeat entries when the
feed is idle so that quiet games remain traceable. When `--sink=rds` is supplied,
the same deltas are persisted into the configured table (tagged with the
provider) and snapshots are copied to the `<table>_snapshots` companion for
replaying historical states.

> Note that the usage of JSON files on disk is a local development feature.

## Providers

- **DraftKings** 
  - Uses Playwright to enumerate event URLs, persists the spread
    selections, and connects to a MsgPack websocket stream. The optional
    `--interval` flag controls how often the monitor refreshes connection state.
- **BetMGM**
  - Scrapes its league pages with Playwright, derives team metadata
    from URLs, and polls the public CDS API on a tight cadence. Odds updates are
    emitted via DOM snapshots that BaseRunner throttles with heartbeat intervals.
- **Bovada**
  - Currently fetches events through HTTP coupon endpoints and ingests
    live odds via the Bovada websocket. Team filters reuse BetMGM slug helpers to
    keep league detection consistent.
- **FanDuel**
  - Scraping is scaffolded with Playwright discovery, but selection
    flattening and monitoring are not yet implemented. Scrape runs succeed with
    placeholder selections; monitor runs log an informational skip.

## Output

Scrape and monitor operations share a sink interface that persists snapshots
and odds deltas. Each sink stores and reloads data a little differently.

### Filesystem sink (`--sink=fs`)

- `scrape` writes snapshot JSON files to `<fs_sink_dir>/<provider>/events/YYYYMMDD-<league>.json`. Existing files are skipped unless `--overwrite` is
  supplied.
- `monitor` appends newline-delimited JSON records to `<fs_sink_dir>/<provider>/odds/YYYYMMDD-<league>-<event_id>.json`, capturing each selection change in order.
- `load_snapshots` rehydrates events by reading the snapshot JSON under the
  provider's `events` directory.

### RDS sink (`--sink=rds`)

- `scrape` inserts one row per event into the primary `--rds-table`, filling
  the `provider`, `league`, `event_id`, `data`, and `created_at` columns and
  logging the batch size. The event snapshot is also written to
  `<table>_snapshots` for historical replay.
- `monitor` inserts each odds delta into the same primary table with the
  provider annotated and the raw/normalized payload stored in `data`.
- `load_snapshots` fetches the most recent snapshot per league from
  `<table>_snapshots` (respecting any `--league` filter) before runners
  reconnect.

### Cache sink (`--sink=cache`)

- `scrape` writes the snapshot payload to Redis keys of the form
  `snuper:snapshots:<provider>:<league>` and tracks the available leagues in
  `snuper:snapshots:<provider>:leagues`, applying the configured TTL to both.
- `monitor` pushes rolling lists of raw messages and normalized selection
  updates to `snuper:<league>:<event_id>:raw` and
  `snuper:<league>:<event_id>:selection`, trimming them to
  `--cache-max-items` while refreshing the TTL.
- `load_snapshots` reads the cached snapshot JSON for the requested leagues so
  monitors can bootstrap without disk or database access.

## Development

```sh
$ git clone https://github.com/stonehedgelabs/snuper.git
$ cd snuper
```
- Clone the repository and enter the project directory.

```sh
$ python3 -m venv .venv
$ source .venv/bin/activate
```
- Create and activate a Python 3.12 virtual environment.

```sh
$ pip install poetry
```
- Install Poetry inside the virtual environment.

```sh
$ poetry install
$ poetry run playwright install chromium
```
- Install project dependencies and fetch the Chromium binary for Playwright.

```sh
$ poetry run pytest
```
- Execute the test suite before shipping changes.

## Glossary

- **Task** – One of `scrape` or `monitor`. Tasks define whether the CLI is
  collecting schedules or streaming odds.
- **Scrape** – A task that navigates provider frontends or APIs to discover the
  upcoming schedule, captures team metadata, and stores selections for later
  monitoring.
- **Monitor** – A task that reuses stored selections to ingest live odds via
  websockets or polling loops, emitting JSONL records with heartbeats for idle
  games.
- **Provider** – A sportsbook integration (`draftkings`, `betmgm`, `bovada`,
  `fanduel`). Providers expose both scrape and monitor entry points when
  implemented.
- **Sink** – Destination backend for selection snapshots and odds updates. Choose via
  `--sink` (`fs`, `rds`, or `cache`) and pair it with the appropriate connection flags.
- **Filesystem Sink Directory (`fs_sink_dir`)** – Root path used by the filesystem sink for
  snapshots and odds logs when `--sink=fs`.
- **Interval** – Optional CLI pacing for DraftKings monitoring; other providers
  manage loop timing internally (e.g., BetMGM reloads every second).
- **Selection** – A single wager option returned by a provider (for example, a
  team’s spread or moneyline). Snapshots record selections so monitors can
  resubscribe accurately.
- **Odds** – Price information attached to each selection. Providers return
  American odds (e.g., +150 / -110) and often include decimal odds for
  comparison.
- **Snapshot** – A timestamped JSON document containing all events for a league
  as of the scrape run. Stored under the provider’s `events` directory and never
  overwritten.
- **Heartbeat** – A periodic log entry emitted by monitors to confirm that the
  connection remains active even when no odds change is detected.
