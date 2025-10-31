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
usage: main.py [-h] [-p PROVIDERS] -t {scrape,monitor} [-l LEAGUES] \
               -o OUTPUT_DIR [-i INTERVAL] [--overwrite]

Unified Event Monitor CLI

options:
  -h, --help            show this help message and exit
  -p PROVIDERS, --provider PROVIDERS
                        Comma-separated sportsbook providers (omit to run all)
  -t {scrape,monitor}, -task {scrape,monitor}, --task {scrape,monitor}
                        Operation to perform
  -l LEAGUES, --league LEAGUES
                        Comma-separated leagues to limit (nba,nfl,mlb)
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Base directory where provider artifacts are written
  -i INTERVAL, --interval INTERVAL
                        Refresh interval in seconds (DraftKings monitor only)
  --overwrite           Replace existing snapshots instead of skipping
```

- Providers must be supplied using their full names (e.g., `draftkings`,
  `betmgm`, `bovada`, `fanduel`). Omit `--provider` to run every available
  scraper or monitor.
- `--output-dir` is required for all tasks; the path is resolved relative to the
  current working directory.
- Restrict execution with `--league nba,mlb` for targeted runs.
- Use `--overwrite` to replace existing daily snapshots during a rescrape.
- DraftKings monitors honor `--interval`; other providers pace themselves.

Examples:

```sh
$ poetry run python main.py --task scrape --output-dir data
```
- Run every supported scraper for all providers and leagues, writing fresh daily snapshots.

```sh
$ poetry run python main.py -p draftkings,betmgm --task monitor --output-dir data
```
- Start DraftKings and BetMGM monitors using their latest snapshots and default pacing.

```sh
$ poetry run python main.py \
  --task monitor \
  --provider draftkings \
  --interval 45 \
  --output-dir data
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
write snapshots to `output_dir/<provider>/events/<league>/<league>-YYYYMMDD.json`.
Snapshots are timestamped and never overwritten; reruns append a new file for
comparison.

### `monitor`

The `monitor` workflows read the latest `scrape` snapshot for each provider and league, reuse
the stored selection IDs, and stream live odds into JSONL files under
`output_dir/<provider>/events/odds/`. Runners emit heartbeat entries when the
feed is idle so that quiet games remain traceable.

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

- `scrape` snapshots are immutable JSON files that record the state of each event at
  scrape time.
- `monitor` snapshot streams are newline-delimited JSON (JSONL) files keyed by the associated
  snapshot timestamp and event identifier.
- The current sink is the local filesystem beneath `data/`. Future work will
  route artifacts to other sinks such as PostgreSQL, Redis, or cloud object
  storage without changing the CLI contract.

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
- **Output Directory (`output_dir`)** – The root sink supplied via `--output-dir`.
  All snapshots and odds logs are written under this path. Designed to be
  swapped for other sink types in future work.
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