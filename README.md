# event_monitor

Asynchronous tooling for collecting sportsbook events, persisting snapshots, and
streaming live odds updates. Each processor contributes a `scrape` command that
captures the day’s schedule and a `monitor` command that streams odds into
timestamped JSONL logs.

## Usage

```text
usage: main.py [-h] [-p {betmgm,draftkings,fanduel}] -t {scrape,monitor}
               [-o OUTPUT_DIR] [-i INTERVAL]

Unified Event Monitor CLI

options:
  -h, --help            show this help message and exit
  -p {betmgm,draftkings,fanduel}, --provider {betmgm,draftkings,fanduel}
                        Target sportsbook provider (omit to run all)
  -t {scrape,monitor}, -task {scrape,monitor}, --task {scrape,monitor}
                        Operation to perform
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Base directory where provider artifacts are written
  -i INTERVAL, --interval INTERVAL
                        Refresh interval in seconds (DraftKings monitor only)
```

Skip `--provider` to execute every processor in sequence. The CLI accepts both
short aliases (`dk`, `fd`) and full names (`draftkings`, `betmgm`, `fanduel`).

## DraftKings Processor (Websocket)

- `python main.py --provider draftkings --task scrape --output-dir data`
  — launches Playwright, enumerates event URLs, normalises spread markets, and
  persists `nba-YYYYMMDD.json` snapshots.
- `python main.py --provider draftkings --task monitor --output-dir data --interval 30`
  — consumes the websocket feed using the selections saved in the snapshot.
  Heartbeats are emitted even when a game is idle so every in-flight event logs
  progress (no more silent NBA sessions).
- Snapshots live under `data/draftkings/events/`, odds streams land in
`data/draftkings/events/odds/`. Each reconnect reuses the subscription
  payload and the base runner ensures only one websocket is opened per game.

## MGM Processor (Playwright Polling)

- `python main.py --provider betmgm --task scrape --output-dir data`
  — navigates the BetMGM league pages, filters to today’s games, and stores
  their metadata.
- `python main.py --provider betmgm --task monitor --output-dir data`
  — reloads each event page on a short cadence and flattens
  spread/total/moneyline rows into JSONL records.
  The shared runner logic emits a heartbeat once per minute so you can see NBA
  and NFL games even when the DOM hasn’t changed.
- Odds logs are appended beneath the same directory structure as DraftKings to
  keep comparisons simple.

## FanDuel Processor (WIP)

- `python main.py --provider fanduel --task scrape --output-dir data`
  — mirrors the scraper API but market normalisation and live polling are still
  stubbed out.
- `python main.py --provider fanduel --task monitor --output-dir data`
  — currently raises `NotImplementedError` until live polling is implemented. Once
  market responses are recorded, it can be wired into the shared runner without
  impacting downstream consumers.

## Shared Architecture

- `event_monitor/scraper.py` houses the `BaseEventScraper` with consistent
  persistence helpers (`event_filepath`, `odds_filepath`).
- `event_monitor/runner.py` contains `BaseRunner` (including heartbeat support)
  and `BaseMonitor` which ensures only one runner is started per live event.
- Constants such as ANSI colours, heartbeat intervals, and league URLs live in
  `event_monitor/constants.py` so processors share the same tuning knobs.

## Development Notes

- Run `poetry install` followed by `poetry run playwright install chromium`
  before scraping.
- Snapshot JSON files are immutable—new runs create new timestamped artefacts
  rather than overwriting existing data.
- Heartbeat logs are now produced for every service via the shared base runner,
  so missing output usually means the event hasn’t started rather than a stalled
  connection.
