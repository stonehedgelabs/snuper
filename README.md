# dk_event_monitor

Asynchronous tooling for collecting sportsbook events, persisting snapshots, and
streaming live odds updates. Each processor contributes a `scrape` command that
captures the day’s schedule and a `monitor` command that streams odds into
timestamped JSONL logs.

## DraftKings Processor (Websocket)

- `python dk.py scrape --league nba --output-dir data/draftkings/events` —
  launches Playwright, enumerates event URLs, normalises spread markets, and
  persists `nba-YYYYMMDD.json` snapshots.
- `python dk.py monitor --input-dir data/draftkings/events --interval 30`
  consumes the websocket feed using the selections saved in the snapshot.
  Heartbeats are emitted even when a game is idle so every in-flight event logs
  progress (no more silent NBA sessions).
- Snapshots live under `data/draftkings/events/`, odds streams land in
  `data/draftkings/events/odds/`. Each reconnect reuses the subscription
  payload and the base runner ensures only one websocket is opened per game.

## MGM Processor (Playwright Polling)

- `python mgm.py scrape --league nba --output-dir data/mgm/events` navigates the
  BetMGM league pages, filters to today’s games, and stores their metadata.
- `python mgm.py monitor --input-dir data/mgm/events` reloads each event page on
  a short cadence and flattens spread/total/moneyline rows into JSONL records.
  The shared runner logic emits a heartbeat once per minute so you can see NBA
  and NFL games even when the DOM hasn’t changed.
- Odds logs are appended beneath the same directory structure as DraftKings to
  keep comparisons simple.

## FanDuel Processor (WIP)

- `fd.py` mirrors the scraper API but market normalisation and live polling are
  still stubbed out. Once the API responses are recorded, it can be wired into
  the same runner infrastructure without altering downstream consumers.

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
