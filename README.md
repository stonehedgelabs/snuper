# dk_event_monitor

Scrapes Draftkings endpoints for sporting event data, and uses that scraped data to extract live odds information for each game, from Draftkings websockets.

## Usage

### Scrape mode

Scrape today's scheduled games/events for a given league and save that info to a specified location.

```text
$ python main.py scrape --help            
usage: main.py scrape [-h] -o OUTPUT_DIR

options:
  -h, --help            show this help message and exit
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Output directory for the JSON file
```

#### Usage

```sh
python dk.py scrape --league nba --output-dir data/
```


### Monitor mode

Use previously scraped games/events, and extract odds information for each game using the Draftkings websocket.

```text
$ python main.py monitor --help
usage: main.py monitor [-h] --input-dir INPUT_DIR [--interval INTERVAL]

options:
  -h, --help            show this help message and exit
  --input-dir INPUT_DIR
                        Directory containing event JSON files
  --interval INTERVAL   Refresh interval in seconds

```

#### Usage

```sh
python dk.py monitor --input-dir data/ --interval 60
```