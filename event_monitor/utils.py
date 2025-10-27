import pathlib
import json
import datetime as dt

from event_monitor.t import Event


def decimal_to_american(decimal_odds: str) -> str:
    """Convert decimal odds (e.g. '1.80') to American format ('-125')."""
    try:
        d = float(decimal_odds)
        if d >= 2.0:
            return f"+{int((d - 1) * 100):d}"
        else:
            return f"-{int(100 / (d - 1)):d}"
    except Exception:
        return None


def load_events(file_path: pathlib.Path) -> tuple[str, list[Event]]:
    """Read a JSON snapshot and return its league plus Event objects."""
    league = file_path.stem.split("-")[0]
    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    events = []
    for e in data:
        start_time = dt.datetime.fromisoformat(e["start_time"])
        away = tuple(e["away"])
        home = tuple(e["home"])
        events.append(
            Event(
                e["event_id"],
                e["league"],
                e["event_url"],
                start_time,
                away,
                home,
                e["selections"],
            )
        )
    return league, events
