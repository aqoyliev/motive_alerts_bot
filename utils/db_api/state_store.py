import json
import os
from datetime import datetime, timezone

STATE_FILE = os.path.join(os.path.dirname(__file__), "../../data/state.json")


def _load() -> dict:
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}


def _save(data: dict):
    os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
    with open(STATE_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_last_fetch_time() -> str:
    """Return ISO timestamp of last successful fetch, or 1 hour ago if not set."""
    data = _load()
    if "last_fetch_time" in data:
        return data["last_fetch_time"]
    # Default: start from 1 hour ago
    from datetime import timedelta
    default = datetime.now(timezone.utc) - timedelta(hours=1)
    return default.strftime("%Y-%m-%dT%H:%M:%SZ")


def set_last_fetch_time(ts: str):
    data = _load()
    data["last_fetch_time"] = ts
    _save(data)


def get_seen_event_ids() -> set:
    data = _load()
    return set(data.get("seen_event_ids", []))


def add_seen_event_ids(ids: list):
    data = _load()
    existing = set(data.get("seen_event_ids", []))
    existing.update(ids)
    # Keep only the last 1000 IDs to prevent unbounded growth
    data["seen_event_ids"] = list(existing)[-1000:]
    _save(data)
