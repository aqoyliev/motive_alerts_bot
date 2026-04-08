import aiohttp
from datetime import datetime, timezone
from data import config

BASE_URL = "https://api.gomotive.com/v1"


async def fetch_safety_events(start_time: str, end_time: str = None, per_page: int = 50) -> list:
    """
    Fetch safety events from GoMotive API.
    start_time / end_time: ISO 8601 strings, e.g. "2024-01-01T00:00:00Z"
    Returns a flat list of safety event dicts.
    """
    if end_time is None:
        end_time = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    headers = {
        "X-Api-Key": config.MOTIVE_API_KEY,
        "Accept": "application/json",
    }

    params = {
        "start_time": start_time,
        "end_time": end_time,
        "per_page": per_page,
        "page_no": 1,
    }

    all_events = []

    async with aiohttp.ClientSession(headers=headers) as session:
        while True:
            async with session.get(f"{BASE_URL}/safety_events", params=params) as resp:
                if resp.status != 200:
                    text = await resp.text()
                    raise Exception(f"GoMotive API error {resp.status}: {text}")

                data = await resp.json()
                events = data.get("safety_events", [])
                all_events.extend(events)

                pagination = data.get("pagination", {})
                total_pages = pagination.get("total", 1)
                current_page = params["page_no"]

                if current_page >= total_pages or not events:
                    break

                params["page_no"] += 1

    return all_events
