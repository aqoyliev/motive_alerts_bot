import aiohttp
import base64
import json
import logging
import urllib.parse

logger = logging.getLogger(__name__)

MOTIVE_API = "https://api.gomotive.com/v1"


def extract_event_id(mandrill_url: str) -> str | None:
    """Decode a Mandrill tracking URL and return the GoMotive event ID."""
    try:
        parsed = urllib.parse.urlparse(mandrill_url)
        p = urllib.parse.parse_qs(parsed.query).get("p", [None])[0]
        if not p:
            return None
        outer = json.loads(base64.b64decode(p + "==").decode())
        inner = json.loads(outer["p"])
        url = inner.get("url", "")
        parts = url.rstrip("/").split("/")
        return parts[-1] if parts[-1].isdigit() else None
    except Exception as e:
        logger.warning(f"Could not extract event ID from URL: {e}")
        return None


class MotiveClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._headers = {"X-Api-Key": api_key, "Accept": "application/json"}

    async def get_event_video_url(self, event_id: str) -> str | None:
        """Fetch safety event by ID and return video clip URL if available."""
        try:
            async with aiohttp.ClientSession(headers=self._headers) as s:
                async with s.get(f"{MOTIVE_API}/safety_events/{event_id}") as r:
                    if r.status != 200:
                        logger.warning(f"Safety event {event_id} returned HTTP {r.status}")
                        return None
                    data = await r.json()
                    event = data.get("safety_event", data)
                    clip = event.get("video_clip") or {}
                    url = clip.get("url") or clip.get("download_url")
                    if url:
                        logger.info(f"Video clip found for event {event_id}")
                    return url
        except Exception as e:
            logger.error(f"get_event_video_url error: {e}")
            return None

    async def download_video(self, video_url: str) -> bytes | None:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(video_url) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    logger.error(f"Video download failed: HTTP {resp.status}")
                    return None
        except Exception as e:
            logger.error(f"Video download error: {e}")
            return None
