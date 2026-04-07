import logging
import asyncio
from datetime import datetime, timedelta
import aiohttp

logger = logging.getLogger(__name__)

MOTIVE_API = "https://api.gomotive.com/v1"


class MotiveClient:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self._headers = {"X-Api-Key": api_key, "Content-Type": "application/json"}

    async def request_video_recall(
        self, vehicle_number: str, start_time_utc: str, duration_minutes: int = 1
    ) -> list[str]:
        """Creates a video recall request and polls until complete. Returns download URLs."""
        try:
            dt = datetime.fromisoformat(start_time_utc.replace("Z", "+00:00"))
            end_dt = dt + timedelta(minutes=duration_minutes)
            payload = {
                "vehicle_number": vehicle_number,
                "start_time": dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_time": end_dt.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "duration": duration_minutes,
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"{MOTIVE_API}/video_recall_requests",
                    headers=self._headers,
                    json=payload,
                ) as r:
                    if r.status != 201:
                        text = await r.text()
                        logger.error(f"Video recall create failed {r.status}: {text}")
                        return []
                    data = await r.json()
                    job_id = data["id"]
                    logger.info(f"Video recall job {job_id} created for vehicle {vehicle_number}")

                # Poll up to 90s
                for attempt in range(18):
                    await asyncio.sleep(5)
                    async with s.get(
                        f"{MOTIVE_API}/video_recall_requests/{job_id}",
                        headers=self._headers,
                    ) as r:
                        data = await r.json()
                        status = data.get("status")
                        cameras = data.get("cameras", [])
                        logger.info(f"[{(attempt+1)*5}s] job {job_id}: {status}, {len(cameras)} camera(s)")
                        if status == "success":
                            urls = [c["download_url"] for c in cameras if c.get("download_url")]
                            logger.info(f"Video recall done: {len(urls)} URL(s)")
                            return urls
                        if status in ("failed", "error"):
                            logger.error(f"Video recall failed: {data}")
                            return []

            logger.warning(f"Video recall job {job_id} timed out after 90s")
            return []
        except Exception as e:
            logger.error(f"Video recall error: {e}")
            return []

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
