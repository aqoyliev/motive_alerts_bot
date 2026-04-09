import asyncio
import io
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from aiohttp import web
from aiogram import Bot
from aiogram.types import InputFile, InputMediaVideo, InputMediaPhoto

from data import config
from utils.motive import MotiveClient

logger = logging.getLogger(__name__)

motive_client = MotiveClient(config.MOTIVE_API_KEY)

SEVERITY_EMOJI = {
    "low": "🟢",
    "medium": "🟡",
    "high": "🔴",
    "critical": "🆘",
}

# Exact Motive event type names → (emoji, display title)
EVENT_TYPE_MAP = {
    "hard_brake":                   ("🛑", "HARD BRAKE"),
    "crash":                        ("💥", "CRASH DETECTED"),
    "cell_phone":                   ("📵", "CELL PHONE USAGE"),
    "stop_sign_violation":          ("🛑", "STOP SIGN VIOLATION"),
    "road_facing_cam_obstruction":  ("📷", "ROAD CAMERA OBSTRUCTED"),
    "driver_facing_cam_obstruction":("📷", "DRIVER CAMERA OBSTRUCTED"),
    "forward_collision_warning":    ("⚠️", "FORWARD COLLISION WARNING"),
    "unsafe_parking":               ("🅿️", "UNSAFE PARKING"),
    "speeding":                     ("🚨", "SPEEDING OVER POSTED"),
}

# Only process these event types — everything else is ignored
ALLOWED_TYPES = set(EVENT_TYPE_MAP.keys())

# Motive speeding webhook uses "action" field with these values
SPEEDING_ACTIONS = {"speeding_event_created", "speeding_event_updated"}


def _kph_to_mph(kph: float) -> float:
    return kph * 0.621371


def _to_et(utc_iso: str) -> str:
    try:
        dt = datetime.fromisoformat(utc_iso.replace("Z", "+00:00"))
        return dt.astimezone(ZoneInfo("America/New_York")).strftime("%b %d, %I:%M %p %Z")
    except Exception:
        return utc_iso


def _get_event_type(event: dict) -> str:
    """Determine canonical event type from action or type field."""
    action = (event.get("action") or "").lower()
    if action in SPEEDING_ACTIONS:
        return "speeding"
    return (event.get("type") or "").lower()


def _get_vehicle(event: dict) -> str:
    """Extract vehicle number from any event payload structure."""
    # current_vehicle present in both speeding (capital keys) and driver_performance (lowercase)
    current = event.get("current_vehicle") or {}
    if current.get("Number") or current.get("number"):
        return current.get("Number") or current.get("number")

    vehicle_info = event.get("vehicle") or {}
    return (
        vehicle_info.get("number")
        or vehicle_info.get("truck_number")
        or event.get("vehicle_number")
        or event.get("truck_number")
        or str(event.get("vehicle_id") or "")
        or "—"
    )


def _format_event(event: dict) -> str:
    event_type = _get_event_type(event)
    emoji, title = EVENT_TYPE_MAP.get(event_type, ("🚨", event_type.upper().replace("_", " ")))

    vehicle = _get_vehicle(event)
    driver_info = event.get("driver") or {}
    driver = driver_info.get("name") or driver_info.get("username") or "Unidentified"

    start_time = _to_et(event.get("start_time", ""))
    location = event.get("location", "")
    intensity = event.get("intensity", "")
    duration = event.get("duration")

    # Severity: prefer metadata.severity, then direct severity field; ignore coaching_status
    meta_sev = ((event.get("metadata") or {}).get("severity") or "").strip()
    sev_display = meta_sev or (event.get("severity") or "").strip()

    lines = [f"{emoji} <b>{title}</b>\n"]
    lines.append(f"🚛 <b>Vehicle:</b> <code>{vehicle}</code>")
    lines.append(f"👤 <b>Driver:</b> {driver}")
    if sev_display and event_type not in {"driver_facing_cam_obstruction", "road_facing_cam_obstruction"}:
        sev_emoji = SEVERITY_EMOJI.get(sev_display.lower(), "⚠️")
        lines.append(f"📊 <b>Severity:</b> {sev_emoji} {sev_display.title()}")
    lines.append(f"🕐 <b>Time:</b> {start_time}")

    if event_type == "speeding":
        avg = event.get("avg_vehicle_speed")
        limit = event.get("min_posted_speed_limit_in_kph")
        over = event.get("max_over_speed_in_kph")
        if avg:
            lines.append(f"💨 <b>Average Speed:</b> {_kph_to_mph(avg):.1f} mph")
        if limit:
            lines.append(f"🚦 <b>Speed Limit:</b> {_kph_to_mph(limit):.1f} mph")
        if over:
            lines.append(f"📈 <b>Max Over Posted:</b> {_kph_to_mph(over):.1f} mph")
        if duration:
            lines.append(f"⏱ <b>Duration:</b> {duration}s")
        nominatim = event.get("nominatim_location", "")
        if nominatim:
            lines.append(f"📍 <b>Location:</b> {nominatim}")
    else:
        if location:
            lines.append(f"📍 <b>Location:</b> {location}")
        if event_type == "hard_brake" and intensity:
            lines.append(f"💥 <b>Intensity:</b> {intensity}")
        if duration:
            lines.append(f"⏱ <b>Duration:</b> {duration}s")

    return "\n".join(lines)


def _get_camera_media_info(event: dict) -> tuple[list[str], list[str]]:
    """Returns (video_urls, image_urls) from camera_media."""
    camera_media = event.get("camera_media") or {}
    if not camera_media.get("available"):
        return [], []
    dl = camera_media.get("downloadable_videos") or {}
    video_urls = [u for u in [dl.get("front_facing_plain_url"), dl.get("driver_facing_plain_url")] if u]
    imgs = camera_media.get("downloadable_images") or {}
    image_urls = [u for u in [imgs.get("front_facing_jpg_url"), imgs.get("driver_facing_jpg_url")] if u]
    return video_urls, image_urls


async def _handle_event(bot: Bot, event: dict):
    """Filter → format → fetch video → send to Telegram."""
    try:
        event_type = _get_event_type(event)
        event_id = event.get("id", "?")

        if event_type not in ALLOWED_TYPES:
            logger.info(f"Ignored event type='{event_type}' id={event_id}")
            return

        logger.info(f"Processing event {event_id} type={event_type}")

        text = _format_event(event)
        vehicle = _get_vehicle(event)
        start_time = event.get("start_time", "")

        video_urls, image_urls = _get_camera_media_info(event)

        if event.get("camera_media") is None and event_type != "speeding":
            text += "\n\n📷 <i>No camera media available</i>"

        videos = []
        for url in video_urls:
            data = await motive_client.download_video(url)
            if data:
                videos.append(data)

        if videos:
            if len(videos) == 1:
                await bot.send_video(
                    config.GROUP_CHAT_ID,
                    InputFile(io.BytesIO(videos[0]), filename="alert.mp4"),
                    caption=text,
                    parse_mode="HTML",
                )
            else:
                media = [
                    InputMediaVideo(
                        InputFile(io.BytesIO(videos[0]), filename="video_1.mp4"),
                        caption=text,
                        parse_mode="HTML",
                    )
                ] + [
                    InputMediaVideo(InputFile(io.BytesIO(v), filename=f"video_{i+2}.mp4"))
                    for i, v in enumerate(videos[1:])
                ]
                await bot.send_media_group(config.GROUP_CHAT_ID, media)
        elif image_urls:
            # Videos not transcoded yet — send photos instead
            images = []
            for url in image_urls:
                data = await motive_client.download_video(url)  # reuse downloader
                if data:
                    images.append(data)
            if len(images) == 1:
                await bot.send_photo(
                    config.GROUP_CHAT_ID,
                    InputFile(io.BytesIO(images[0]), filename="alert.jpg"),
                    caption=text,
                    parse_mode="HTML",
                )
            elif images:
                media = [
                    InputMediaPhoto(
                        InputFile(io.BytesIO(images[0]), filename="photo_1.jpg"),
                        caption=text,
                        parse_mode="HTML",
                    )
                ] + [
                    InputMediaPhoto(InputFile(io.BytesIO(img), filename=f"photo_{i+2}.jpg"))
                    for i, img in enumerate(images[1:])
                ]
                await bot.send_media_group(config.GROUP_CHAT_ID, media)
            else:
                await bot.send_message(config.GROUP_CHAT_ID, text, parse_mode="HTML")
        else:
            await bot.send_message(config.GROUP_CHAT_ID, text, parse_mode="HTML")

    except Exception as e:
        logger.error(f"Event handling error: {e}", exc_info=True)


async def motive_webhook(request: web.Request) -> web.Response:
    """Receive Motive webhook POST, respond 200 immediately, process async."""
    try:
        body = await request.json()

        bot: Bot = request.app["bot"]

        # Verification ping — list of event type strings
        if isinstance(body, list) and all(isinstance(i, str) for i in body):
            logger.info(f"Webhook verification ping: {body}")
            return web.Response(text="OK", status=200)

        items = body if isinstance(body, list) else [body]

        for item in items:
            if not isinstance(item, dict):
                continue
            # Unwrap if Motive wrapped the event in a key
            event = (
                item.get("driver_performance_event")
                or item.get("safety_event")
                or item.get("event")
                or item
            )
            # Log unrecognized top-level keys to help map payload structures
            event_type = _get_event_type(event)
            if event_type not in ALLOWED_TYPES:
                logger.info(f"Unhandled event type='{event_type}' keys={list(event.keys())} payload={json.dumps(event, default=str)[:500]}")
            asyncio.create_task(_handle_event(bot, event))

        return web.Response(text="OK", status=200)
    except Exception as e:
        logger.error(f"Webhook parse error: {e}")
        return web.Response(text="Error", status=500)


async def start_webhook_server(bot: Bot, port: int = 8080):
    app = web.Application()
    app["bot"] = bot
    app.router.add_post("/webhook/{company}", motive_webhook)
    app.router.add_get("/health", lambda r: web.Response(text="OK"))

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Motive webhook server listening on port {port}")
