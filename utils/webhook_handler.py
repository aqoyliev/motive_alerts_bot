import asyncio
import json
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import aiohttp
from aiohttp import web
from aiogram import Bot
from aiogram.types import InputMediaVideo, InputMediaPhoto
from aiogram.utils.exceptions import NetworkError

from data import config
from utils.db_api.companies import get_groups_for_event, get_company_name
from utils.db_api.violations import save_violation
from utils.db_api.admins import get_subscribed_admins

logger = logging.getLogger(__name__)


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


def _format_event(event: dict, company_name: str = "") -> str:
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
    if company_name:
        lines.append(company_name)
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


async def _handle_event(bot: Bot, event: dict, company_slug: str = "gurman"):
    """Filter → format → send to Telegram (URLs sent directly, no download)."""
    try:
        event_type = _get_event_type(event)
        event_id = event.get("id", "?")

        if event_type not in ALLOWED_TYPES:
            logger.info(f"Ignored event type='{event_type}' id={event_id}")
            return

        if event_type == "speeding":
            meta_sev = ((event.get("metadata") or {}).get("severity") or "").strip().lower()
            sev = meta_sev or (event.get("severity") or "").strip().lower()
            if sev and sev not in {"critical", "high"}:
                logger.info(f"Ignored speeding event {event_id} severity='{sev}' (below threshold)")
                return

        logger.info(f"Processing event {event_id} type={event_type}")

        # Parse occurred_at
        occurred_at_str = event.get("start_time") or event.get("created_at") or ""
        try:
            occurred_at = datetime.fromisoformat(occurred_at_str.replace("Z", "+00:00"))
        except Exception:
            occurred_at = datetime.now()

        vehicle_number = _get_vehicle(event)
        meta_sev = ((event.get("metadata") or {}).get("severity") or "").strip().lower()
        severity = meta_sev or (event.get("severity") or "").strip().lower() or None
        await save_violation(
            company_slug=company_slug,
            vehicle_number=vehicle_number,
            event_type=event_type,
            event_id=event.get("id"),
            occurred_at=occurred_at,
            severity=severity,
        )

        group_ids = await get_groups_for_event(company_slug, event_type)
        if not group_ids:
            logger.info(f"No groups configured for company='{company_slug}' event='{event_type}' — skipping")
            return

        company_display = await get_company_name(company_slug) or company_slug.title()
        text = _format_event(event, company_display)
        video_urls, image_urls = _get_camera_media_info(event)

        if event.get("camera_media") is None and event_type != "speeding":
            text += "\n\n📷 <i>No camera media available</i>"

        for chat_id in group_ids:
            await _send_with_retry(bot, chat_id, text, video_urls, image_urls)

        dm_ids = await get_subscribed_admins(event_type, company_slug)
        for telegram_id in dm_ids:
            await _send_with_retry(bot, telegram_id, text, video_urls, image_urls)

    except Exception as e:
        logger.error(f"Event handling error: {e}", exc_info=True)


async def _send_with_retry(bot: Bot, chat_id: int, text: str, video_urls: list[str], image_urls: list[str],
                            retries: int = 3, delay: float = 5.0):
    """Send alert to a single chat using direct URLs, retrying on transient errors."""
    for attempt in range(1, retries + 1):
        try:
            if video_urls:
                if len(video_urls) == 1:
                    await bot.send_video(
                        chat_id, video_urls[0],
                        caption=text, parse_mode="HTML",
                    )
                else:
                    media = [
                        InputMediaVideo(video_urls[0], caption=text, parse_mode="HTML")
                    ] + [
                        InputMediaVideo(u) for u in video_urls[1:]
                    ]
                    await bot.send_media_group(chat_id, media)
            elif image_urls:
                if len(image_urls) == 1:
                    await bot.send_photo(
                        chat_id, image_urls[0],
                        caption=text, parse_mode="HTML",
                    )
                else:
                    media = [
                        InputMediaPhoto(image_urls[0], caption=text, parse_mode="HTML")
                    ] + [
                        InputMediaPhoto(u) for u in image_urls[1:]
                    ]
                    await bot.send_media_group(chat_id, media)
            else:
                await bot.send_message(chat_id, text, parse_mode="HTML")
            return  # success
        except NetworkError as e:
            if attempt < retries:
                logger.warning(f"NetworkError sending to {chat_id} (attempt {attempt}/{retries}): {e} — retrying in {delay}s")
                await asyncio.sleep(delay)
            else:
                logger.error(f"NetworkError sending to {chat_id} after {retries} attempts: {e}")


async def motive_webhook(request: web.Request) -> web.Response:
    """Receive Motive webhook POST, respond 200 immediately, process async."""
    try:
        company_slug = request.match_info.get("company", "gurman")
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
            event_type = _get_event_type(event)
            if event_type not in ALLOWED_TYPES:
                logger.debug(f"Unhandled event type='{event_type}' keys={list(event.keys())} payload={json.dumps(event, default=str)[:500]}")
            asyncio.create_task(_handle_event(bot, event, company_slug))

        return web.Response(text="OK", status=200)
    except Exception as e:
        logger.error(f"Webhook parse error: {e}")
        return web.Response(text="Error", status=500)


async def start_webhook_server(bot: Bot, port: int = 8080):
    app = web.Application()
    app["bot"] = bot
    app.router.add_post("/webhook/{company}", motive_webhook)
    app.router.add_get("/health", lambda r: web.Response(text="OK"))

    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Motive webhook server listening on port {port}")
