"""
Motive webhook receiver.
Listens on port 8080 for POST /webhook from Motive, formats and sends to Telegram.
"""
import logging
import json
from aiohttp import web
from aiogram import Bot

from data import config

logger = logging.getLogger(__name__)

SEVERITY_EMOJI = {
    "low":      "🟢",
    "medium":   "🟡",
    "high":     "🔴",
    "critical": "🆘",
}

EVENT_TYPE_LABELS = {
    "speeding":             ("🚨", "SPEEDING OVER POSTED"),
    "hard_brake":           ("🛑", "HARD BRAKE"),
    "hard_braking":         ("🛑", "HARD BRAKE"),
    "near_collision":       ("⚠️", "NEAR COLLISION"),
    "collision":            ("💥", "COLLISION DETECTED"),
    "unsafe_parking":       ("🅿️", "UNSAFE PARKING"),
    "cell_phone":           ("📵", "CELL PHONE USAGE"),
    "stop_sign_violation":  ("🛑", "STOP SIGN VIOLATION"),
    "obstructed_camera":    ("📷", "OBSTRUCTED CAMERA"),
    "distracted_driving":   ("😴", "DISTRACTED DRIVING"),
    "roll_stability":       ("⚠️", "ROLL STABILITY EVENT"),
    "lane_departure":       ("↔️", "LANE DEPARTURE"),
    "tailgating":           ("🚗", "TAILGATING"),
}


def _sev_line(severity: str) -> str:
    if not severity:
        return ""
    emoji = SEVERITY_EMOJI.get(severity.lower(), "⚠️")
    return f"📊 <b>Severity:</b> {emoji} {severity.capitalize()}\n"


def _fmt_time(ts: str) -> str:
    """Convert ISO UTC timestamp to Eastern for display."""
    if not ts:
        return "—"
    try:
        from datetime import datetime, timezone
        from zoneinfo import ZoneInfo
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        et = dt.astimezone(ZoneInfo("America/New_York"))
        suffix = "ET"
        return et.strftime(f"%b {et.day}, %I:%M %p {suffix}")
    except Exception:
        return ts


def _vehicle_name(vehicle: dict) -> str:
    return (
        vehicle.get("number")
        or vehicle.get("name")
        or vehicle.get("unit_number")
        or "—"
    )


def _driver_name(driver: dict) -> str:
    if not driver:
        return ""
    first = driver.get("first_name", "")
    last = driver.get("last_name", "")
    name = f"{first} {last}".strip()
    return name or ""


def _location_str(location: dict) -> str:
    if not location:
        return ""
    description = location.get("description") or location.get("address") or ""
    if description:
        return description
    lat = location.get("lat") or location.get("latitude")
    lon = location.get("lon") or location.get("longitude")
    if lat and lon:
        return f"{lat:.4f}, {lon:.4f}"
    return ""


def format_webhook_event(event: dict) -> str:
    """Format a Motive safety_event dict into a Telegram HTML message."""
    event_type = (event.get("type") or event.get("event_type") or "").lower()
    emoji, label = EVENT_TYPE_LABELS.get(event_type, ("🚨", event_type.upper().replace("_", " ") or "SAFETY ALERT"))

    vehicle = event.get("vehicle") or {}
    driver  = event.get("driver") or {}
    location = event.get("location") or {}
    severity = event.get("severity") or ""

    ts = event.get("start_time") or event.get("occurred_at") or event.get("created_at") or ""
    time_str = _fmt_time(ts)

    vehicle_str  = _vehicle_name(vehicle)
    driver_str   = _driver_name(driver)
    location_str = _location_str(location)

    lines = [f"{emoji} <b>{label}</b>\n"]
    lines.append(f"🚛 <b>Vehicle:</b> {vehicle_str}")
    if driver_str:
        lines.append(f"👤 <b>Driver:</b> {driver_str}")
    if severity:
        lines.append(_sev_line(severity).rstrip())
    lines.append(f"🕐 <b>Time:</b> {time_str}")
    if location_str:
        lines.append(f"📍 <b>Location:</b> {location_str}")

    # Type-specific extras
    if event_type == "speeding":
        speed      = event.get("max_speed_mph") or event.get("speed") or ""
        speed_limit = event.get("speed_limit_mph") or event.get("speed_limit") or ""
        over        = event.get("max_over_posted_mph") or ""
        if speed:
            lines.append(f"💨 <b>Speed:</b> {speed} mph")
        if speed_limit:
            lines.append(f"🚦 <b>Speed Limit:</b> {speed_limit} mph")
        if over:
            lines.append(f"📈 <b>Max Over Posted:</b> +{over} mph")

    elif event_type in ("hard_brake", "hard_braking"):
        g_force = event.get("max_g_force") or event.get("g_force") or ""
        if g_force:
            lines.append(f"💥 <b>G-Force:</b> {g_force}g")

    elif event_type == "unsafe_parking":
        duration = event.get("duration_seconds") or ""
        if duration:
            mins = int(duration) // 60
            secs = int(duration) % 60
            lines.append(f"⏱ <b>Duration:</b> {mins}m {secs}s")

    elif event_type in ("cell_phone",):
        duration = event.get("duration_seconds") or ""
        if duration:
            lines.append(f"⏱ <b>Duration:</b> {int(duration)}s")

    return "\n".join(lines)


async def handle_webhook(request: web.Request) -> web.Response:
    """Handle incoming POST from Motive."""
    bot: Bot = request.app["bot"]

    try:
        body = await request.json()
    except Exception:
        raw = await request.text()
        logger.warning(f"Non-JSON webhook body: {raw[:200]}")
        return web.Response(status=400, text="Invalid JSON")

    logger.info(f"Webhook received: {json.dumps(body)[:300]}")

    # Motive wraps events differently depending on webhook version
    # Try common envelope shapes
    event = (
        body.get("safety_event")
        or body.get("data", {}).get("safety_event")
        or body.get("event")
        or body
    )

    if not isinstance(event, dict):
        logger.warning(f"Unexpected webhook payload shape: {body}")
        return web.Response(status=200, text="OK")

    text = format_webhook_event(event)

    try:
        await bot.send_message(config.GROUP_CHAT_ID, text, parse_mode="HTML")
        logger.info("Alert sent to Telegram.")
    except Exception as e:
        logger.error(f"Telegram send error: {e}")

    return web.Response(status=200, text="OK")


async def start_webhook_server(bot: Bot, port: int = 8080):
    """Create and start the aiohttp webhook server."""
    app = web.Application()
    app["bot"] = bot
    app.router.add_post("/", handle_webhook)
    app.router.add_post("/webhook", handle_webhook)

    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logger.info(f"Webhook server listening on port {port} — POST /webhook")
