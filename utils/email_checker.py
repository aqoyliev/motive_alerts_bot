import asyncio
import imaplib
import email
import io
import logging
import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from bs4 import BeautifulSoup
from aiogram import Bot
from aiogram.types import InputFile, InputMediaVideo

from data import config
from utils.motive import MotiveClient

logger = logging.getLogger(__name__)

IMAP_SERVER = "imap.gmail.com"
SENDER = "notifications@gomotive.com"
CHECK_INTERVAL = 60

_processed_ids: set[str] = set()  # dedup by email Message-ID


# ── Email fetching ────────────────────────────────────────────────────────────

def _fetch_unread_motive_emails() -> list[email.message.Message]:
    mail = imaplib.IMAP4_SSL(IMAP_SERVER)
    mail.login(config.GMAIL_USER, config.GMAIL_PASS)
    mail.select("inbox")
    _, data = mail.search(None, f'(UNSEEN FROM "{SENDER}")')
    msgs = []
    for msg_id in data[0].split():
        _, raw = mail.fetch(msg_id, "(RFC822)")
        msg = email.message_from_bytes(raw[0][1])
        mid = msg.get("Message-ID", "").strip()
        if mid and mid in _processed_ids:
            continue
        msgs.append(msg)
        mail.store(msg_id, "+FLAGS", "\\Seen")
        if mid:
            _processed_ids.add(mid)
    mail.logout()
    return msgs


# ── Email parsing ─────────────────────────────────────────────────────────────

def _get_html_body(msg: email.message.Message) -> str:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "text/html":
                return part.get_payload(decode=True).decode(errors="replace")
    payload = msg.get_payload(decode=True)
    return payload.decode(errors="replace") if payload else ""


def _extract_pdf(msg: email.message.Message) -> bytes | None:
    if msg.is_multipart():
        for part in msg.walk():
            if part.get_content_type() == "application/pdf":
                return part.get_payload(decode=True)
    return None


def _find_field(lines: list[str], label: str) -> str:
    for i, line in enumerate(lines):
        if re.match(rf"^{label}$", line, re.IGNORECASE):
            for j in range(i + 1, min(i + 15, len(lines))):
                if lines[j]:
                    return lines[j]
    return ""


def _find_vehicle(lines: list[str]) -> str:
    for label in [r"VEHICLE/GROUPS", r"VEHICLE GROUPS", r"VEHICLE"]:
        val = _find_field(lines, label)
        if val:
            return val
    return "—"


_TZ_MAP = {
    "UTC": timezone.utc,
    "CDT": ZoneInfo("America/Chicago"),
    "CST": ZoneInfo("America/Chicago"),
    "MDT": ZoneInfo("America/Denver"),
    "MST": ZoneInfo("America/Denver"),
    "PDT": ZoneInfo("America/Los_Angeles"),
    "PST": ZoneInfo("America/Los_Angeles"),
    "EDT": ZoneInfo("America/New_York"),
    "EST": ZoneInfo("America/New_York"),
}


def _parse_event_time(time_str: str) -> datetime | None:
    """Parse email datetime string → UTC datetime object."""
    if not time_str:
        return None
    time_str = time_str.strip()
    for label, tz in _TZ_MAP.items():
        if label in time_str:
            try:
                cleaned = time_str.replace(f" {label}", "").strip()
                year = datetime.now().year
                dt = datetime.strptime(f"{cleaned} {year}", "%b %d, %I:%M %p %Y")
                return dt.replace(tzinfo=tz).astimezone(timezone.utc)
            except Exception as e:
                logger.warning(f"Time parse failed for '{time_str}': {e}")
                return None
    return None


def _normalize_time(time_str: str) -> str:
    """Convert any timezone to Eastern for display."""
    dt_utc = _parse_event_time(time_str)
    if dt_utc:
        eastern = dt_utc.astimezone(ZoneInfo("America/New_York"))
        return eastern.strftime("%b %d, %I:%M %p ET")
    # Fallback: already Eastern or unknown
    return time_str.strip().replace(" EDT", " ET").replace(" EST", " ET") if time_str else "—"


def _extract_video_url(soup: BeautifulSoup) -> str | None:
    """Returns URL if 'Watch Now' link exists, None if video not available."""
    for a in soup.find_all("a", href=True):
        if "watch" in a.get_text(strip=True).lower():
            return a["href"]
    return None


def parse_safety_email(msg: email.message.Message) -> dict:
    html = _get_html_body(msg)
    soup = BeautifulSoup(html, "html.parser")
    lines = [l.strip() for l in soup.get_text(separator="\n").splitlines()]

    raw_time = _find_field(lines, r"DATE\s*/\s*TIME")
    dt_utc = _parse_event_time(raw_time)

    return {
        "severity":   _find_field(lines, "SEVERITY"),
        "driver":     _find_field(lines, "DRIVER"),
        "vehicle":    _find_vehicle(lines),
        "datetime":   _normalize_time(raw_time),
        "event_utc":  dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ") if dt_utc else None,
        "behaviors":  _find_field(lines, r"UNSAFE BEHAVIO(?:U)?RS"),
        "location":   _find_field(lines, "LOCATION"),
        "duration":   _find_field(lines, "DURATION"),
        "intensity":  _find_field(lines, r"INTENSITY\s*/\s*G-FORCE"),
        # Speeding fields
        "avg_speed":   _find_field(lines, "AVERAGE SPEED"),
        "max_over":    _find_field(lines, "MAX OVER POSTED"),
        "speed_limit": _find_field(lines, "SPEED LIMIT"),
        # Video
        "has_video":  _extract_video_url(soup) is not None,
        "pdf":        _extract_pdf(msg),
    }


# ── Formatting ────────────────────────────────────────────────────────────────

SEVERITY_EMOJI = {
    "low":      "🟢",
    "medium":   "🟡",
    "high":     "🔴",
    "critical": "🆘",
}


def _sev(data: dict) -> str:
    if not data["severity"]:
        return ""
    emoji = SEVERITY_EMOJI.get(data["severity"].lower(), "⚠️")
    return f"{emoji} {data['severity']}"


def _opt(label: str, value: str) -> str:
    """Return a formatted line only if value is non-empty."""
    return f"{label} {value}\n" if value else ""


def format_message(data: dict) -> str:
    beh = data["behaviors"].lower()

    if "speeding" in beh:
        return (
            f"🚨 <b>SPEEDING OVER POSTED</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            f"📍 <b>Location:</b> {data['location']}\n"
            f"💨 <b>Average Speed:</b> {data['avg_speed']}\n"
            f"🚦 <b>Speed Limit:</b> {data['speed_limit']}\n"
            f"📈 <b>Max Over Posted:</b> {data['max_over']}"
        )

    if "hard brake" in beh or "hard braking" in beh:
        return (
            f"🛑 <b>HARD BRAKE</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            + _opt("📍 <b>Location:</b>", data["location"])
            + _opt("💥 <b>Intensity:</b>", data["intensity"])
        ).rstrip()

    if "near collision" in beh:
        return (
            f"⚠️ <b>NEAR COLLISION</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            + _opt("📍 <b>Location:</b>", data["location"])
        ).rstrip()

    if "collision" in beh:
        return (
            f"💥 <b>COLLISION DETECTED</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            + _opt("📍 <b>Location:</b>", data["location"])
        ).rstrip()

    if "parking" in beh:
        return (
            f"🅿️ <b>UNSAFE PARKING</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            + _opt("⏱ <b>Duration:</b>", data["duration"])
            + _opt("⚠️ <b>Behaviors:</b>", data["behaviors"])
        ).rstrip()

    if "cell phone" in beh or "phone" in beh:
        return (
            f"📵 <b>CELL PHONE USAGE</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            + _opt("📍 <b>Location:</b>", data["location"])
            + _opt("⏱ <b>Duration:</b>", data["duration"])
        ).rstrip()

    if "stop sign" in beh:
        return (
            f"🛑 <b>STOP SIGN VIOLATION</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            + _opt("📍 <b>Location:</b>", data["location"])
        ).rstrip()

    if "obstructed" in beh or "camera" in beh:
        return (
            f"📷 <b>OBSTRUCTED CAMERA</b>\n\n"
            f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
            + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
            + f"🕐 <b>Time:</b> {data['datetime']}\n"
            + _opt("⏱ <b>Duration:</b>", data["duration"])
        ).rstrip()

    # Generic fallback
    return (
        f"🚨 <b>{data['behaviors'].upper() or 'SAFETY ALERT'}</b>\n\n"
        f"🚛 <b>Vehicle:</b> {data['vehicle']}\n"
        + (f"📊 <b>Severity:</b> {_sev(data)}\n" if data["severity"] else "")
        + f"🕐 <b>Time:</b> {data['datetime']}\n"
        + _opt("📍 <b>Location:</b>", data["location"])
        + _opt("⏱ <b>Duration:</b>", data["duration"])
    ).rstrip()


# ── Sending ───────────────────────────────────────────────────────────────────

motive_client = MotiveClient(config.MOTIVE_API_KEY)


async def _get_videos(vehicle: str, event_utc: str) -> list[bytes]:
    """Request video recall from Motive and download all camera videos."""
    try:
        logger.info(f"[video] requesting recall for vehicle={vehicle} at {event_utc}")
        video_urls = await motive_client.request_video_recall(vehicle, event_utc)
        if not video_urls:
            return []
        videos = []
        for url in video_urls:
            data = await motive_client.download_video(url)
            if data:
                videos.append(data)
        logger.info(f"[video] downloaded {len(videos)} video(s)")
        return videos
    except Exception as e:
        logger.error(f"Video fetch error: {e}")
        return []


async def process_email(bot: Bot, msg: email.message.Message):
    data = parse_safety_email(msg)
    text = format_message(data)

    # Download videos if available and we have the event timestamp
    videos: list[bytes] = []
    if data["has_video"] and data["event_utc"] and data["vehicle"] != "—":
        videos = await _get_videos(data["vehicle"], data["event_utc"])

    # Send PDF if present (crash report)
    if data["pdf"]:
        pdf_file = InputFile(io.BytesIO(data["pdf"]), filename="collision_report.pdf")
        await bot.send_document(config.GROUP_CHAT_ID, pdf_file, caption="📄 Collision Report")

    if videos:
        if len(videos) == 1:
            await bot.send_video(
                config.GROUP_CHAT_ID,
                InputFile(io.BytesIO(videos[0]), filename="alert.mp4"),
                caption=text,
                parse_mode="HTML",
            )
        else:
            # Send text first, then media group with all videos
            await bot.send_message(config.GROUP_CHAT_ID, text, parse_mode="HTML")
            media = [
                InputMediaVideo(
                    InputFile(io.BytesIO(v), filename=f"video_{i+1}.mp4"),
                    caption="Front camera" if i == 0 else "Interior camera",
                )
                for i, v in enumerate(videos)
            ]
            await bot.send_media_group(config.GROUP_CHAT_ID, media)
    else:
        await bot.send_message(config.GROUP_CHAT_ID, text, parse_mode="HTML")


async def check_emails(bot: Bot):
    while True:
        try:
            loop = asyncio.get_event_loop()
            messages = await loop.run_in_executor(None, _fetch_unread_motive_emails)
            for msg in messages:
                await process_email(bot, msg)
        except Exception as e:
            logger.error(f"Email check error: {e}")
        await asyncio.sleep(CHECK_INTERVAL)


async def start_email_checker(bot: Bot):
    asyncio.create_task(check_emails(bot))
