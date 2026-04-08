import re
from datetime import datetime, timezone
from zoneinfo import ZoneInfo
from typing import Optional

EASTERN = ZoneInfo("America/New_York")

SEVERITY_WORDS = {"Low", "Medium", "High", "Critical"}

# Maps exact label in email → key we store it under
LABEL_MAP = {
    "Vehicle":                      "Vehicle",
    "Driver":                       "Driver",
    "Date/Time":                    "Date/Time",
    "Location":                     "Location",
    "Unsafe Behaviours":            "Unsafe Behaviours",
    "Distance":                     "Distance",
    "Duration":                     "Duration",
    "Average Speed detected":       "Average Speed",
    "Max Over Posted detected":     "Max Over Posted",
    "Speed Limit detected":         "Speed Limit",
    "Vehicle Speed Range detected": "Vehicle Speed Range",
    "Average Exceeded detected":    "Average Exceeded",
    "Max Speed detected":           "Max Speed",
    "Posted Speed Limit detected":  "Posted Speed Limit",
}

EVENT_EMOJIS = {
    "speeding":           "🚨",
    "harsh brake":        "⚠️",
    "harsh braking":      "⚠️",
    "collision":          "💥",
    "unsafe parking":     "🅿️",
    "mobile phone":       "📵",
    "phone":              "📵",
    "distraction":        "📵",
    "harsh acceleration": "⚡",
    "harsh turn":         "↩️",
    "following distance": "🚗",
    "seatbelt":           "🔒",
    "drowsy":             "😴",
    "drowsiness":         "😴",
}

SEVERITY_EMOJIS = {
    "low":      "🟢",
    "medium":   "🟡",
    "high":     "🔴",
    "critical": "🚨",
}


def _to_eastern(time_str: str) -> str:
    """Convert 'Apr 7, 12:46 AM UTC' → 'Apr 7, 8:46 AM EDT'."""
    try:
        clean = time_str.replace(" UTC", "").strip()
        year = datetime.now().year
        dt = datetime.strptime(f"{clean} {year}", "%b %d, %I:%M %p %Y")
        dt_utc = dt.replace(tzinfo=timezone.utc)
        dt_et = dt_utc.astimezone(EASTERN)
        tz_name = dt_et.strftime("%Z")
        day = str(dt_et.day)
        return dt_et.strftime(f"%b {day}, %I:%M %p {tz_name}")
    except Exception:
        return time_str


def parse_motive_email(body: str) -> Optional[dict]:
    """Parse a Motive plain-text safety alert email into a structured dict."""
    if "safety alert" not in body.lower():
        return None

    lines = [l.strip() for l in body.splitlines() if l.strip()]
    data = {}

    # Summary line: "X detected involving Vehicle Y from Groups Z"
    for line in lines:
        if "detected involving" in line.lower() or "detected for" in line.lower():
            data["summary"] = line
            break

    # Groups from summary line: "from Groups NO ALERT, APPLE JB"
    if "summary" in data:
        m = re.search(r'from Groups?\s+(.+)', data["summary"], re.IGNORECASE)
        if m:
            data["Groups"] = m.group(1).strip()

    # Severity is a standalone word (no label before it)
    for line in lines:
        if line in SEVERITY_WORDS:
            data["Severity"] = line
            break

    # View Details link
    for line in lines:
        if line.startswith("View Details at "):
            data["url"] = line.replace("View Details at ", "").strip()
            break

    # Parse label → value pairs
    label_set = set(LABEL_MAP.keys())
    i = 0
    while i < len(lines):
        line = lines[i]
        if line in label_set:
            key = LABEL_MAP[line]
            if i + 1 < len(lines) and lines[i + 1] not in label_set:
                data[key] = lines[i + 1]
                i += 2
            else:
                i += 1
        else:
            i += 1

    return data if len(data) > 1 else None


def format_for_telegram(data: dict) -> str:
    """Format parsed Motive email into a Telegram HTML message."""

    event_type = data.get("Unsafe Behaviours", "")
    summary = data.get("summary", "")

    emoji = "⚠️"
    for keyword, e in EVENT_EMOJIS.items():
        if keyword in (event_type + " " + summary).lower():
            emoji = e
            break

    severity = data.get("Severity", "")
    sev_emoji = SEVERITY_EMOJIS.get(severity.lower(), "")

    vehicle = data.get("Vehicle", "N/A")
    groups = data.get("Groups", "")
    driver = data.get("Driver", "")
    dt = data.get("Date/Time", "")
    location = data.get("Location", "")
    url = data.get("url", "")

    lines = [
        f"{emoji} <b>{event_type.upper() if event_type else 'SAFETY ALERT'}</b>",
        "",
        f"🚛 <b>Vehicle:</b> {vehicle}",
    ]

    if driver:
        lines.append(f"👤 <b>Driver:</b> {driver}")

    if severity:
        lines.append(f"📊 <b>Severity:</b> {sev_emoji} {severity}")

    if dt:
        lines.append(f"🕐 <b>Time:</b> {_to_eastern(dt)}")

    if location:
        lines.append(f"📍 <b>Location:</b> {location}")

    for label, icon in [
        ("Average Speed",   "💨"),
        ("Speed Limit",     "🚦"),
        ("Max Over Posted", "📈"),
    ]:
        val = data.get(label)
        if val:
            lines.append(f"{icon} <b>{label}:</b> {val}")

    return "\n".join(lines)
