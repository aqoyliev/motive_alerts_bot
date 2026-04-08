from datetime import datetime

# Maps GoMotive event_type values to display labels and emojis
EVENT_TYPE_MAP = {
    "Speeding":               ("🚨 SPEEDING",             "speed"),
    "HardBraking":            ("⚠️ HARSH BRAKE",           "brake"),
    "HardAcceleration":       ("⚡ HARSH ACCELERATION",    "accel"),
    "HardTurn":               ("↩️ HARSH TURN",             "turn"),
    "FollowingDistance":      ("🚗 FOLLOWING DISTANCE",    "follow"),
    "Distraction":            ("📵 DISTRACTION",           "distract"),
    "SeatbeltCompliance":     ("🔒 SEATBELT VIOLATION",    "seatbelt"),
    "Collision":              ("💥 COLLISION",             "collision"),
    "UnsafeParking":          ("🅿️ UNSAFE PARKING",        "parking"),
    "RolloverProtection":     ("🔄 ROLLOVER RISK",         "rollover"),
    "DriverDrowsiness":       ("😴 DROWSINESS",            "drowsy"),
    "UnauthorizedDriving":    ("🚫 UNAUTHORIZED DRIVING",  "unauth"),
    "StopSignViolation":      ("🛑 STOP SIGN VIOLATION",   "stopsign"),
    "TrafficLightViolation":  ("🚦 TRAFFIC LIGHT VIOLATION","light"),
    "RailroadViolation":      ("🚂 RAILROAD VIOLATION",    "railroad"),
}

SEVERITY_MAP = {
    0: "Low",
    1: "Medium",
    2: "High",
    3: "Critical",
}


def _fmt_time(ts: str) -> str:
    """Format ISO timestamp to readable form."""
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %Y  %H:%M UTC")
    except Exception:
        return ts


def _fmt_driver(event: dict) -> str:
    driver = event.get("driver") or {}
    first = driver.get("first_name", "")
    last = driver.get("last_name", "")
    username = driver.get("username", "")
    name = f"{first} {last}".strip() or username or "Unknown"
    return name


def _fmt_vehicle(event: dict) -> str:
    vehicle = event.get("vehicle") or {}
    number = vehicle.get("number", "")
    year = vehicle.get("year", "")
    make = vehicle.get("make", "")
    model = vehicle.get("model", "")
    parts = [p for p in [number, f"{year} {make} {model}".strip()] if p]
    return " | ".join(parts) if parts else "Unknown"


def _fmt_location(event: dict) -> str:
    loc = event.get("start_location") or {}
    desc = loc.get("description", "")
    lat = loc.get("lat")
    lon = loc.get("lon")
    if desc:
        return desc
    if lat and lon:
        return f"{lat:.5f}, {lon:.5f}"
    return "N/A"


def _fmt_speed(event: dict) -> str:
    max_val = event.get("max_value")
    speed_limit = event.get("posted_speed_limit")
    unit = "mph"
    if max_val is not None:
        line = f"{max_val} {unit}"
        if speed_limit:
            line += f" (limit: {speed_limit} {unit})"
        return line
    return None


def format_safety_event(event: dict) -> str:
    """Format a single GoMotive safety event into a Telegram HTML message."""
    raw_type = event.get("event_type", "Unknown")
    label, _ = EVENT_TYPE_MAP.get(raw_type, (f"⚠️ {raw_type.upper()}", "unknown"))

    severity_raw = event.get("severity")
    severity = SEVERITY_MAP.get(severity_raw, str(severity_raw) if severity_raw is not None else None)

    driver = _fmt_driver(event)
    vehicle = _fmt_vehicle(event)
    location = _fmt_location(event)
    start_time = _fmt_time(event.get("start_time", ""))

    lines = [
        f"<b>{label}</b>",
        "",
        f"👤 <b>Driver:</b> {driver}",
        f"🚛 <b>Vehicle:</b> {vehicle}",
        f"📍 <b>Location:</b> {location}",
        f"🕐 <b>Time:</b> {start_time}",
    ]

    if severity:
        lines.append(f"📊 <b>Severity:</b> {severity}")

    speed_line = _fmt_speed(event)
    if speed_line:
        lines.append(f"💨 <b>Speed:</b> {speed_line}")

    duration = event.get("duration")
    if duration:
        lines.append(f"⏱ <b>Duration:</b> {duration}s")

    # Video clip link if available
    video_url = (event.get("video_clip") or {}).get("url")
    if video_url:
        lines.append(f'🎥 <a href="{video_url}">View Clip</a>')

    return "\n".join(lines)
