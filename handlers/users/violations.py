import io
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import types

from loader import dp
from utils.db_api.admins import is_admin
from utils.db_api.companies import get_all_companies
from utils.db_api.violations import get_top_violators, get_vehicle_breakdown, get_vehicle_events
from utils.webhook_handler import EVENT_TYPE_MAP
from keyboards.inline.violations import (
    companies_keyboard,
    period_keyboard,
    event_type_keyboard,
    top10_keyboard,
    PERIOD_LABELS,
)
ET = ZoneInfo("America/New_York")


def _period_range(period: str) -> tuple[datetime, datetime]:
    """Returns (since, until) for the period."""
    now = datetime.now(tz=ET)
    if period == "today":
        since = now.replace(hour=0, minute=0, second=0, microsecond=0)
        until = now
    elif period == "last_week":
        start_of_this_week = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
        since = start_of_this_week - timedelta(weeks=1)
        until = start_of_this_week
    else:  # last_month
        first_of_this_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        since = (first_of_this_month - timedelta(days=1)).replace(day=1)
        until = first_of_this_month
    return since, until


def _period_since(period: str) -> datetime:
    return _period_range(period)[0]


def _format_top10_text(rows: list[dict], period_label: str, company_name: str, event_type: str) -> str:
    if event_type == "speeding":
        header = f"📊 <b>{company_name} — {period_label}</b>\n<i>🚨 Speeding only</i>\n"
    elif event_type == "other":
        header = f"📊 <b>{company_name} — {period_label}</b>\n<i>⚠️ Other violations (excl. speeding)</i>\n"
    else:
        header = f"📊 <b>{company_name} — {period_label}</b>\n"

    if not rows:
        return header + "\n✅ No violations found."

    lines = [header]
    for i, row in enumerate(rows, 1):
        unit = row["vehicle_number"]
        total = row["total"]
        lines.append(f"{i}. 🚛 Unit {unit} — {total}")
    return "\n".join(lines)


# Entry point — called from start menu
async def show_violations_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ Access denied.")
        return
    companies = await get_all_companies()
    if not companies:
        await message.answer("No companies configured.")
        return
    await message.answer("Select a company:", reply_markup=companies_keyboard(companies))


# Back: to company list
@dp.callback_query_handler(lambda c: c.data == "viol_bk_co")
async def cb_back_companies(call: types.CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer("⛔ Access denied.", show_alert=True)
        return
    companies = await get_all_companies()
    await call.message.edit_text("Select a company:", reply_markup=companies_keyboard(companies))
    await call.answer()


# Back: to period selection
@dp.callback_query_handler(lambda c: c.data.startswith("viol_bk_per:"))
async def cb_back_period(call: types.CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer("⛔ Access denied.", show_alert=True)
        return
    company_slug = call.data.split(":")[1]
    await call.message.edit_text("Select period:", reply_markup=period_keyboard(company_slug))
    await call.answer()


# Back: to event type selection
@dp.callback_query_handler(lambda c: c.data.startswith("viol_bk_et:"))
async def cb_back_event_type(call: types.CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer("⛔ Access denied.", show_alert=True)
        return
    _, company_slug, period = call.data.split(":")
    await call.message.edit_text("Select event type:", reply_markup=event_type_keyboard(company_slug, period))
    await call.answer()


# Company selected
@dp.callback_query_handler(lambda c: c.data.startswith("viol_company:"))
async def cb_company(call: types.CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer("⛔ Access denied.", show_alert=True)
        return
    company_slug = call.data.split(":")[1]
    await call.message.edit_text("Select period:", reply_markup=period_keyboard(company_slug))
    await call.answer()


# Period selected
@dp.callback_query_handler(lambda c: c.data.startswith("viol_period:"))
async def cb_period(call: types.CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer("⛔ Access denied.", show_alert=True)
        return
    _, company_slug, period = call.data.split(":")
    await call.message.edit_text(
        "Select event type:",
        reply_markup=event_type_keyboard(company_slug, period)
    )
    await call.answer()


# Event type selected → show top 10
@dp.callback_query_handler(lambda c: c.data.startswith("viol_etype:"))
async def cb_event_type(call: types.CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer("⛔ Access denied.", show_alert=True)
        return
    parts = call.data.split(":")
    company_slug = parts[1]
    period = parts[2]
    event_type = parts[3]

    companies = await get_all_companies()
    company_name = next((c["name"] for c in companies if c["slug"] == company_slug), company_slug)
    since, until = _period_range(period)
    rows = await get_top_violators(company_slug, since, until=until, event_type=event_type, limit=10)
    text = _format_top10_text(rows, PERIOD_LABELS[period], company_name, event_type)
    kb = top10_keyboard(rows, company_slug, period, event_type)
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await call.answer()


# Download full report
@dp.callback_query_handler(lambda c: c.data.startswith("viol_dl:"))
async def cb_download(call: types.CallbackQuery):
    if not await is_admin(call.from_user.id):
        await call.answer("⛔ Access denied.", show_alert=True)
        return
    parts = call.data.split(":")
    company_slug = parts[1]
    period = parts[2]
    event_type = parts[3]

    await call.answer("Generating report...")

    companies = await get_all_companies()
    company_name = next((c["name"] for c in companies if c["slug"] == company_slug), company_slug)
    since, until = _period_range(period)
    rows = await get_top_violators(company_slug, since, until=until, event_type=event_type, limit=50)

    now = datetime.now(tz=ET)
    date_range = f"{since.strftime('%b %d')} — {until.strftime('%b %d, %Y')}"
    lines = [
        f"{company_name} — {PERIOD_LABELS[period]} ({date_range})",
        f"Generated: {now.strftime('%b %d, %Y %I:%M %p ET')}",
        "",
    ]
    if event_type == "speeding":
        lines.append("Filter: Speeding only")
        lines.append("")
    elif event_type == "other":
        lines.append("Filter: Other violations (excl. speeding)")
        lines.append("")

    rank = 1
    for row in rows:
        unit = row["vehicle_number"]
        events = await get_vehicle_events(company_slug, unit, since, until=until, event_type=event_type)

        if event_type == "speeding":
            # Group by date, skip days with < 3 events
            by_date: dict[str, list[str]] = {}
            for e in events:
                local = e["occurred_at"].astimezone(ET)
                day = local.strftime("%b %d")
                by_date.setdefault(day, []).append(local.strftime("%I:%M %p").lstrip("0"))
            day_lines = []
            for day, times in by_date.items():
                if len(times) >= 3:
                    day_lines.append(f"     • {day}, {', '.join(times)} — {len(times)} times")
            if not day_lines:
                continue  # skip this unit entirely
            total_shown = sum(len(by_date[d]) for d in by_date if len(by_date[d]) >= 3)
            lines.append(f"{rank}. Unit {unit} — {total_shown} speeding events")
            lines.extend(day_lines)
        else:
            total = row["total"]
            lines.append(f"{rank}. Unit {unit} — {total} event{'s' if total != 1 else ''}")
            for e in events:
                local = e["occurred_at"].astimezone(ET)
                ts = local.strftime("%b %d, %I:%M %p ET")
                if event_type in (None, "other") and e.get("event_type"):
                    _, title = EVENT_TYPE_MAP.get(e["event_type"], ("🚨", e["event_type"].upper()))
                    lines.append(f"     • {ts} — {title}")
                else:
                    lines.append(f"     • {ts}")

        lines.append("")
        rank += 1

    content = "\n".join(lines).encode("utf-8")
    filename = f"{company_slug}_{period}_report.txt"
    await call.message.answer_document(
        types.InputFile(io.BytesIO(content), filename=filename),
        caption=f"📊 {company_name} — {PERIOD_LABELS[period]}"
    )
