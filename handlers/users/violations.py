from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import types

from loader import dp
from utils.db_api.admins import is_admin
from utils.db_api.companies import get_all_companies
from utils.db_api.violations import get_top_violators, get_vehicle_breakdown
from utils.webhook_handler import EVENT_TYPE_MAP
from keyboards.inline.violations import (
    companies_keyboard,
    period_keyboard,
    event_type_keyboard,
    top10_keyboard,
    back_to_top10_keyboard,
    PERIOD_LABELS,
)
ET = ZoneInfo("America/New_York")


def _period_since(period: str) -> datetime:
    now = datetime.now(tz=ET)
    if period == "today":
        return now.replace(hour=0, minute=0, second=0, microsecond=0)
    elif period == "week":
        return (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
    else:  # month
        return now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)


def _format_top10_text(rows: list[dict], period_label: str, company_name: str, event_type: str) -> str:
    if event_type and event_type != "all":
        emoji, title = EVENT_TYPE_MAP.get(event_type, ("🚨", event_type.upper()))
        header = f"📊 <b>{company_name} — {period_label}</b>\n<i>{emoji} {title} only</i>\n"
    else:
        header = f"📊 <b>{company_name} — {period_label}</b>\n"

    if not rows:
        return header + "\n✅ No violations found."

    lines = [header]
    for i, row in enumerate(rows, 1):
        unit = row["vehicle_number"]
        total = row["total"]
        lines.append(f"{i}. 🚛 Unit {unit} — {total} violation{'s' if total != 1 else ''}")
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


# Company selected
@dp.callback_query_handler(lambda c: c.data.startswith("viol_company:"))
async def cb_company(call: types.CallbackQuery):
    company_slug = call.data.split(":")[1]
    await call.message.edit_text("Select period:", reply_markup=period_keyboard(company_slug))
    await call.answer()


# Period selected
@dp.callback_query_handler(lambda c: c.data.startswith("viol_period:"))
async def cb_period(call: types.CallbackQuery):
    _, company_slug, period = call.data.split(":")
    await call.message.edit_text(
        "Select event type:",
        reply_markup=event_type_keyboard(company_slug, period)
    )
    await call.answer()


# Event type selected → show top 10
@dp.callback_query_handler(lambda c: c.data.startswith("viol_etype:"))
async def cb_event_type(call: types.CallbackQuery):
    parts = call.data.split(":")
    company_slug = parts[1]
    period = parts[2]
    event_type = parts[3]

    companies = await get_all_companies()
    company_name = next((c["name"] for c in companies if c["slug"] == company_slug), company_slug)
    since = _period_since(period)
    et = event_type if event_type != "all" else None
    rows = await get_top_violators(company_slug, since, event_type=et, limit=10)
    text = _format_top10_text(rows, PERIOD_LABELS[period], company_name, event_type)
    kb = top10_keyboard(rows, company_slug, period, event_type)
    await call.message.edit_text(text, parse_mode="HTML", reply_markup=kb)
    await call.answer()


# Truck detail
@dp.callback_query_handler(lambda c: c.data.startswith("viol_detail:"))
async def cb_detail(call: types.CallbackQuery):
    parts = call.data.split(":")
    company_slug = parts[1]
    period = parts[2]
    event_type = parts[3]
    vehicle_number = ":".join(parts[4:])  # in case unit has colons

    since = _period_since(period)
    rows = await get_vehicle_breakdown(company_slug, vehicle_number, since)

    lines = [f"🚛 <b>Unit {vehicle_number}</b> — {PERIOD_LABELS[period]}\n"]
    for row in rows:
        etype = row["event_type"]
        emoji, title = EVENT_TYPE_MAP.get(etype, ("🚨", etype.upper()))
        lines.append(f"• {emoji} {title}: <b>{row['total']}</b>")

    await call.message.edit_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=back_to_top10_keyboard(company_slug, period, event_type)
    )
    await call.answer()
