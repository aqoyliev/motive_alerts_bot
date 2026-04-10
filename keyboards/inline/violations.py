from aiogram import types

from utils.webhook_handler import EVENT_TYPE_MAP

PERIOD_LABELS = {
    "today": "Today",
    "week": "This Week",
    "month": "This Month",
}


def companies_keyboard(companies: list[dict]) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    for c in companies:
        kb.insert(types.InlineKeyboardButton(c["name"], callback_data=f"viol_company:{c['slug']}"))
    return kb


def period_keyboard(company_slug: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=3)
    for key, label in PERIOD_LABELS.items():
        kb.insert(types.InlineKeyboardButton(label, callback_data=f"viol_period:{company_slug}:{key}"))
    return kb


def event_type_keyboard(company_slug: str, period: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=3)
    for etype, (emoji, _) in EVENT_TYPE_MAP.items():
        kb.insert(types.InlineKeyboardButton(
            emoji, callback_data=f"viol_etype:{company_slug}:{period}:{etype}"
        ))
    kb.add(types.InlineKeyboardButton("All Types ✅", callback_data=f"viol_etype:{company_slug}:{period}:all"))
    return kb


def top10_keyboard(rows: list[dict], company_slug: str, period: str, event_type: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for row in rows:
        unit = row["vehicle_number"]
        total = row["total"]
        kb.add(types.InlineKeyboardButton(
            f"🚛 Unit {unit} ({total})",
            callback_data=f"viol_detail:{company_slug}:{period}:{event_type}:{unit}"
        ))
    return kb


def back_to_top10_keyboard(company_slug: str, period: str, event_type: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup()
    kb.add(types.InlineKeyboardButton("◀ Back", callback_data=f"viol_etype:{company_slug}:{period}:{event_type}"))
    return kb
