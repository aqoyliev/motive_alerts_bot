import asyncio
import logging
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from aiogram import Bot

from utils.db_api.companies import get_all_companies, get_company_groups
from utils.db_api.violations import get_top_violators

logger = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


def _format_daily_report(company_name: str, rows: list[dict], date_str: str) -> str:
    header = f"📊 <b>Daily Violations Report</b>\n<b>{company_name}</b> — {date_str}\n"
    if not rows:
        return header + "\n✅ No violations today."
    lines = [header]
    for i, row in enumerate(rows, 1):
        lines.append(f"{i}. 🚛 Unit {row['vehicle_number']} — {row['total']} violation{'s' if row['total'] != 1 else ''}")
    return "\n".join(lines)


async def send_daily_reports(bot: Bot):
    now_et = datetime.now(tz=ET)
    today_start = now_et.replace(hour=0, minute=0, second=0, microsecond=0)
    date_str = today_start.strftime("%b %d, %Y")

    companies = await get_all_companies()
    for company in companies:
        try:
            rows = await get_top_violators(company["slug"], since=today_start, limit=10)
            text = _format_daily_report(company["name"], rows, date_str)
            group_ids = await get_company_groups(company["slug"])
            for chat_id in group_ids:
                await bot.send_message(chat_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Daily report error for {company['slug']}: {e}", exc_info=True)


async def schedule_daily_reports(bot: Bot):
    """Runs forever, sending the daily report at midnight ET each day."""
    while True:
        now_et = datetime.now(tz=ET)
        next_midnight = (now_et + timedelta(days=1)).replace(hour=0, minute=0, second=0, microsecond=0)
        wait_seconds = (next_midnight - now_et).total_seconds()
        logger.info(f"Daily report scheduled in {wait_seconds:.0f}s (at {next_midnight.strftime('%Y-%m-%d %H:%M %Z')})")
        await asyncio.sleep(wait_seconds)
        await send_daily_reports(bot)
