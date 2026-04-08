import logging
from datetime import datetime, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from loader import bot
from data import config
from utils.motive.client import fetch_safety_events
from utils.motive.formatter import format_safety_event
from utils.db_api.state_store import (
    get_last_fetch_time,
    set_last_fetch_time,
    get_seen_event_ids,
    add_seen_event_ids,
)

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def check_safety_events():
    """Poll GoMotive for new safety events and send alerts to Telegram."""
    try:
        start_time = get_last_fetch_time()
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        logger.info(f"Fetching safety events from {start_time} to {now}")
        events = await fetch_safety_events(start_time=start_time, end_time=now)

        if not events:
            set_last_fetch_time(now)
            return

        seen_ids = get_seen_event_ids()
        new_events = [e for e in events if str(e.get("id")) not in seen_ids]

        if not new_events:
            set_last_fetch_time(now)
            return

        logger.info(f"Found {len(new_events)} new safety event(s)")

        # Sort by time ascending so alerts arrive in order
        new_events.sort(key=lambda e: e.get("start_time", ""))

        sent_ids = []
        for event in new_events:
            try:
                message = format_safety_event(event)
                await bot.send_message(
                    chat_id=config.ALERT_CHANNEL_ID,
                    text=message,
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                )
                sent_ids.append(str(event.get("id")))
            except Exception as e:
                logger.error(f"Failed to send event {event.get('id')}: {e}")

        add_seen_event_ids(sent_ids)
        set_last_fetch_time(now)

    except Exception as e:
        logger.error(f"Error in check_safety_events: {e}")


def setup_scheduler(interval_minutes: int = None):
    """Register the polling job and start the scheduler."""
    if interval_minutes is None:
        interval_minutes = config.POLL_INTERVAL_MINUTES

    scheduler.add_job(
        check_safety_events,
        trigger="interval",
        minutes=interval_minutes,
        id="safety_events_poller",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Safety events scheduler started (every {interval_minutes} min)")
