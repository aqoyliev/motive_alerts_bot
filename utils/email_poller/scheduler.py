import asyncio
import logging

from apscheduler.schedulers.asyncio import AsyncIOScheduler

from loader import bot
from data import config
from utils.email_poller.imap_client import fetch_unread_motive_emails
from utils.email_poller.parser import parse_motive_email, format_for_telegram

logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()


async def check_emails():
    """Fetch unread Motive emails and forward alerts to Telegram."""
    try:
        loop = asyncio.get_event_loop()
        emails = await loop.run_in_executor(
            None,
            fetch_unread_motive_emails,
            config.GMAIL_USER,
            config.GMAIL_APP_PASSWORD,
        )

        if not emails:
            return

        logger.info(f"Found {len(emails)} new Motive email(s)")

        for body in emails:
            logger.info(f"--- EMAIL BODY START ---\n{body}\n--- EMAIL BODY END ---")
            data = parse_motive_email(body)
            if not data:
                logger.warning("Email did not parse as a Motive safety alert — skipping")
                continue

            logger.info(f"Parsed data: {data}")
            message = format_for_telegram(data)
            await bot.send_message(
                chat_id=config.ALERT_CHANNEL_ID,
                text=message,
                parse_mode="HTML",
            )

    except Exception as e:
        logger.error(f"Error in check_emails: {e}")


def setup_email_scheduler(interval_minutes: int = 2):
    scheduler.add_job(
        check_emails,
        trigger="interval",
        minutes=interval_minutes,
        id="email_poller",
        replace_existing=True,
    )
    scheduler.start()
    logger.info(f"Email scheduler started (every {interval_minutes} min)")
