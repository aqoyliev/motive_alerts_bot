import logging

from aiogram import types

from loader import dp

logger = logging.getLogger(__name__)


@dp.my_chat_member_handler()
async def on_bot_chat_member_update(update: types.ChatMemberUpdated):
    """Track when the bot is added to or removed from a group/channel."""
    old = update.old_chat_member.status
    new = update.new_chat_member.status
    chat = update.chat

    if new in ("member", "administrator") and old in ("left", "kicked"):
        logger.info(f"Bot added to {chat.type} '{chat.title}' (id={chat.id})")
    elif new in ("left", "kicked") and old in ("member", "administrator"):
        logger.info(f"Bot removed from {chat.type} '{chat.title}' (id={chat.id})")
