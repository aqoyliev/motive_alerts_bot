from aiogram import types
from aiogram.dispatcher.filters.builtin import CommandStart

from loader import dp
from utils.db_api.users import upsert_user


@dp.message_handler(CommandStart())
async def bot_start(message: types.Message):
    await upsert_user(
        telegram_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username,
        language_code=message.from_user.language_code,
    )
    await message.answer(f"Salom, {message.from_user.full_name}!")
