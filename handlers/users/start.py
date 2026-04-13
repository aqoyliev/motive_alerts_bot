from aiogram import types
from aiogram.dispatcher.filters.builtin import CommandStart

from loader import dp
from utils.db_api.users import upsert_user
from utils.db_api.admins import is_admin, is_super_admin, add_admin
from keyboards.default.main_menu import main_menu_keyboard


@dp.message_handler(CommandStart())
async def bot_start(message: types.Message):
    await upsert_user(
        telegram_id=message.from_user.id,
        full_name=message.from_user.full_name,
        username=message.from_user.username,
        language_code=message.from_user.language_code,
    )
    if not await is_admin(message.from_user.id):
        await message.answer("⛔ You don't have access to this bot.")
        return
    await message.answer(
        f"Welcome, {message.from_user.full_name}!",
        reply_markup=main_menu_keyboard()
    )


@dp.message_handler(commands=["addadmin"])
async def cmd_add_admin(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("⛔ Only super admins can add admins.")
        return
    parts = message.text.strip().split()
    if len(parts) != 2 or not parts[1].lstrip("-").isdigit():
        await message.answer("Usage: /addadmin <telegram_id>")
        return
    new_id = int(parts[1])
    try:
        await add_admin(telegram_id=new_id, added_by=message.from_user.id, is_super=False)
        await message.answer(f"✅ Admin {new_id} added successfully.")
    except Exception as e:
        if "foreign key" in str(e).lower():
            await message.answer(f"❌ User {new_id} has never started the bot. Ask them to send /start first.")
        else:
            await message.answer(f"❌ Error: {e}")


@dp.message_handler(text="📊 Violations Report")
async def btn_violations(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    from handlers.users.violations import show_violations_menu
    await show_violations_menu(message)


@dp.message_handler(text="⚙️ Settings")
async def btn_settings(message: types.Message):
    await message.answer("Settings coming soon.")
