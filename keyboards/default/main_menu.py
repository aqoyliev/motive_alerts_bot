from aiogram import types


def main_menu_keyboard() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(types.KeyboardButton("📊 Violations Report"))
    kb.add(types.KeyboardButton("⚙️ Settings"))
    return kb
