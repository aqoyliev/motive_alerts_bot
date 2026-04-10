from aiogram import types


async def set_default_commands(dp):
    await dp.bot.set_my_commands(
        [
            types.BotCommand("start", "Open main menu"),
            types.BotCommand("help", "How to use this bot"),
        ]
    )
