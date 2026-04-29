from aiogram import types
from aiogram.utils.exceptions import MessageCantBeEdited, MessageNotModified


async def edit_or_send(call: types.CallbackQuery, text: str, reply_markup, parse_mode="HTML"):
    try:
        await call.message.edit_text(text, parse_mode=parse_mode, reply_markup=reply_markup)
    except MessageNotModified:
        pass
    except MessageCantBeEdited:
        await call.message.answer(text, parse_mode=parse_mode, reply_markup=reply_markup)