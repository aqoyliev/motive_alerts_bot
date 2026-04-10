from aiogram.dispatcher.filters.state import State, StatesGroup


class ViolationsFlow(StatesGroup):
    company = State()
    period = State()
    event_type = State()
    top10 = State()
