import logging
import urllib.parse

from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage

from data import config

bot = Bot(token=config.BOT_TOKEN, parse_mode=types.ParseMode.HTML, timeout=300)

if config.REDIS_URL:
    try:
        from aiogram.contrib.fsm_storage.redis import RedisStorage2
        _u = urllib.parse.urlparse(config.REDIS_URL)
        storage = RedisStorage2(
            host=_u.hostname or "localhost",
            port=_u.port or 6379,
            db=int(_u.path.lstrip("/") or 0),
            password=_u.password,
        )
        logging.info("FSM storage: Redis")
    except ImportError:
        logging.warning("aioredis not installed — falling back to MemoryStorage. Run: pipenv install aioredis")
        storage = MemoryStorage()
else:
    logging.warning("REDIS_URL not set — using MemoryStorage. FSM state lost on restart.")
    storage = MemoryStorage()

dp = Dispatcher(bot, storage=storage)
