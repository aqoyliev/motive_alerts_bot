from environs import Env

env = Env()
env.read_env()

BOT_TOKEN = env.str("BOT_TOKEN")
ADMINS = env.list("ADMINS")
IP = env.str("ip")

GROUP_CHAT_ID = env.int("GROUP_CHAT_ID", default=0)

DATABASE_URL = env.str("DATABASE_URL")

COMPANY_SLUG = env.str("COMPANY_SLUG")

SAMSARA_API_KEY = env.str("SAMSARA_API_KEY", default="")

# Webhook HMAC secrets — if set, incoming webhook signatures are validated
MOTIVE_WEBHOOK_SECRET = env.str("MOTIVE_WEBHOOK_SECRET", default="")
SAMSARA_WEBHOOK_SECRET = env.str("SAMSARA_WEBHOOK_SECRET", default="")

# Optional Redis URL for persistent FSM storage (e.g. redis://localhost:6379/0)
REDIS_URL = env.str("REDIS_URL", default="")
