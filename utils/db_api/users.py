from utils.db_api import db


async def upsert_user(telegram_id: int, full_name: str, username: str | None, language_code: str | None):
    await db.execute(
        """
        INSERT INTO users (telegram_id, full_name, username, language_code)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (telegram_id) DO UPDATE
            SET full_name     = EXCLUDED.full_name,
                username      = EXCLUDED.username,
                language_code = EXCLUDED.language_code,
                updated_at    = NOW()
        """,
        telegram_id, full_name, username, language_code,
    )
