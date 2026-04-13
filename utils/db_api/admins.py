from utils.db_api import db


async def is_admin(telegram_id: int, company_id: int | None = None) -> bool:
    """Returns True if user is a super admin, or an active admin for the given company."""
    row = await db.fetchrow(
        "SELECT id, is_super, is_active FROM admins WHERE telegram_id = $1",
        telegram_id,
    )
    if not row or not row["is_active"]:
        return False
    if row["is_super"]:
        return True
    if company_id is None:
        return True
    exists = await db.fetchval(
        "SELECT 1 FROM admin_companies WHERE admin_id = $1 AND company_id = $2",
        row["id"], company_id,
    )
    return exists is not None


async def is_super_admin(telegram_id: int) -> bool:
    row = await db.fetchrow(
        "SELECT is_super, is_active FROM admins WHERE telegram_id = $1",
        telegram_id,
    )
    return bool(row and row["is_active"] and row["is_super"])


async def add_admin(telegram_id: int, added_by: int | None = None, is_super: bool = False) -> int:
    """Creates an admin record. User must already exist in users table. Returns admin id."""
    return await db.fetchval(
        """
        INSERT INTO admins (telegram_id, added_by, is_super)
        VALUES ($1, $2, $3)
        ON CONFLICT (telegram_id) DO UPDATE SET is_active = TRUE
        RETURNING id
        """,
        telegram_id, added_by, is_super,
    )


async def assign_company(admin_id: int, company_id: int):
    await db.execute(
        "INSERT INTO admin_companies (admin_id, company_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
        admin_id, company_id,
    )


async def get_subscribed_admins(event_type: str) -> list[int]:
    """Returns telegram_ids of active admins who want a DM for this event type."""
    rows = await db.fetch(
        """
        SELECT u.telegram_id
        FROM admin_subscriptions sub
        JOIN admins a ON a.id = sub.admin_id
        JOIN users u ON u.telegram_id = a.telegram_id
        WHERE a.is_active = TRUE
          AND (sub.event_type = $1 OR sub.event_type = 'all')
        """,
        event_type,
    )
    return [r["telegram_id"] for r in rows]
