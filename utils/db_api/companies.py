from utils.db_api import db


async def get_all_companies() -> list[dict]:
    rows = await db.fetch("SELECT id, slug, name FROM companies ORDER BY id")
    return [dict(r) for r in rows]


async def get_groups_for_event(company_slug: str, event_type: str) -> list[int]:
    """
    Returns telegram_group_ids that should receive this event type for the given company.
    - Company-specific groups: matched by slug, filtered by group_event_types if present.
    - Global groups (company_id IS NULL): matched for all companies, filtered by group_event_types if present.
    """
    rows = await db.fetch(
        """
        SELECT cg.telegram_group_id
        FROM company_groups cg
        LEFT JOIN companies c ON c.id = cg.company_id
        WHERE (c.slug = $1 OR cg.company_id IS NULL)
          AND (
              NOT EXISTS (
                  SELECT 1 FROM group_event_types WHERE group_id = cg.id
              )
              OR EXISTS (
                  SELECT 1 FROM group_event_types
                  WHERE group_id = cg.id AND event_type = $2
              )
          )
        """,
        company_slug, event_type,
    )
    return [r["telegram_group_id"] for r in rows]
