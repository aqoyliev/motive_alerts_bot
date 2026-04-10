from utils.db_api import db


async def get_all_companies() -> list[dict]:
    rows = await db.fetch("SELECT id, slug, name FROM companies ORDER BY name")
    return [dict(r) for r in rows]


async def get_groups_for_event(company_slug: str, event_type: str) -> list[int]:
    """
    Returns telegram_group_ids for the company that should receive this event type.
    Groups with no rows in group_event_types receive all event types.
    Groups with rows only receive matching event types.
    """
    rows = await db.fetch(
        """
        SELECT cg.telegram_group_id
        FROM company_groups cg
        JOIN companies c ON c.id = cg.company_id
        WHERE c.slug = $1
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
