from utils.db_api import db


async def save_violation(company_slug: str, vehicle_number: str, event_type: str,
                         event_id: int | None, occurred_at) -> None:
    await db.execute(
        """
        INSERT INTO violations (company_slug, vehicle_number, event_type, event_id, occurred_at)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (event_id) DO NOTHING
        """,
        company_slug, vehicle_number, event_type, event_id, occurred_at,
    )


async def get_top_violators(company_slug: str, since, event_type: str | None = None,
                             limit: int = 10) -> list[dict]:
    """Returns top vehicles ranked by violation count."""
    if event_type:
        rows = await db.fetch(
            """
            SELECT vehicle_number, COUNT(*) AS total
            FROM violations
            WHERE company_slug = $1 AND occurred_at >= $2 AND event_type = $3
            GROUP BY vehicle_number
            ORDER BY total DESC
            LIMIT $4
            """,
            company_slug, since, event_type, limit,
        )
    else:
        rows = await db.fetch(
            """
            SELECT vehicle_number, COUNT(*) AS total
            FROM violations
            WHERE company_slug = $1 AND occurred_at >= $2
            GROUP BY vehicle_number
            ORDER BY total DESC
            LIMIT $4
            """,
            company_slug, since, limit,
        )
    return [dict(r) for r in rows]


async def get_vehicle_breakdown(company_slug: str, vehicle_number: str, since) -> list[dict]:
    """Returns violation counts per event type for a specific vehicle."""
    rows = await db.fetch(
        """
        SELECT event_type, COUNT(*) AS total
        FROM violations
        WHERE company_slug = $1 AND vehicle_number = $2 AND occurred_at >= $3
        GROUP BY event_type
        ORDER BY total DESC
        """,
        company_slug, vehicle_number, since,
    )
    return [dict(r) for r in rows]


async def get_top_violators_all_companies(since) -> list[dict]:
    """Returns top 5 violators per company for daily auto-report."""
    rows = await db.fetch(
        """
        SELECT company_slug, vehicle_number, COUNT(*) AS total
        FROM violations
        WHERE occurred_at >= $1
        GROUP BY company_slug, vehicle_number
        ORDER BY company_slug, total DESC
        """,
        since,
    )
    return [dict(r) for r in rows]
