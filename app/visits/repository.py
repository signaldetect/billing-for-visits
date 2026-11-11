from collections.abc import Sequence
from datetime import datetime
from decimal import Decimal
from uuid import UUID

import asyncpg


async def mark_completed(
    conn: asyncpg.Connection, visit_id: UUID, allowed_from: Sequence[str]
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "UPDATE visits.visits SET status='completed', updated_at=now() "
        "WHERE id=$1 AND status = ANY($2::text[]) RETURNING *",
        visit_id,
        list(allowed_from),
    )


async def get_status(conn: asyncpg.Connection, visit_id: UUID) -> str | None:
    return await conn.fetchval("SELECT status FROM visits.visits WHERE id=$1", visit_id)


async def insert_visit(
    conn: asyncpg.Connection,
    *,
    start_date_time: datetime,
    end_date_time: datetime,
    description: str,
    rate: Decimal,
    status: str,
) -> asyncpg.Record:
    return await conn.fetchrow(
        "INSERT INTO visits.visits (start_date_time, end_date_time, description, rate, status) "
        "VALUES ($1, $2, $3, $4, $5) RETURNING *",
        start_date_time, end_date_time, description, rate, status,
    )


async def get_visit(conn: asyncpg.Connection, visit_id: UUID) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT * FROM visits.visits WHERE id=$1", visit_id)
