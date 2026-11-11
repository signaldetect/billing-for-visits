from decimal import Decimal
from uuid import UUID

import asyncpg


async def try_mark_processed(conn: asyncpg.Connection, event_id: UUID) -> bool:
    row = await conn.fetchval(
        "INSERT INTO billing.processed_events (event_id) VALUES ($1) "
        "ON CONFLICT DO NOTHING RETURNING event_id",
        event_id,
    )
    return row is not None


async def insert_record(conn: asyncpg.Connection, *, visit_id: UUID, amount: Decimal) -> UUID:
    return await conn.fetchval(
        "INSERT INTO billing.billing_records (visit_id, amount) VALUES ($1, $2) RETURNING id",
        visit_id,
        amount,
    )


async def list_records(conn: asyncpg.Connection, visit_id: UUID) -> list[asyncpg.Record]:
    return await conn.fetch(
        "SELECT * FROM billing.billing_records WHERE visit_id=$1 ORDER BY created_at, id",
        visit_id,
    )
