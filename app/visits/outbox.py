from typing import Any
from uuid import UUID

import asyncpg


async def insert_event(
    conn: asyncpg.Connection,
    *,
    event_id: UUID,
    aggregate_id: UUID,
    event_type: str,
    payload: dict[str, Any],
) -> None:
    await conn.execute(
        "INSERT INTO visits.outbox (id, aggregate_id, event_type, payload) VALUES ($1, $2, $3, $4)",
        event_id, aggregate_id, event_type, payload,
    )
