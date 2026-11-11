from collections.abc import Sequence
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


async def fetch_unpublished(conn: asyncpg.Connection, limit: int) -> list[asyncpg.Record]:
    return await conn.fetch(
        "SELECT id, aggregate_id, event_type, payload FROM visits.outbox "
        "WHERE published_at IS NULL ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED",
        limit,
    )


async def mark_published(conn: asyncpg.Connection, ids: Sequence[UUID]) -> None:
    await conn.execute(
        "UPDATE visits.outbox SET published_at = now() WHERE id = ANY($1::uuid[])", list(ids)
    )


async def record_failure(conn: asyncpg.Connection, outbox_id: UUID, error: str) -> None:
    await conn.execute(
        "UPDATE visits.outbox SET attempts = attempts + 1, last_error = $2 WHERE id = $1",
        outbox_id, error,
    )
