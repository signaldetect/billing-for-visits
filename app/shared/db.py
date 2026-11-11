import json

import asyncpg
from fastapi import Request


async def _init_connection(conn: asyncpg.Connection) -> None:
    await conn.set_type_codec(
        "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
    )


async def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool:
    return await asyncpg.create_pool(
        dsn, min_size=min_size, max_size=max_size, init=_init_connection
    )


def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool
