import asyncio
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from uuid import UUID, uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from testcontainers.community.postgres import PostgresContainer

from app.contracts.events import VisitCompletedV1
from app.main import create_app
from app.shared.db import create_pool

# Docker Desktop's host socket path can't be mounted into Ryuk; /var/run/docker.sock is the
# in-VM path on Docker Desktop, Colima and Linux.
os.environ.setdefault("TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE", "/var/run/docker.sock")

ROOT = Path(__file__).resolve().parent.parent
MIGRATION_DIRS = [ROOT / "db" / "visits" / "migrations", ROOT / "db" / "billing" / "migrations"]


def _up_section(sql: str) -> str:
    return sql.split("-- migrate:up", 1)[1].split("-- migrate:down", 1)[0]


async def apply_migrations(dsn: str) -> None:
    conn = await asyncpg.connect(dsn)
    try:
        for directory in MIGRATION_DIRS:
            for path in sorted(directory.glob("*.sql")):
                await conn.execute(_up_section(path.read_text()))
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def postgres_dsn() -> str:
    with PostgresContainer("postgres:16-alpine", driver=None) as pg:
        dsn = pg.get_connection_url()
        asyncio.run(apply_migrations(dsn))
        yield dsn


@pytest_asyncio.fixture
async def pool(postgres_dsn):
    pool = await create_pool(postgres_dsn)
    await pool.execute(
        "TRUNCATE visits.visits, visits.outbox, billing.billing_records, billing.processed_events"
    )
    try:
        yield pool
    finally:
        await pool.close()


@pytest.fixture
def make_visit(pool):
    async def _make(
        status: str = "in_progress",
        *,
        start: datetime = datetime(2026, 11, 11, 8, tzinfo=UTC),
        end: datetime = datetime(2026, 11, 11, 9, 30, tzinfo=UTC),
        rate: Decimal = Decimal("40.00"),
        description: str = "Home care",
    ) -> UUID:
        return await pool.fetchval(
            "INSERT INTO visits.visits (start_date_time, end_date_time, rate, status, description) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id",
            start, end, rate, status, description,
        )

    return _make


@pytest_asyncio.fixture
async def client(pool):
    transport = httpx.ASGITransport(app=create_app(pool=pool))
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


@pytest.fixture
def make_event():
    def _make(**overrides) -> VisitCompletedV1:
        fields = {
            "event_id": uuid4(),
            "occurred_at": datetime(2026, 11, 11, 9, 30, tzinfo=UTC),
            "visit_id": uuid4(),
            "start_date_time": datetime(2026, 11, 11, 8, tzinfo=UTC),
            "end_date_time": datetime(2026, 11, 11, 9, 30, tzinfo=UTC),
            "rate": Decimal("40.00"),
        }
        return VisitCompletedV1(**{**fields, **overrides})

    return _make
