from datetime import UTC, datetime
from uuid import uuid4

import asyncpg
import pytest

T0 = datetime(2026, 11, 11, 8, tzinfo=UTC)


async def test_visit_rejects_end_not_after_start(pool):
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute("INSERT INTO visits.visits (start_date_time, end_date_time, rate, status) "
                           "VALUES ($1, $1, 40, 'scheduled')", T0)


async def test_visit_rejects_unknown_status(pool):
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute("INSERT INTO visits.visits (start_date_time, end_date_time, rate, status) "
                           "VALUES ($1::timestamptz, $1::timestamptz + interval '1 hour', 40, 'done')", T0)


async def test_one_created_billing_record_per_visit(pool):
    v = uuid4()
    insert = "INSERT INTO billing.billing_records (visit_id, amount, status) VALUES ($1, 10, $2)"
    await pool.execute(insert, v, "reversed")
    await pool.execute(insert, v, "created")
    with pytest.raises(asyncpg.UniqueViolationError):
        await pool.execute(insert, v, "created")


async def test_outbox_payload_roundtrips_as_dict(pool):
    await pool.execute("INSERT INTO visits.outbox (id, aggregate_id, event_type, payload) "
                       "VALUES ($1, $2, 'x', $3)", uuid4(), uuid4(), {"rate": "40.00"})
    assert await pool.fetchval("SELECT payload FROM visits.outbox") == {"rate": "40.00"}
