from datetime import timedelta
from decimal import Decimal

import asyncpg
import pytest
from faststream.kafka import TestKafkaBroker

from app.billing.consumer import build_broker
from app.billing.handler import process_visits_message
from app.contracts.events import VISITS_TOPIC
from app.shared.db import create_pool

HEADERS = {"event_type": "visits.visit_completed.v1"}


async def publish(pool, body, headers):
    async with TestKafkaBroker(build_broker("localhost:9092", lambda: pool)) as br:
        await br.publish(body, VISITS_TOPIC, headers=headers)


async def count(pool):
    return await pool.fetchval("SELECT count(*) FROM billing.billing_records")


async def test_visit_completed_message_bills_visit(pool, make_event):
    e = make_event()
    await publish(pool, e.model_dump(mode="json"), HEADERS | {"event_id": str(e.event_id)})
    amount = await pool.fetchval(
        "SELECT amount FROM billing.billing_records WHERE visit_id=$1", e.visit_id
    )
    assert amount == Decimal("60.00")


async def test_duplicate_message_bills_once(pool, make_event):
    e = make_event()
    for _ in range(2):
        await publish(pool, e.model_dump(mode="json"), HEADERS)
    assert await count(pool) == 1


async def test_unknown_event_type_is_skipped(pool, make_event):
    await publish(
        pool, make_event().model_dump(mode="json"), {"event_type": "visits.visit_rescheduled.v1"}
    )
    assert await count(pool) == 0


@pytest.mark.parametrize(
    "body",
    [
        b"not json",
        b"[" * 100_000,
        {"visit_id": "x"},
        {
            "event_id": "6f1c1c0e-5c58-4b8e-9d57-0d5f5a3c1a11",
            "occurred_at": "2026-11-11T09:30:00Z",
            "visit_id": "0b6c1d2e-5c58-4b8e-9d57-0d5f5a3c1a12",
            "start_date_time": "2026-11-11T08:00:00Z",
            "end_date_time": "2026-11-11T08:00:00Z",
            "rate": "40.00",
        },
    ],
)
async def test_malformed_message_is_skipped(pool, body):
    await publish(pool, body, HEADERS)  # must not raise: TestKafkaBroker re-raises handler errors
    assert await count(pool) == 0


async def test_non_json_body_with_json_content_type_is_skipped(pool):
    await publish(pool, b"not json", HEADERS | {"content-type": "application/json"})
    assert await count(pool) == 0


async def test_amount_overflowing_column_is_skipped(pool, make_event):
    e = make_event(
        rate=Decimal("99999999.99"),
        end_date_time=make_event().start_date_time + timedelta(hours=101),
    )
    await publish(pool, e.model_dump(mode="json"), HEADERS)
    assert await count(pool) == 0


async def test_infrastructure_error_propagates(postgres_dsn, make_event):
    closed = await create_pool(postgres_dsn)
    await closed.close()
    raw = make_event().model_dump_json().encode()
    with pytest.raises(asyncpg.InterfaceError):
        await process_visits_message(closed, raw, HEADERS)

