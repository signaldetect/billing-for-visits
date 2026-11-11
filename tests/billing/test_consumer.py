from decimal import Decimal

import pytest
from faststream.kafka import TestKafkaBroker

from app.billing.consumer import build_broker
from app.contracts.events import VISITS_TOPIC

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
