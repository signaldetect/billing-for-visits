import asyncio
from decimal import Decimal

from app.billing.handler import handle_visit_completed


async def records(pool, visit_id):
    return await pool.fetch("SELECT amount, status FROM billing.billing_records WHERE visit_id=$1", visit_id)


async def test_first_delivery_creates_record(pool, make_event):
    e = make_event()
    assert await handle_visit_completed(pool, e) is True
    assert [(r["amount"], r["status"]) for r in await records(pool, e.visit_id)] == [(Decimal("60.00"), "created")]
    assert await pool.fetchval("SELECT count(*) FROM billing.processed_events WHERE event_id=$1", e.event_id) == 1


async def test_redelivery_is_skipped(pool, make_event):
    e = make_event()
    await handle_visit_completed(pool, e)
    assert await handle_visit_completed(pool, e) is False
    assert len(await records(pool, e.visit_id)) == 1


async def test_concurrent_duplicate_deliveries_bill_once(pool, make_event):
    e = make_event()
    results = await asyncio.gather(handle_visit_completed(pool, e), handle_visit_completed(pool, e))
    assert sorted(results) == [False, True]
    assert len(await records(pool, e.visit_id)) == 1


async def test_second_event_for_same_visit_is_skipped(pool, make_event):
    first = make_event()
    await handle_visit_completed(pool, first)
    assert await handle_visit_completed(pool, make_event(visit_id=first.visit_id)) is False
    assert len(await records(pool, first.visit_id)) == 1
