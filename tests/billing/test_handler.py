import asyncio
from datetime import UTC, datetime
from decimal import Decimal

import asyncpg

from app.billing import repository
from app.billing.handler import handle_visit_completed
from app.billing.pricing import calculate_amount
from tests.pg_helpers import wait_for_lock_waiters


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


async def test_concurrent_duplicate_deliveries_bill_once(pool, make_event, postgres_dsn, monkeypatch):
    e = make_event()
    gate = asyncio.Event()
    real_insert = repository.insert_record

    async def gated_insert(conn, **kwargs):
        # The first delivery holds its uncommitted inbox row here until the second one queues on it.
        await gate.wait()
        return await real_insert(conn, **kwargs)

    monkeypatch.setattr(repository, "insert_record", gated_insert)
    # Poll from a separate autocommit connection: pg_stat_activity is snapshotted once per transaction.
    monitor = await asyncpg.connect(postgres_dsn)
    tasks = [asyncio.create_task(handle_visit_completed(pool, e)) for _ in range(2)]
    try:
        await wait_for_lock_waiters(monitor, 1, timeout=10.0)
        gate.set()
        results = await asyncio.wait_for(asyncio.gather(*tasks), 10)
    finally:
        gate.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await monitor.close()
    assert sorted(results) == [False, True]
    assert len(await records(pool, e.visit_id)) == 1


async def test_second_event_for_same_visit_is_skipped(pool, make_event):
    first = make_event()
    await handle_visit_completed(pool, first)
    assert await handle_visit_completed(pool, make_event(visit_id=first.visit_id)) is False
    assert len(await records(pool, first.visit_id)) == 1


async def test_largest_valid_event_is_billed(pool, make_event):
    rate = Decimal("99999999.99")
    start = datetime(1, 1, 1, tzinfo=UTC)
    end = datetime(9999, 12, 31, 23, 59, 59, tzinfo=UTC)
    e = make_event(rate=rate, start_date_time=start, end_date_time=end)
    assert await handle_visit_completed(pool, e) is True
    [record] = await records(pool, e.visit_id)
    assert record["amount"] == calculate_amount(rate, start, end)
