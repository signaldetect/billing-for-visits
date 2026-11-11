import asyncio

import asyncpg

from tests.pg_helpers import wait_for_lock_waiters


async def test_two_concurrent_completes_emit_one_event(client, pool, make_visit, postgres_dsn):
    vid = await make_visit("in_progress")
    locker = await asyncpg.connect(postgres_dsn)
    # Poll from a separate autocommit connection: pg_stat_activity is snapshotted once
    # per transaction, so polls inside the locker's transaction never see the pool's
    # second backend, which connects after the first poll.
    monitor = await asyncpg.connect(postgres_dsn)
    tx = locker.transaction()
    await tx.start()
    await locker.execute("SELECT 1 FROM visits.visits WHERE id=$1 FOR UPDATE", vid)
    a = asyncio.create_task(client.post(f"/api/visits/{vid}/complete"))
    b = asyncio.create_task(client.post(f"/api/visits/{vid}/complete"))
    try:
        await wait_for_lock_waiters(monitor, 2)
    finally:
        await tx.commit()
        await locker.close()
        await monitor.close()
    assert sorted(r.status_code for r in await asyncio.gather(a, b)) == [200, 409]
    assert await pool.fetchval("SELECT count(*) FROM visits.outbox WHERE aggregate_id=$1", vid) == 1
