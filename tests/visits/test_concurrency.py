import asyncio

import asyncpg


async def wait_for_lock_waiters(conn, n, timeout=5.0):
    """Poll pg_stat_activity until n backends wait on a lock."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        waiting = await conn.fetchval(
            "SELECT count(*) FROM pg_stat_activity "
            "WHERE datname = current_database() AND wait_event_type = 'Lock'"
        )
        if waiting >= n:
            return
        if asyncio.get_running_loop().time() > deadline:
            raise TimeoutError(f"expected {n} lock waiters, saw {waiting}")
        await asyncio.sleep(0.02)


async def test_two_concurrent_completes_emit_one_event(client, pool, make_visit, postgres_dsn):
    vid = await make_visit("in_progress")
    locker = await asyncpg.connect(postgres_dsn)
    # A separate connection does the polling: polling through the connection that
    # holds the row lock starves the pool's second connection from opening.
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
