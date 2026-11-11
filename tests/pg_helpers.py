import asyncio


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
