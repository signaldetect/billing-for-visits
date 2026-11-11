import asyncio
import contextlib
import logging
import signal
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg
from faststream.kafka import KafkaBroker

from app.contracts.events import VISITS_TOPIC
from app.shared.db import create_pool
from app.shared.kafka import ensure_topic
from app.shared.settings import Settings
from app.visits import outbox

logger = logging.getLogger(__name__)

Publish = Callable[[dict[str, Any], UUID, dict[str, str]], Awaitable[None]]


@dataclass(frozen=True)
class RelayResult:
    published: int
    failed: bool


async def relay_once(pool: asyncpg.Pool, publish: Publish, *, batch_size: int) -> RelayResult:
    async with pool.acquire() as conn, conn.transaction():
        rows = await outbox.fetch_unpublished(conn, batch_size)
        published: list[UUID] = []
        failed = False
        for row in rows:
            headers = {"event_id": str(row["id"]), "event_type": row["event_type"]}
            try:
                await publish(row["payload"], row["aggregate_id"], headers)
            except Exception as exc:
                logger.warning("Publishing outbox event %s failed: %r", row["id"], exc)
                await outbox.record_failure(conn, row["id"], repr(exc))
                failed = True
                break
            published.append(row["id"])
        if published:
            await outbox.mark_published(conn, published)
        return RelayResult(published=len(published), failed=failed)


async def _wait(stop: asyncio.Event, seconds: float) -> None:
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop.wait(), seconds)


async def run_relay(
    pool: asyncpg.Pool,
    publish: Publish,
    *,
    batch_size: int,
    poll_interval_s: float,
    backoff_initial_s: float,
    backoff_max_s: float,
    stop: asyncio.Event,
) -> None:
    backoff = backoff_initial_s
    while not stop.is_set():
        try:
            result = await relay_once(pool, publish, batch_size=batch_size)
        except Exception:
            logger.exception("Relay pass failed")
            result = RelayResult(published=0, failed=True)
        if result.failed:
            await _wait(stop, backoff)
            backoff = min(backoff * 2, backoff_max_s)
            continue
        backoff = backoff_initial_s
        if result.published < batch_size:
            await _wait(stop, poll_interval_s)


class KafkaPublisher:
    def __init__(
        self, bootstrap_servers: str, topic: str = VISITS_TOPIC, *, request_timeout_ms: int = 10_000
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._request_timeout_ms = request_timeout_ms
        self._broker: KafkaBroker | None = None

    async def __call__(self, payload: dict[str, Any], key: UUID, headers: dict[str, str]) -> None:
        try:
            if self._broker is None:
                await ensure_topic(self._bootstrap_servers, self._topic)
                # No idempotence: with it aiokafka retries a batch forever, so a publish never fails
                # during an outage; billing dedups by event_id anyway.
                self._broker = KafkaBroker(
                    self._bootstrap_servers,
                    acks="all",
                    request_timeout_ms=self._request_timeout_ms,
                )
                await self._broker.start()
            await self._broker.publish(
                payload, self._topic, key=str(key).encode(), headers=headers
            )
        except BaseException:
            await self.close()
            raise

    async def close(self) -> None:
        broker, self._broker = self._broker, None
        if broker is not None:
            try:
                await broker.stop()
            except Exception:
                logger.exception("Stopping Kafka broker failed")


async def _run(settings: Settings) -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    pool = await create_pool(settings.database_url)
    publisher = KafkaPublisher(settings.kafka_bootstrap_servers)
    try:
        await run_relay(
            pool,
            publisher,
            batch_size=settings.relay_batch_size,
            poll_interval_s=settings.relay_poll_interval_s,
            backoff_initial_s=settings.relay_backoff_initial_s,
            backoff_max_s=settings.relay_backoff_max_s,
            stop=stop,
        )
    finally:
        await publisher.close()
        await pool.close()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(_run(Settings()))


if __name__ == "__main__":
    main()
