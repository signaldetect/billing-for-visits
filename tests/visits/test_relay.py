import asyncio
import json
from uuid import uuid4

import pytest
import pytest_asyncio
from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaConnectionError

from app.contracts.events import VISIT_COMPLETED_V1
from app.visits import outbox, relay
from app.visits.relay import KafkaPublisher, RelayResult, relay_once, run_relay


class FakePublish:
    def __init__(self, fail_on=(), on_success=None):
        self.calls, self.fail_on, self.on_success = [], set(fail_on), on_success

    async def __call__(self, payload, key, headers):
        n = len(self.calls)
        self.calls.append((payload, key, headers))
        if n in self.fail_on:
            raise RuntimeError("boom")
        if self.on_success:
            self.on_success()


@pytest_asyncio.fixture
async def insert_events(pool):
    async def _insert(n):
        ids = []
        for _ in range(n):
            event_id, visit_id = uuid4(), uuid4()
            async with pool.acquire() as conn:
                await outbox.insert_event(
                    conn,
                    event_id=event_id,
                    aggregate_id=visit_id,
                    event_type=VISIT_COMPLETED_V1,
                    payload={"visit_id": str(visit_id)},
                )
            ids.append((event_id, visit_id))
        return ids

    return _insert


async def test_relay_once_publishes_in_order_and_marks(pool, insert_events):
    ids = await insert_events(2)
    fake = FakePublish()
    assert await relay_once(pool, fake, batch_size=100) == RelayResult(published=2, failed=False)
    assert [c[1] for c in fake.calls] == [v for _, v in ids]
    assert fake.calls[0][2] == {"event_id": str(ids[0][0]), "event_type": "visits.visit_completed.v1"}
    assert await pool.fetchval("SELECT count(*) FROM visits.outbox WHERE published_at IS NULL") == 0


async def test_relay_once_stops_at_first_failure(pool, insert_events):
    await insert_events(3)
    assert await relay_once(pool, FakePublish(fail_on={1}), batch_size=100) == RelayResult(published=1, failed=True)
    rows = await pool.fetch("SELECT published_at IS NOT NULL AS pub, attempts, last_error FROM visits.outbox ORDER BY seq")
    assert [(r["pub"], r["attempts"]) for r in rows] == [(True, 0), (False, 1), (False, 0)]
    assert "boom" in rows[1]["last_error"]


async def test_run_relay_retries_until_publish_succeeds(pool, insert_events):
    await insert_events(1)
    stop = asyncio.Event()
    fake = FakePublish(fail_on={0, 1}, on_success=stop.set)
    await asyncio.wait_for(run_relay(pool, fake, batch_size=100, poll_interval_s=0.01, backoff_initial_s=0.01,
                                     backoff_max_s=0.05, stop=stop), timeout=5)
    row = await pool.fetchrow("SELECT published_at, attempts FROM visits.outbox")
    assert row["published_at"] is not None and row["attempts"] == 2


async def test_run_relay_survives_relay_once_raising(pool, insert_events, monkeypatch, caplog):
    await insert_events(1)
    real = relay.relay_once
    raised = []

    async def flaky(pool, publish, *, batch_size):
        if not raised:
            raised.append(True)
            raise ConnectionError("postgres down")
        return await real(pool, publish, batch_size=batch_size)

    monkeypatch.setattr(relay, "relay_once", flaky)
    stop = asyncio.Event()
    fake = FakePublish(on_success=stop.set)
    await asyncio.wait_for(run_relay(pool, fake, batch_size=100, poll_interval_s=0.01, backoff_initial_s=0.01,
                                     backoff_max_s=0.05, stop=stop), timeout=5)
    assert raised and len(fake.calls) == 1
    assert await pool.fetchval("SELECT count(*) FROM visits.outbox WHERE published_at IS NULL") == 0
    assert "postgres down" in caplog.text


async def test_kafka_publisher_delivers_key_headers_payload(kafka_bootstrap):
    vid = uuid4()
    pub = KafkaPublisher(kafka_bootstrap, topic="relay-test")
    await pub({"visit_id": str(vid)}, vid, {"event_id": "e1", "event_type": "visits.visit_completed.v1"})
    await pub.close()
    consumer = AIOKafkaConsumer(
        "relay-test", bootstrap_servers=kafka_bootstrap, auto_offset_reset="earliest", group_id=None
    )
    await consumer.start()
    try:
        msg = await asyncio.wait_for(consumer.getone(), timeout=30)
    finally:
        await consumer.stop()
    assert msg.key == str(vid).encode()
    assert dict(msg.headers)["event_type"] == b"visits.visit_completed.v1"
    assert json.loads(msg.value) == {"visit_id": str(vid)}


async def test_kafka_publisher_raises_when_kafka_unreachable():
    pub = KafkaPublisher("127.0.0.1:1")
    with pytest.raises(KafkaConnectionError):
        await asyncio.wait_for(pub({}, uuid4(), {}), timeout=30)
