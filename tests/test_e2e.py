import asyncio
from contextlib import asynccontextmanager
from uuid import uuid4

from app.billing.consumer import build_broker
from app.contracts.events import VISITS_TOPIC
from app.shared.kafka import ensure_topic
from app.visits.relay import KafkaPublisher, run_relay


async def wait_for_records(client, visit_id, n=1, timeout=30.0):
    async with asyncio.timeout(timeout):
        while True:
            resp = await client.get("/api/billing-records", params={"visit_id": str(visit_id)})
            records = resp.json()
            if len(records) >= n:
                return records
            await asyncio.sleep(0.2)


@asynccontextmanager
async def running_relay(pool, bootstrap):
    stop = asyncio.Event()
    publisher = KafkaPublisher(bootstrap)
    task = asyncio.create_task(
        run_relay(
            pool, publisher, batch_size=100, poll_interval_s=0.05,
            backoff_initial_s=0.05, backoff_max_s=0.2, stop=stop,
        )
    )
    try:
        yield
    finally:
        stop.set()
        try:
            await asyncio.wait_for(task, 15)
        finally:
            await publisher.close()


async def wait_for_outbox(pool, visit_id, predicate, timeout=30.0):
    async with asyncio.timeout(timeout):
        while True:
            row = await pool.fetchrow(
                "SELECT published_at, attempts FROM visits.outbox WHERE aggregate_id=$1", visit_id
            )
            if row is not None and predicate(row):
                return row
            await asyncio.sleep(0.05)


async def test_completed_visit_is_billed_end_to_end(client, pool, make_visit, kafka_bootstrap):
    await ensure_topic(kafka_bootstrap, VISITS_TOPIC)
    consumer = build_broker(kafka_bootstrap, lambda: pool, group_id=f"billing-{uuid4()}")
    async with consumer:
        await consumer.start()
        async with running_relay(pool, kafka_bootstrap):
            vid = await make_visit("in_progress")
            assert (await client.post(f"/api/visits/{vid}/complete")).status_code == 200
            [rec] = await wait_for_records(client, vid)
            assert rec["amount"] == "60.00"


async def test_event_survives_kafka_outage(client, pool, make_visit, kafka_bootstrap):
    await ensure_topic(kafka_bootstrap, VISITS_TOPIC)
    vid = await make_visit("in_progress")
    assert (await client.post(f"/api/visits/{vid}/complete")).status_code == 200

    # Phase 1, Kafka down: the relay keeps failing and the row stays unpublished.
    async with running_relay(pool, "127.0.0.1:1"):
        await wait_for_outbox(pool, vid, lambda r: r["attempts"] >= 1)
    row = await pool.fetchrow(
        "SELECT published_at, attempts FROM visits.outbox WHERE aggregate_id=$1", vid
    )
    assert row["published_at"] is None and row["attempts"] >= 1

    # Phase 2, Kafka back: the relay publishes the stored event.
    async with running_relay(pool, kafka_bootstrap):
        await wait_for_outbox(pool, vid, lambda r: r["published_at"] is not None)

    # Phase 3: a consumer with a new group only sees the event because of "earliest".
    consumer = build_broker(kafka_bootstrap, lambda: pool, group_id=f"billing-{uuid4()}")
    async with consumer:
        await consumer.start()
        [rec] = await wait_for_records(client, vid)
    assert rec["amount"] == "60.00"
