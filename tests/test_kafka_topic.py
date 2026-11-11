import pytest
from aiokafka.admin import AIOKafkaAdminClient
from aiokafka.errors import KafkaError

from app.shared.kafka import ensure_topic


async def test_ensure_topic_is_idempotent(kafka_bootstrap):
    await ensure_topic(kafka_bootstrap, "visits.events")
    await ensure_topic(kafka_bootstrap, "visits.events")
    admin = AIOKafkaAdminClient(bootstrap_servers=kafka_bootstrap)
    await admin.start()
    try:
        topics = await admin.list_topics()
    finally:
        await admin.close()
    assert "visits.events" in topics


async def test_ensure_topic_raises_on_creation_error(kafka_bootstrap):
    with pytest.raises(KafkaError):
        await ensure_topic(kafka_bootstrap, "bad topic!")
