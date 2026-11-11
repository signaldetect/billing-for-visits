from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError


async def ensure_topic(bootstrap_servers: str, topic: str, *, partitions: int = 3) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        if topic in await admin.list_topics():
            return
        try:
            await admin.create_topics([NewTopic(topic, partitions, replication_factor=1)])
        except TopicAlreadyExistsError:
            pass
    finally:
        await admin.close()
