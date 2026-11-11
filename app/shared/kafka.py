from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import TopicAlreadyExistsError, for_code


async def ensure_topic(bootstrap_servers: str, topic: str, *, partitions: int = 3) -> None:
    admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
    await admin.start()
    try:
        if topic in await admin.list_topics():
            return
        response = await admin.create_topics(
            [NewTopic(topic, partitions, replication_factor=1)]
        )
        # aiokafka reports per-topic failures in the response instead of raising.
        for name, code, message, *_ in response.topic_errors:
            if code and code != TopicAlreadyExistsError.errno:
                raise for_code(code)(f"Could not create topic {name}: {message}")
    finally:
        await admin.close()
