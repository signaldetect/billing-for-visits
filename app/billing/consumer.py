from collections.abc import Callable

import asyncpg
from faststream import AckPolicy, FastStream
from faststream.kafka import KafkaBroker, KafkaMessage

from app.billing.handler import process_visits_message
from app.contracts.events import VISITS_TOPIC
from app.shared.db import create_pool
from app.shared.kafka import ensure_topic
from app.shared.settings import Settings


def build_broker(
    bootstrap_servers: str, get_pool: Callable[[], asyncpg.Pool], *, group_id: str = "billing"
) -> KafkaBroker:
    broker = KafkaBroker(bootstrap_servers)

    @broker.subscriber(
        VISITS_TOPIC,
        group_id=group_id,
        ack_policy=AckPolicy.NACK_ON_ERROR,
        auto_offset_reset="earliest",
    )
    async def on_visits_event(msg: KafkaMessage) -> None:
        await process_visits_message(get_pool(), msg.body, msg.headers)

    return broker


def create_app(settings: Settings | None = None) -> FastStream:
    settings = settings or Settings()
    pools: list[asyncpg.Pool] = []
    app = FastStream(
        build_broker(settings.kafka_bootstrap_servers, lambda: pools[0])
    )

    @app.on_startup
    async def _startup() -> None:
        pools.append(await create_pool(settings.database_url))
        await ensure_topic(settings.kafka_bootstrap_servers, VISITS_TOPIC)

    @app.after_shutdown
    async def _shutdown() -> None:
        while pools:
            await pools.pop().close()

    return app
