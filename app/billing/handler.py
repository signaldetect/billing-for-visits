import logging

import asyncpg

from app.billing import repository
from app.billing.pricing import calculate_amount
from app.contracts.events import VisitCompletedV1

logger = logging.getLogger(__name__)


async def handle_visit_completed(pool: asyncpg.Pool, event: VisitCompletedV1) -> bool:
    try:
        async with pool.acquire() as conn:
            async with conn.transaction():
                if not await repository.try_mark_processed(conn, event.event_id):
                    return False
                amount = calculate_amount(event.rate, event.start_date_time, event.end_date_time)
                await repository.insert_record(conn, visit_id=event.visit_id, amount=amount)
                return True
    except asyncpg.UniqueViolationError:
        logger.warning(
            "Visit already billed, skipping event %s for visit %s", event.event_id, event.visit_id
        )
        return False
