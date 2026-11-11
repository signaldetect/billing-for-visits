import json
import logging
from collections.abc import Mapping

import asyncpg

from app.billing import repository
from app.billing.pricing import calculate_amount
from app.contracts.events import VISIT_COMPLETED_V1, VisitCompletedV1

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


async def process_visits_message(pool: asyncpg.Pool, raw: bytes, headers: Mapping[str, str]) -> bool:
    """Handle one raw `visits.events` message. Bad messages are logged and skipped (False);
    only infrastructure errors propagate, so the broker redelivers those."""
    if headers.get("event_type") != VISIT_COMPLETED_V1:
        logger.info("Skipping message of event type %r", headers.get("event_type"))
        return False
    try:
        event = VisitCompletedV1.model_validate(json.loads(raw))
    except Exception:  # json.loads can raise ValueError or RecursionError; no I/O in this try
        logger.error("Skipping invalid message, event_id=%s", headers.get("event_id"), exc_info=True)
        return False
    try:
        return await handle_visit_completed(pool, event)
    except asyncpg.DataError:
        # Deterministic data error (SQLSTATE class 22, e.g. amount out of range): redelivery
        # would fail forever, so skip. Other errors (connection etc.) propagate for redelivery.
        logger.error(
            "Skipping event %s for visit %s: data error", event.event_id, event.visit_id,
            exc_info=True,
        )
        return False
