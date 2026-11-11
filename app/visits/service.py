from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import asyncpg

from app.contracts.events import VISIT_COMPLETED_V1, VisitCompletedV1
from app.visits import outbox, repository
from app.visits.status import VisitStatus, sources_for


class VisitNotFound(Exception):
    pass


class InvalidTransition(Exception):
    def __init__(self, current: VisitStatus, target: VisitStatus) -> None:
        super().__init__(f"cannot move a visit from '{current}' to '{target}'")
        self.current = current
        self.target = target


@dataclass(frozen=True)
class Visit:
    id: UUID
    start_date_time: datetime
    end_date_time: datetime
    description: str
    rate: Decimal
    status: VisitStatus


def to_visit(row: asyncpg.Record) -> Visit:
    return Visit(
        id=row["id"],
        start_date_time=row["start_date_time"],
        end_date_time=row["end_date_time"],
        description=row["description"],
        rate=row["rate"],
        status=VisitStatus(row["status"]),
    )


async def complete_visit(pool: asyncpg.Pool, visit_id: UUID) -> Visit:
    async with pool.acquire() as conn, conn.transaction():
        allowed_from = [s.value for s in sources_for(VisitStatus.COMPLETED)]
        row = await repository.mark_completed(conn, visit_id, allowed_from)
        if row is None:
            current = await repository.get_status(conn, visit_id)
            if current is None:
                raise VisitNotFound(visit_id)
            raise InvalidTransition(VisitStatus(current), VisitStatus.COMPLETED)
        event = VisitCompletedV1(
            event_id=uuid4(),
            occurred_at=datetime.now(UTC),
            visit_id=row["id"],
            start_date_time=row["start_date_time"],
            end_date_time=row["end_date_time"],
            rate=row["rate"],
        )
        await outbox.insert_event(
            conn,
            event_id=event.event_id,
            aggregate_id=event.visit_id,
            event_type=VISIT_COMPLETED_V1,
            payload=event.model_dump(mode="json"),
        )
        return to_visit(row)


async def create_visit(pool: asyncpg.Pool, **fields) -> Visit:
    async with pool.acquire() as conn:
        return to_visit(await repository.insert_visit(conn, **fields))


async def get_visit(pool: asyncpg.Pool, visit_id: UUID) -> Visit:
    async with pool.acquire() as conn:
        row = await repository.get_visit(conn, visit_id)
    if row is None:
        raise VisitNotFound(visit_id)
    return to_visit(row)
