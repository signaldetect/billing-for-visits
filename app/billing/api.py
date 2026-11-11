from datetime import datetime
from decimal import Decimal
from typing import Literal
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends
from pydantic import BaseModel

from app.billing import repository
from app.shared.db import get_pool

router = APIRouter(prefix="/api/billing-records", tags=["billing"])


class BillingRecordOut(BaseModel):
    id: UUID
    visit_id: UUID
    amount: Decimal
    status: Literal["created", "reversed"]
    created_at: datetime


@router.get("", response_model=list[BillingRecordOut])
async def list_billing_records(visit_id: UUID, pool: asyncpg.Pool = Depends(get_pool)):
    async with pool.acquire() as conn:
        records = await repository.list_records(conn, visit_id)
    return [dict(r) for r in records]
