from dataclasses import asdict
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, HTTPException

from app.shared.db import get_pool
from app.visits import service
from app.visits.schemas import VisitIn, VisitOut

router = APIRouter(prefix="/api/visits", tags=["visits"])


@router.post("/{visit_id}/complete", response_model=VisitOut)
async def complete_visit(visit_id: UUID, pool: asyncpg.Pool = Depends(get_pool)):
    try:
        visit = await service.complete_visit(pool, visit_id)
    except service.VisitNotFound:
        raise HTTPException(status_code=404, detail="visit not found") from None
    except service.InvalidTransition as exc:
        raise HTTPException(
            status_code=409, detail=f"cannot complete a visit in status '{exc.current}'"
        ) from None
    return asdict(visit)


@router.post("", response_model=VisitOut, status_code=201)
async def create_visit(body: VisitIn, pool: asyncpg.Pool = Depends(get_pool)):
    visit = await service.create_visit(pool, **body.model_dump())
    return asdict(visit)


@router.get("/{visit_id}", response_model=VisitOut)
async def get_visit(visit_id: UUID, pool: asyncpg.Pool = Depends(get_pool)):
    try:
        visit = await service.get_visit(pool, visit_id)
    except service.VisitNotFound:
        raise HTTPException(status_code=404, detail="visit not found") from None
    return asdict(visit)
