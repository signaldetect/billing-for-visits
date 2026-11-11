from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import asyncpg
from fastapi import FastAPI

from app.billing.api import router as billing_router
from app.shared.db import create_pool
from app.shared.settings import Settings
from app.visits.api import router as visits_router


def create_app(settings: Settings | None = None, *, pool: asyncpg.Pool | None = None) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if pool is not None:
            yield
            return
        app.state.pool = await create_pool((settings or Settings()).database_url)
        try:
            yield
        finally:
            await app.state.pool.close()

    app = FastAPI(title="Billing for visits", lifespan=lifespan)
    if pool is not None:
        app.state.pool = pool
    app.include_router(visits_router)
    app.include_router(billing_router)
    return app
