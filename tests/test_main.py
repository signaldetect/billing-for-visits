from uuid import uuid4

import httpx

from app.main import create_app
from app.shared.settings import Settings


async def test_app_starts_with_its_own_pool(postgres_dsn):
    app = create_app(Settings(database_url=postgres_dsn))
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/api/billing-records", params={"visit_id": str(uuid4())})
    assert resp.status_code == 200
