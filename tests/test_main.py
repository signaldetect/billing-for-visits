from uuid import uuid4

from starlette.testclient import TestClient

from app.main import create_app
from app.shared.settings import Settings


def test_app_starts_with_its_own_pool(postgres_dsn):
    with TestClient(create_app(Settings(database_url=postgres_dsn))) as client:
        assert client.get("/api/billing-records", params={"visit_id": str(uuid4())}).status_code == 200
