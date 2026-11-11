from uuid import uuid4

import pytest


async def test_complete_in_progress_visit(client, pool, make_visit):
    vid = await make_visit("in_progress")
    r = await client.post(f"/api/visits/{vid}/complete")
    assert r.status_code == 200 and r.json()["status"] == "completed"
    assert await pool.fetchval("SELECT status FROM visits.visits WHERE id=$1", vid) == "completed"
    [row] = await pool.fetch("SELECT * FROM visits.outbox")
    assert row["aggregate_id"] == vid and row["published_at"] is None
    assert row["event_type"] == "visits.visit_completed.v1"
    assert row["payload"]["event_id"] == str(row["id"])
    assert row["payload"]["visit_id"] == str(vid)
    assert row["payload"]["rate"] == "40.00"
    assert row["payload"]["end_date_time"] == "2026-11-11T09:30:00Z"


@pytest.mark.parametrize("status", ["scheduled", "completed", "canceled", "missed"])
async def test_complete_rejects_other_statuses(client, pool, make_visit, status):
    vid = await make_visit(status)
    r = await client.post(f"/api/visits/{vid}/complete")
    assert r.status_code == 409
    assert r.json() == {"detail": f"cannot complete a visit in status '{status}'"}
    assert await pool.fetchval("SELECT status FROM visits.visits WHERE id=$1", vid) == status
    assert await pool.fetchval("SELECT count(*) FROM visits.outbox") == 0


async def test_complete_unknown_visit(client):
    r = await client.post(f"/api/visits/{uuid4()}/complete")
    assert (r.status_code, r.json()) == (404, {"detail": "visit not found"})


async def test_complete_non_uuid_id(client):
    assert (await client.post("/api/visits/abc/complete")).status_code == 422


async def test_complete_rolls_back_when_outbox_insert_fails(client, pool, make_visit, monkeypatch):
    async def fail(*args, **kwargs):
        raise RuntimeError("outbox down")

    monkeypatch.setattr("app.visits.service.outbox.insert_event", fail)
    vid = await make_visit("in_progress")
    with pytest.raises(RuntimeError):
        await client.post(f"/api/visits/{vid}/complete")
    assert await pool.fetchval("SELECT status FROM visits.visits WHERE id=$1", vid) == "in_progress"
    assert await pool.fetchval("SELECT count(*) FROM visits.outbox") == 0


async def test_create_and_get_visit(client):
    body = {"start_date_time": "2026-11-11T08:00:00Z", "end_date_time": "2026-11-11T09:30:00Z",
            "rate": "40.00", "status": "in_progress"}
    r = await client.post("/api/visits", json=body)
    assert r.status_code == 201
    got = (await client.get(f"/api/visits/{r.json()['id']}")).json()
    assert (got["rate"], got["status"]) == ("40.00", "in_progress")


async def test_create_visit_rejects_end_before_start(client):
    body = {"start_date_time": "2026-11-11T09:30:00Z", "end_date_time": "2026-11-11T08:00:00Z", "rate": "40.00"}
    assert (await client.post("/api/visits", json=body)).status_code == 422


async def test_create_visit_rejects_completed_status(client):
    body = {"start_date_time": "2026-11-11T08:00:00Z", "end_date_time": "2026-11-11T09:30:00Z",
            "rate": "40.00", "status": "completed"}
    assert (await client.post("/api/visits", json=body)).status_code == 422
