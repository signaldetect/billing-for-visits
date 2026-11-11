from uuid import uuid4

from app.billing.handler import handle_visit_completed


async def test_list_billing_records(client, pool, make_event):
    e = make_event()
    await handle_visit_completed(pool, e)
    [rec] = (await client.get("/api/billing-records", params={"visit_id": str(e.visit_id)})).json()
    assert (rec["amount"], rec["status"]) == ("60.00", "created")


async def test_list_billing_records_empty_for_unbilled_visit(client):
    resp = await client.get("/api/billing-records", params={"visit_id": str(uuid4())})
    assert resp.status_code == 200
    assert resp.json() == []
