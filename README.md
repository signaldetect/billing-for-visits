# Billing for visits

A modular monolith with two modules, **Visits** and **Billing**, on FastAPI, PostgreSQL and Kafka.
One business operation, `POST /api/visits/{id}/complete`, completes a visit and produces exactly one
billing record. This holds when Kafka is down, when an event arrives twice, and when two requests race.
The modules talk only through a `VisitCompleted` event (transactional outbox, relay, Kafka, idempotent consumer).

Diagrams, data model and key decisions: [docs/design/2026-11-11-architecture.md](docs/design/2026-11-11-architecture.md).

## Run it

Needs Docker with Compose.

```bash
docker compose up -d --build
```

| Service | Role |
|---|---|
| `postgres`, `kafka` | Infrastructure |
| `migrate-visits`, `migrate-billing` | Apply each module's migrations, then exit |
| `seed` | Inserts five visits, then exits |
| `api` | FastAPI on port 8000 |
| `outbox-relay` | Publishes outbox rows to Kafka |
| `billing-consumer` | Reads `visits.events`, writes billing records |

Seed visits `00000000-0000-0000-0000-00000000000N`, all on 2026-11-11, 08:00 to 09:30 UTC, rate 40.00
(so the amount is 60.00):

| N | Status |
|---|---|
| 1 | `in_progress` |
| 2 | `scheduled` |
| 3 | `completed` |
| 4 | `canceled` |
| 5 | `missed` |

Try it:

```bash
V=00000000-0000-0000-0000-000000000001
curl -s -XPOST localhost:8000/api/visits/$V/complete      # 200, status "completed"
curl -s -XPOST localhost:8000/api/visits/$V/complete      # 409
sleep 3                                                  # billing is asynchronous
curl -s "localhost:8000/api/billing-records?visit_id=$V"  # one record, amount "60.00"
```

Now stop Kafka and complete another visit. Create one first (dev endpoint):

```bash
docker compose stop kafka
curl -s -XPOST localhost:8000/api/visits -H 'content-type: application/json' \
  -d '{"start_date_time":"2026-11-11T08:00:00Z","end_date_time":"2026-11-11T09:30:00Z","rate":"40.00","status":"in_progress"}'
curl -s -XPOST localhost:8000/api/visits/<id from the response>/complete   # 200 while Kafka is down
docker compose exec postgres psql -U app -c "SELECT attempts, published_at FROM visits.outbox ORDER BY seq"
docker compose start kafka       # within about a minute the billing record appears
curl -s "localhost:8000/api/billing-records?visit_id=<id>"
```

The `psql` query shows the new event still unpublished (`published_at` empty). `attempts` rises once a publish attempt fails.

Other dev endpoints: `POST /api/visits`, `GET /api/visits/{id}`. Stop everything with `docker compose down -v`.

## Test it

```bash
uv run pytest        # needs only Docker: testcontainers start Postgres and Kafka
uv run lint-imports  # 2 contracts
```

The two import contracts: Visits and Billing do not import each other; the API never imports
`faststream` or `aiokafka`.

Notes:

- Python is pinned to 3.12 (`.python-version`), the same as the image.
- `tests/conftest.py` defaults `TESTCONTAINERS_DOCKER_SOCKET_OVERRIDE=/var/run/docker.sock`, which Docker Desktop for Mac needs.
  On rootless Docker (Linux), export the real socket path instead, or set `TESTCONTAINERS_RYUK_DISABLED=true`.

## Guarantees

| The task requires | Mechanism | Proved by |
|---|---|---|
| A completed visit always gets a billing record, even if the broker is down | Transactional outbox; the relay retries with backoff; the consumer commits its offset only after its database commit | `tests/test_e2e.py::test_event_survives_kafka_outage`, `tests/test_e2e.py::test_completed_visit_is_billed_end_to_end`, `tests/test_e2e.py::test_failed_delivery_is_redelivered`, `tests/visits/test_relay.py::test_run_relay_retries_until_publish_succeeds`, `tests/visits/test_complete.py::test_complete_rolls_back_when_outbox_insert_fails` |
| No second billing record when an event is delivered twice | Inbox (`processed_events`) written in the same transaction; partial unique index | `tests/billing/test_handler.py::test_redelivery_is_skipped`, `tests/billing/test_handler.py::test_concurrent_duplicate_deliveries_bill_once`, `tests/billing/test_handler.py::test_second_event_for_same_visit_is_skipped`, `tests/billing/test_consumer.py::test_duplicate_message_bills_once`, `tests/test_schema.py::test_one_created_billing_record_per_visit` |
| No second billing record when two requests race | Conditional `UPDATE ... WHERE status = 'in_progress'`, so there is one event per visit | `tests/visits/test_concurrency.py::test_two_concurrent_completes_emit_one_event` |
| Modules are separate and talk only through events | One schema per module; no foreign key between them; an import-linter contract | `uv run lint-imports`; the events travel in `tests/test_e2e.py::test_completed_visit_is_billed_end_to_end` |

## Assumptions

- Only `in_progress` can be completed. Any other status gives `409`, an unknown id gives `404`.
- Amount = rate x scheduled hours, rounded half-up to cents, in one currency.
- The status changes in the request transaction. Billing follows later, asynchronously.
- Visits come from the seed and the dev endpoints.
- No time-based checks (a visit is not compared with the current time).
- Status values are snake_case.
- The producer uses `acks="all"` without `enable_idempotence`. aiokafka never expires idempotent batches,
  so a Kafka outage in mid-connection would hang the relay. The billing inbox dedupes by `event_id`,
  so delivery is still effectively once.
- Poison messages are logged and skipped, never retried:
  - non-JSON or deeply nested bodies;
  - payloads that fail the contract;
  - domain-invalid events: end <= start, or a rate outside `numeric(10,2)`;
  - deterministic database data errors (`asyncpg.DataError`).

  Infrastructure errors still propagate, so Kafka redelivers them. The subscriber gets the raw body
  through a pass-through decoder.
- The relay never stops on errors. Any failed pass, including Postgres being unavailable, backs off and
  retries. `restart: unless-stopped` in Compose covers a relay or consumer that starts while Postgres is down.

## What's left

- Retry topics and a dead-letter topic for messages that keep failing (today poison messages are logged and skipped).
- The consumer retries an infrastructure error immediately, with no backoff (`NACK_ON_ERROR` seeks straight back).
- Outbox cleanup or partitioning.
- LISTEN/NOTIFY or Debezium CDC instead of polling.
- Per-module database roles.
- Metrics and tracing.
- An `Idempotency-Key` header.
- Ordering when several relays run (one relay keeps per-visit order).
- The reversal flow. It must keep a redelivered `VisitCompleted` from billing the visit a second time;
  today the partial unique index blocks that.
- `ensure_topic` uses aiokafka's default 40 s admin timeout.
