# billing-for-visits Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `POST /api/visits/{id}/complete` completes a visit, and through Kafka the visit gets exactly one billing
record. This must hold even when Kafka is down, an event arrives twice, or two requests race.

**Architecture:** One codebase runs as three processes (`api`, `outbox-relay`, `billing-consumer`) against one PostgreSQL
database with one schema per module.
- **Visits** writes the status change and a `VisitCompleted` row to its outbox in one transaction.
- **The relay** publishes outbox rows to Kafka.
- **Billing** consumes them idempotently through an inbox table.

The two modules share only the event contract (`app.contracts`) and infrastructure helpers (`app.shared`).

**Tech Stack:** Python 3.12, FastAPI, asyncpg with plain SQL, Pydantic 2, FastStream 0.7 (Kafka via aiokafka),
PostgreSQL 16, Kafka 3.9 (KRaft), dbmate, docker-compose, pytest with pytest-asyncio and testcontainers, uv.

**Spec:** `docs/design/2026-11-11-architecture.md`.

## Deviations during implementation

The code and the README supersede this plan where they differ.

- `faststream[kafka,cli]` instead of `faststream[kafka]`: the `billing-consumer` container runs `faststream run`.
- A pass-through subscriber decoder for the raw body, so a body that is not JSON reaches `process_visits_message` and is
  skipped instead of failing in FastStream and being redelivered forever.
- Contract bounds on `rate` (`max_digits=10, decimal_places=2`). Domain-invalid events (end <= start, or a rate outside
  `numeric(10,2)`) and `asyncpg.DataError` are treated as poison and skipped.
- `run_relay` catches any failed pass, including Postgres being unavailable, and backs off.
- `ensure_topic` raises on topic creation errors, other than "already exists".
- The concurrency test polls `pg_stat_activity` from a separate autocommit monitor connection, unlike the poll on the
  locker connection in the Task 4 step: the locker's transaction snapshots `pg_stat_activity` once.
- `.python-version` pins Python 3.12, and `tests/conftest.py` defaults the testcontainers socket override.
- `amount numeric(18,2)` instead of `numeric(12,2)`, so the column cannot overflow.
- `VisitIn` rejects `completed`; a visit becomes completed only through the complete endpoint.
- A redelivery e2e test, `test_failed_delivery_is_redelivered`, pins `AckPolicy.NACK_ON_ERROR`.
- Seed visit `…06`, `in_progress`, for the README's Kafka outage demo.
- The concurrent duplicate-delivery test gates `insert_record` until a monitor connection sees the second delivery
  waiting on the first one's inbox row, so the two deliveries always overlap.
- The `postgres` service publishes no host port; the README reaches it through `docker compose exec`.

## Global Constraints

- **Versions:** Python `>=3.12`.
  - Runtime dependencies: `fastapi>=0.162`, `uvicorn[standard]>=0.54`, `asyncpg>=0.32`, `pydantic>=2.14`,
    `pydantic-settings>=2.16`, `faststream[kafka]>=0.7.8`.
  - Dev dependencies: `pytest>=9.2`, `pytest-asyncio>=1.4`, `httpx>=0.28`, `testcontainers[postgres,kafka]>=4.15`,
    `import-linter>=2.16`.
  - uv project with `[tool.uv] package = false`.
- **Module boundary:** `app.visits` and `app.billing` never import each other; both may import `app.contracts` and `app.shared`.
  SQL in `app/visits` names only schema `visits`, and SQL in `app/billing` names only schema `billing`.
- **No foreign key between schemas.**
- **Money:** `NUMERIC` in SQL and `Decimal` in Python, never `float`. Columns are `rate numeric(10,2)` and `amount numeric(12,2)`.
  Amounts are rounded to 2 places with `ROUND_HALF_UP`.
- **Time:** `timestamptz` in SQL, timezone-aware `datetime` in Python.
- **Status values:**
  - Visit: `scheduled`, `in_progress`, `completed`, `canceled`, `missed`.
  - Billing record: `created`, `reversed`.
- **Completion rule:** only `in_progress` → `completed`. Any other status → `409`; an unknown id → `404`.
- **Kafka:**
  - Message format:
    - topic `visits.events`;
    - key = the visit id's string form as UTF-8 bytes;
    - headers `event_id` and `event_type`;
    - event type `visits.visit_completed.v1`.
  - Producer: `acks="all"`, no idempotence (aiokafka retries idempotent batches forever, so a publish would hang during an outage; the consumer dedups by `event_id`).
  - Consumer: `group_id="billing"`, `ack_policy=AckPolicy.NACK_ON_ERROR`, `auto_offset_reset="earliest"`.
- **Kafka stays out of the `api` process.** It never connects to Kafka; nothing it imports touches `faststream` or `aiokafka`.
- **Tests:** `uv run pytest` needs only Docker.
- **Commits:** a one-line imperative message, with no body and no trailer.

## Review Focus

These are failure modes the spec implies but no happy-path test exercises.

1. **The relay starts while Kafka is down.** It records the failure and keeps retrying; it never crashes. Owned by Task 7:
   `test_kafka_publisher_raises_when_kafka_unreachable` and `test_run_relay_retries_until_publish_succeeds`.
2. **A poison message arrives on `visits.events`:** a non-JSON body, an invalid payload, or an unknown event type. It is logged and
   skipped, and it never blocks the partition. Owned by Task 6.
3. **A path id that is not a UUID** returns `422`, never `500`. Owned by Task 3.
4. **The outbox insert fails after the status UPDATE.** The transaction rolls back and the visit stays `in_progress`. Owned by Task 3.
5. **Start and end carry different UTC offsets.** The amount uses the absolute duration. Owned by Task 5.

---

## File Structure

```
pyproject.toml              uv project, dependencies, pytest + import-linter config
Dockerfile                  one image for api, outbox-relay, billing-consumer
docker-compose.yml          postgres, kafka, migrate-visits, migrate-billing, seed, api, outbox-relay, billing-consumer
db/visits/migrations/       dbmate SQL for schema visits
db/billing/migrations/      dbmate SQL for schema billing
db/seed.sql                 five demo visits, one per status
app/main.py                 FastAPI factory; mounts both modules' routers
app/shared/settings.py      Settings (env prefix APP_)
app/shared/db.py            asyncpg pool factory, get_pool dependency
app/shared/kafka.py         ensure_topic
app/contracts/events.py     VISITS_TOPIC, VISIT_COMPLETED_V1, VisitCompletedV1
app/visits/status.py        VisitStatus, TRANSITIONS, sources_for
app/visits/repository.py    SQL on visits.visits
app/visits/outbox.py        SQL on visits.outbox
app/visits/service.py       complete_visit and its errors
app/visits/schemas.py       VisitIn, VisitOut
app/visits/api.py           /api/visits router
app/visits/relay.py         relay_once, run_relay, KafkaPublisher, main()
app/billing/pricing.py      calculate_amount
app/billing/repository.py   SQL on billing.*
app/billing/handler.py      handle_visit_completed, process_visits_message
app/billing/consumer.py     build_broker, create_app (FastStream)
app/billing/api.py          /api/billing-records router
tests/conftest.py           containers, pool, client, factories
tests/...                   one test file per unit, named in each task
README.md
```

---

### Task 1: Project scaffold, schema and test harness

**Files:**
- Create: `pyproject.toml`, `.gitignore`, `app/__init__.py`, `app/shared/__init__.py`, `app/shared/settings.py`,
  `app/shared/db.py`, `app/contracts/__init__.py`, `app/visits/__init__.py`, `app/billing/__init__.py`
- Create: `db/visits/migrations/20261111000001_create_visits.sql`, `db/visits/migrations/20261111000002_create_outbox.sql`,
  `db/billing/migrations/20261111000001_create_billing.sql`
- Test: `tests/conftest.py`, `tests/test_schema.py`

**Interfaces:**
- Produces:
  - `class Settings(BaseSettings)`, with `env_prefix="APP_"`:
    - `database_url: str = "postgresql://app:app@localhost:5432/app"`
    - `kafka_bootstrap_servers: str = "localhost:9092"`
    - `relay_poll_interval_s: float = 0.5`
    - `relay_batch_size: int = 100`
    - `relay_backoff_initial_s: float = 1.0`
    - `relay_backoff_max_s: float = 30.0`
  - `async def create_pool(dsn: str, *, min_size: int = 1, max_size: int = 10) -> asyncpg.Pool` registers a `jsonb`
    codec (`json.dumps` / `json.loads`, schema `pg_catalog`) on every connection.
  - Fixtures:
    - `postgres_dsn` (session) → `str`.
    - `pool` (function) → `asyncpg.Pool`. All four tables are truncated before each test.
    - `make_visit` (function) → `async (status="in_progress", *, start=2026-11-11T08:00Z, end=2026-11-11T09:30Z,
      rate=Decimal("40.00"), description="Home care") -> UUID`. It inserts straight into `visits.visits`.

- [x] **Step 1: Write `pyproject.toml` and run `uv sync`**
  - Add the dependencies from Global Constraints.
  - pytest config: `asyncio_mode = "auto"`, `asyncio_default_fixture_loop_scope = "function"`,
    `addopts = "--import-mode=importlib"`, `testpaths = ["tests"]`.
  - import-linter config:

```toml
[tool.importlinter]
root_package = "app"

[[tool.importlinter.contracts]]
name = "Visits and Billing are independent"
type = "independence"
modules = ["app.visits", "app.billing"]
```

  Expected: `uv.lock` is created and `.venv/` holds the packages.

- [x] **Step 2: Create the empty packages, `Settings`, and `create_pool`**

  Also add `get_pool(request: Request) -> asyncpg.Pool` in `app/shared/db.py`; it returns `request.app.state.pool`.
  `.gitignore` covers `.venv/`, `__pycache__/` and `.pytest_cache/`.

- [x] **Step 3: Write `tests/conftest.py`**
  - **`postgres_dsn`:** start `PostgresContainer("postgres:16-alpine", driver=None)`, then run
    `asyncio.run(apply_migrations(dsn))`. `apply_migrations` takes the `*.sql` files of `db/visits/migrations`, then
    `db/billing/migrations`, each sorted by name. For each file it executes the text between `-- migrate:up` and `-- migrate:down`.
  - **`pool`:** `create_pool(postgres_dsn)`, then
    `TRUNCATE visits.visits, visits.outbox, billing.billing_records, billing.processed_events`, then yield, then close.

- [x] **Step 4: Write the failing schema tests in `tests/test_schema.py`**

```python
T0 = datetime(2026, 11, 11, 8, tzinfo=UTC)

async def test_visit_rejects_end_not_after_start(pool):
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute("INSERT INTO visits.visits (start_date_time, end_date_time, rate, status) "
                           "VALUES ($1, $1, 40, 'scheduled')", T0)

async def test_visit_rejects_unknown_status(pool):
    with pytest.raises(asyncpg.CheckViolationError):
        await pool.execute("INSERT INTO visits.visits (start_date_time, end_date_time, rate, status) "
                           "VALUES ($1, $1 + interval '1 hour', 40, 'done')", T0)

async def test_one_created_billing_record_per_visit(pool):
    v = uuid4()
    insert = "INSERT INTO billing.billing_records (visit_id, amount, status) VALUES ($1, 10, $2)"
    await pool.execute(insert, v, "reversed")
    await pool.execute(insert, v, "created")
    with pytest.raises(asyncpg.UniqueViolationError):
        await pool.execute(insert, v, "created")

async def test_outbox_payload_roundtrips_as_dict(pool):
    await pool.execute("INSERT INTO visits.outbox (id, aggregate_id, event_type, payload) "
                       "VALUES ($1, $2, 'x', $3)", uuid4(), uuid4(), {"rate": "40.00"})
    assert await pool.fetchval("SELECT payload FROM visits.outbox") == {"rate": "40.00"}
```

- [x] **Step 5: Run the tests and confirm they fail**

  Run: `uv run pytest tests/test_schema.py -v`
  Expected: errors with `relation "visits.visits" does not exist`, because there are no migrations yet.

- [x] **Step 6: Write the three migrations**

  Each file holds `-- migrate:up` with the DDL below and `-- migrate:down` with the matching `DROP`s.

```sql
-- 20261111000001_create_visits.sql
CREATE SCHEMA IF NOT EXISTS visits;
CREATE TABLE visits.visits (
  id              uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  start_date_time timestamptz NOT NULL,
  end_date_time   timestamptz NOT NULL,
  description     text NOT NULL DEFAULT '',
  rate            numeric(10,2) NOT NULL CHECK (rate >= 0),
  status          text NOT NULL CHECK (status IN ('scheduled','in_progress','completed','canceled','missed')),
  updated_at      timestamptz NOT NULL DEFAULT now(),
  CHECK (end_date_time > start_date_time)
);

-- 20261111000002_create_outbox.sql
CREATE TABLE visits.outbox (
  id           uuid PRIMARY KEY,                     -- = event_id
  seq          bigint GENERATED ALWAYS AS IDENTITY,  -- publish order
  aggregate_id uuid NOT NULL,                        -- visit id
  event_type   text NOT NULL,
  payload      jsonb NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz,
  attempts     integer NOT NULL DEFAULT 0,
  last_error   text
);
CREATE INDEX outbox_unpublished_idx ON visits.outbox (seq) WHERE published_at IS NULL;

-- billing: 20261111000001_create_billing.sql
CREATE SCHEMA IF NOT EXISTS billing;
CREATE TABLE billing.billing_records (
  id         uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  visit_id   uuid NOT NULL,                          -- no FK: another module's aggregate
  amount     numeric(12,2) NOT NULL CHECK (amount >= 0),
  status     text NOT NULL DEFAULT 'created' CHECK (status IN ('created','reversed')),
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX billing_records_one_active ON billing.billing_records (visit_id) WHERE status = 'created';
CREATE TABLE billing.processed_events (
  event_id     uuid PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);
```

- [x] **Step 7: Run the tests and the import contract**

  Run: `uv run pytest tests/test_schema.py -v && uv run lint-imports`
  Expected: 4 passed, and `Contracts: 1 kept, 0 broken.`

- [x] **Step 8: Commit**

```bash
git add pyproject.toml uv.lock .gitignore app db tests
git commit -m "Add project scaffold, schema migrations and test harness"
```

---

### Task 2: Event contract and visit transition table

**Files:**
- Create: `app/contracts/events.py`, `app/visits/status.py`
- Test: `tests/test_events.py`, `tests/visits/test_status.py`

**Interfaces:**
- Produces:
  - `VISITS_TOPIC: Final = "visits.events"`
  - `VISIT_COMPLETED_V1: Final = "visits.visit_completed.v1"`
  - `class VisitCompletedV1(BaseModel)`, with `ConfigDict(frozen=True)` and these fields:
    - `event_type: Literal["visits.visit_completed.v1"] = VISIT_COMPLETED_V1`
    - `event_id: UUID`
    - `occurred_at: AwareDatetime`
    - `visit_id: UUID`
    - `start_date_time: AwareDatetime`
    - `end_date_time: AwareDatetime`
    - `rate: Decimal`
  - `class VisitStatus(StrEnum)` with `SCHEDULED="scheduled"`, `IN_PROGRESS="in_progress"`, `COMPLETED="completed"`,
    `CANCELED="canceled"`, `MISSED="missed"`.
  - `TRANSITIONS: Final[Mapping[VisitStatus, frozenset[VisitStatus]]] = {VisitStatus.IN_PROGRESS: frozenset({VisitStatus.COMPLETED})}`
  - `def sources_for(target: VisitStatus) -> list[VisitStatus]` returns, sorted, the statuses that may move to `target`.

- [x] **Step 1: Write the failing tests**

```python
# tests/test_events.py
START, END = datetime(2026, 11, 11, 8, tzinfo=UTC), datetime(2026, 11, 11, 9, 30, tzinfo=UTC)

def test_visit_completed_json_shape():
    e = VisitCompletedV1(event_id=uuid4(), occurred_at=END, visit_id=uuid4(),
                         start_date_time=START, end_date_time=END, rate=Decimal("40.00"))
    data = e.model_dump(mode="json")
    assert data["event_type"] == "visits.visit_completed.v1"
    assert data["rate"] == "40.00"
    assert data["start_date_time"] == "2026-11-11T08:00:00Z"
    assert VisitCompletedV1.model_validate(data) == e

def test_visit_completed_rejects_naive_datetime():
    with pytest.raises(ValidationError):
        VisitCompletedV1(event_id=uuid4(), occurred_at=END, visit_id=uuid4(),
                         start_date_time=START.replace(tzinfo=None), end_date_time=END, rate=Decimal("1"))

# tests/visits/test_status.py
def test_only_in_progress_can_be_completed():
    assert sources_for(VisitStatus.COMPLETED) == [VisitStatus.IN_PROGRESS]

def test_nothing_moves_back_to_scheduled():
    assert sources_for(VisitStatus.SCHEDULED) == []
```

- [x] **Step 2: Run the tests and confirm they fail**

  Run: `uv run pytest tests/test_events.py tests/visits/test_status.py -v`
  Expected: FAIL with `ModuleNotFoundError: No module named 'app.contracts.events'`.

- [x] **Step 3: Implement `app/contracts/events.py` and `app/visits/status.py`**

- [x] **Step 4: Run the tests and confirm they pass**

  Run: `uv run pytest tests/test_events.py tests/visits/test_status.py -v`
  Expected: 4 passed.

- [x] **Step 5: Commit**

```bash
git add app/contracts app/visits/status.py tests
git commit -m "Add VisitCompleted contract and visit transition table"
```

---

### Task 3: Complete-visit endpoint

**Files:**
- Create: `app/visits/repository.py`, `app/visits/outbox.py`, `app/visits/service.py`, `app/visits/schemas.py`,
  `app/visits/api.py`, `app/main.py`
- Modify: `tests/conftest.py` (add the `client` fixture)
- Test: `tests/visits/test_complete.py`

**Interfaces:**
- Consumes:
  - `create_pool` and `get_pool` (Task 1);
  - `VisitStatus` and `sources_for` (Task 2);
  - `VisitCompletedV1` and `VISIT_COMPLETED_V1` (Task 2).
- Produces:
  - **`repository`:**
    - `mark_completed(conn, visit_id: UUID, allowed_from: Sequence[str]) -> asyncpg.Record | None` runs
      `UPDATE visits.visits SET status='completed', updated_at=now() WHERE id=$1 AND status = ANY($2::text[]) RETURNING *`.
    - `get_status(conn, visit_id) -> str | None`
    - `insert_visit(conn, *, start_date_time, end_date_time, description, rate, status) -> asyncpg.Record`
    - `get_visit(conn, visit_id) -> asyncpg.Record | None`
  - **`outbox`:** `insert_event(conn, *, event_id: UUID, aggregate_id: UUID, event_type: str, payload: dict[str, Any]) -> None`.
  - **`service`:**
    - `class VisitNotFound(Exception)`
    - `class InvalidTransition(Exception)` with `current: VisitStatus` and `target: VisitStatus`
    - `@dataclass(frozen=True) class Visit(id, start_date_time, end_date_time, description, rate, status: VisitStatus)`
    - `async def complete_visit(pool: asyncpg.Pool, visit_id: UUID) -> Visit`. It does all its work in one transaction:
      1. Call `mark_completed` with `sources_for(COMPLETED)`.
      2. If no row comes back, call `get_status`, then raise `VisitNotFound` or `InvalidTransition`.
      3. Build a `VisitCompletedV1` with `event_id=uuid4()` and `occurred_at=datetime.now(UTC)`.
      4. Call `outbox.insert_event(..., payload=event.model_dump(mode="json"))`. Call it as a module attribute,
         `from app.visits import outbox`, so the test can patch it.
  - **`schemas`:**
    - `VisitIn` has `start_date_time` and `end_date_time` (both `AwareDatetime`), `description: str = ""`,
      `rate: Decimal` (`ge=0`, `max_digits=10`, `decimal_places=2`) and `status: VisitStatus = SCHEDULED`.
      A model validator rejects `end_date_time <= start_date_time`, which gives a 422.
    - `VisitOut` has `id`, `start_date_time`, `end_date_time`, `description`, `rate` and `status`.
  - **`api`:** `router = APIRouter(prefix="/api/visits", tags=["visits"])` with these routes:
    - `POST /{visit_id}/complete` → 200 `VisitOut`. An unknown visit gives 404 `{"detail": "visit not found"}`.
      An invalid status gives 409 `{"detail": "cannot complete a visit in status '<status>'"}`.
    - `POST ""` (dev) → 201 `VisitOut`.
    - `GET /{visit_id}` (dev) → 200 `VisitOut`, or the same 404.
  - **`main`:** `def create_app(settings: Settings | None = None, *, pool: asyncpg.Pool | None = None) -> FastAPI`.
    If a `pool` is given, it is used and never closed. Otherwise the lifespan opens one from `settings.database_url`
    and closes it. The app mounts the visits router and runs with `uvicorn --factory app.main:create_app`.
  - **Fixture `client`:** `httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(pool=pool)), base_url="http://test")`.

- [x] **Step 1: Write the failing tests in `tests/visits/test_complete.py`**

```python
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
```

- [x] **Step 2: Run the tests and confirm they fail**

  Run: `uv run pytest tests/visits/test_complete.py -v`
  Expected: FAIL with `ModuleNotFoundError: No module named 'app.main'`.

- [x] **Step 3: Implement `repository`, `outbox.insert_event`, `service`, `schemas`, `api` and `main` as specified above**

- [x] **Step 4: Run the tests and confirm they pass**

  Run: `uv run pytest tests/visits/test_complete.py -v && uv run lint-imports`
  Expected: 10 passed, and the contract is kept.

- [x] **Step 5: Commit**

```bash
git add app tests
git commit -m "Add complete-visit endpoint with transactional outbox"
```

---

### Task 4: Concurrency test for two complete requests (bonus)

**Files:**
- Test: `tests/visits/test_concurrency.py`

**Interfaces:**
- Consumes: the fixtures `client`, `pool`, `make_visit` and `postgres_dsn`; the endpoint from Task 3.

- [x] **Step 1: Write the test**

  The test holds the row lock itself. Both requests then queue on the same row, which makes the race deterministic.

```python
async def wait_for_lock_waiters(conn, n, timeout=5.0):
    """Poll pg_stat_activity until n backends wait on a lock."""
    # SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND wait_event_type = 'Lock'
    # every 20 ms; raise TimeoutError after `timeout`.

async def test_two_concurrent_completes_emit_one_event(client, pool, make_visit, postgres_dsn):
    vid = await make_visit("in_progress")
    locker = await asyncpg.connect(postgres_dsn)
    tx = locker.transaction()
    await tx.start()
    await locker.execute("SELECT 1 FROM visits.visits WHERE id=$1 FOR UPDATE", vid)
    a = asyncio.create_task(client.post(f"/api/visits/{vid}/complete"))
    b = asyncio.create_task(client.post(f"/api/visits/{vid}/complete"))
    await wait_for_lock_waiters(locker, 2)
    await tx.commit()
    await locker.close()
    assert sorted(r.status_code for r in await asyncio.gather(a, b)) == [200, 409]
    assert await pool.fetchval("SELECT count(*) FROM visits.outbox WHERE aggregate_id=$1", vid) == 1
```

- [x] **Step 2: Run the test and confirm it passes**

  Run: `uv run pytest tests/visits/test_concurrency.py -v`
  Expected: PASS.

- [x] **Step 3: Prove the test catches the race**

  Temporarily replace ` AND status = ANY($2::text[])` in `mark_completed` with ` AND $2::text[] IS NOT NULL` (deleting it outright leaves `$2` unbound, and asyncpg rejects the call client-side before any request waits on the lock).
  Run: `uv run pytest tests/visits/test_concurrency.py -v`
  Expected: FAIL with `[200, 200] != [200, 409]`.
  Restore the line, run the test again, and expect PASS.

- [x] **Step 4: Commit**

```bash
git add tests/visits/test_concurrency.py
git commit -m "Add concurrency test for two complete requests"
```

---

### Task 5: Billing pricing and idempotent handler

**Files:**
- Create: `app/billing/pricing.py`, `app/billing/repository.py`, `app/billing/handler.py`
- Modify: `tests/conftest.py` (add the `make_event` fixture)
- Test: `tests/billing/test_pricing.py`, `tests/billing/test_handler.py`

**Interfaces:**
- Consumes: `VisitCompletedV1` (Task 2) and `create_pool` (Task 1).
- Produces:
  - **`pricing`:** `def calculate_amount(rate: Decimal, start: datetime, end: datetime) -> Decimal`.
    - It raises `ValueError` if `end <= start`.
    - It counts the duration as integer microseconds, `(end - start) // timedelta(microseconds=1)`.
    - It returns `rate * micros / 3_600_000_000` quantized to `Decimal("0.01")` with `ROUND_HALF_UP`.
  - **`repository`:**
    - `try_mark_processed(conn, event_id: UUID) -> bool` runs `INSERT … ON CONFLICT DO NOTHING RETURNING event_id`.
    - `insert_record(conn, *, visit_id: UUID, amount: Decimal) -> UUID`
    - `list_records(conn, visit_id: UUID) -> list[asyncpg.Record]`
  - **`handler`:** `async def handle_visit_completed(pool: asyncpg.Pool, event: VisitCompletedV1) -> bool`
    returns True when it created a record. It does all its work in one transaction:
    1. Call `try_mark_processed`. If it returns False, return False.
    2. Compute the amount.
    3. Call `insert_record`.

    If the partial unique index raises `asyncpg.UniqueViolationError` (a different event already billed this
    visit), the transaction rolls back, a warning is logged, and the handler returns False.
  - **Fixture `make_event`:** `(**overrides) -> VisitCompletedV1`. Its defaults are a fresh `event_id` and `visit_id`,
    start `2026-11-11T08:00Z`, end `2026-11-11T09:30Z`, and `rate=Decimal("40.00")`.

- [x] **Step 1: Write the failing tests**

```python
# tests/billing/test_pricing.py
T = datetime(2026, 11, 11, 8, tzinfo=UTC)

def test_doc_example():
    assert calculate_amount(Decimal("40.00"), T, T + timedelta(minutes=90)) == Decimal("60.00")

def test_fraction_rounds_to_cents():
    assert calculate_amount(Decimal("10.00"), T, T + timedelta(minutes=80)) == Decimal("13.33")

def test_half_cent_rounds_up():
    assert calculate_amount(Decimal("0.30"), T, T + timedelta(minutes=1)) == Decimal("0.01")

def test_mixed_utc_offsets_use_absolute_time():
    start = datetime(2026, 11, 11, 8, tzinfo=timezone(timedelta(hours=2)))
    end = datetime(2026, 11, 11, 7, 30, tzinfo=UTC)
    assert calculate_amount(Decimal("40.00"), start, end) == Decimal("60.00")

def test_empty_duration_is_rejected():
    with pytest.raises(ValueError):
        calculate_amount(Decimal("40.00"), T, T)

# tests/billing/test_handler.py
async def records(pool, visit_id):
    return await pool.fetch("SELECT amount, status FROM billing.billing_records WHERE visit_id=$1", visit_id)

async def test_first_delivery_creates_record(pool, make_event):
    e = make_event()
    assert await handle_visit_completed(pool, e) is True
    assert [(r["amount"], r["status"]) for r in await records(pool, e.visit_id)] == [(Decimal("60.00"), "created")]
    assert await pool.fetchval("SELECT count(*) FROM billing.processed_events WHERE event_id=$1", e.event_id) == 1

async def test_redelivery_is_skipped(pool, make_event):
    e = make_event()
    await handle_visit_completed(pool, e)
    assert await handle_visit_completed(pool, e) is False
    assert len(await records(pool, e.visit_id)) == 1

async def test_concurrent_duplicate_deliveries_bill_once(pool, make_event):
    e = make_event()
    results = await asyncio.gather(handle_visit_completed(pool, e), handle_visit_completed(pool, e))
    assert sorted(results) == [False, True]
    assert len(await records(pool, e.visit_id)) == 1

async def test_second_event_for_same_visit_is_skipped(pool, make_event):
    first = make_event()
    await handle_visit_completed(pool, first)
    assert await handle_visit_completed(pool, make_event(visit_id=first.visit_id)) is False
    assert len(await records(pool, first.visit_id)) == 1
```

- [x] **Step 2: Run the tests and confirm they fail**

  Run: `uv run pytest tests/billing -v`
  Expected: FAIL with `ModuleNotFoundError: No module named 'app.billing.pricing'`.

- [x] **Step 3: Implement `pricing`, `repository` and `handler` as specified above**

- [x] **Step 4: Run the tests and confirm they pass**

  Run: `uv run pytest tests/billing -v && uv run lint-imports`
  Expected: 9 passed, and the contract is kept.

- [x] **Step 5: Commit**

```bash
git add app/billing tests
git commit -m "Add billing pricing and idempotent VisitCompleted handler"
```

---

### Task 6: Billing consumer and billing-records API

**Files:**
- Create: `app/shared/kafka.py`, `app/billing/consumer.py`, `app/billing/api.py`
- Modify: `app/billing/handler.py` (add `process_visits_message`), `app/main.py` (mount the billing router), and
  `tests/conftest.py` (add the `kafka_bootstrap` fixture)
- Test: `tests/billing/test_consumer.py`, `tests/billing/test_api.py`, `tests/test_kafka_topic.py`

**Interfaces:**
- Consumes: `handle_visit_completed` and `list_records` (Task 5); `VISITS_TOPIC`, `VISIT_COMPLETED_V1` and `VisitCompletedV1` (Task 2).
- Produces:
  - **`shared.kafka`:** `async def ensure_topic(bootstrap_servers: str, topic: str, *, partitions: int = 3) -> None`.
    It uses `AIOKafkaAdminClient`: `list_topics()`, then `create_topics([NewTopic(topic, partitions, replication_factor=1)])`
    if the topic is missing. It ignores `TopicAlreadyExistsError`.
  - **`handler`:** `async def process_visits_message(pool, raw: bytes, headers: Mapping[str, str]) -> bool`.
    - If `headers.get("event_type") != VISIT_COMPLETED_V1`, log at info level and return False.
    - If `json.loads` or `VisitCompletedV1.model_validate` fails, log an error that includes `headers.get("event_id")` and return False.
    - Otherwise return `await handle_visit_completed(pool, event)`.

    It never raises for a bad message. Only infrastructure errors (the database) propagate, and those trigger a redelivery.
  - **`consumer`:**
    - `def build_broker(bootstrap_servers: str, get_pool: Callable[[], asyncpg.Pool], *, group_id: str = "billing") -> KafkaBroker`
      registers `on_visits_event(msg: KafkaMessage) -> None` on `VISITS_TOPIC` with `group_id=group_id`,
      `ack_policy=AckPolicy.NACK_ON_ERROR` and `auto_offset_reset="earliest"`. The handler awaits
      `process_visits_message(get_pool(), msg.body, msg.headers)`.
    - `def create_app(settings: Settings | None = None) -> FastStream`. `on_startup` creates the pool and runs
      `ensure_topic`; `after_shutdown` closes the pool. Run it with `faststream run --factory app.billing.consumer:create_app`.
  - **`billing.api`:** `router = APIRouter(prefix="/api/billing-records", tags=["billing"])` with `GET ""` and the query
    `visit_id: UUID`. It returns a list of `{id, visit_id, amount, status, created_at}`, with `amount` serialized as a string.
  - **Fixture `kafka_bootstrap`** (session): `KafkaContainer("confluentinc/cp-kafka:7.6.0").get_bootstrap_server()`.

- [x] **Step 1: Write the failing tests**

```python
# tests/billing/test_consumer.py
HEADERS = {"event_type": "visits.visit_completed.v1"}

async def publish(pool, body, headers):
    async with TestKafkaBroker(build_broker("localhost:9092", lambda: pool)) as br:
        await br.publish(body, VISITS_TOPIC, headers=headers)

async def count(pool):
    return await pool.fetchval("SELECT count(*) FROM billing.billing_records")

async def test_visit_completed_message_bills_visit(pool, make_event):
    e = make_event()
    await publish(pool, e.model_dump(mode="json"), HEADERS | {"event_id": str(e.event_id)})
    assert await pool.fetchval("SELECT amount FROM billing.billing_records WHERE visit_id=$1", e.visit_id) == Decimal("60.00")

async def test_duplicate_message_bills_once(pool, make_event):
    e = make_event()
    for _ in range(2):
        await publish(pool, e.model_dump(mode="json"), HEADERS)
    assert await count(pool) == 1

async def test_unknown_event_type_is_skipped(pool, make_event):
    await publish(pool, make_event().model_dump(mode="json"), {"event_type": "visits.visit_rescheduled.v1"})
    assert await count(pool) == 0

@pytest.mark.parametrize("body", [b"not json", {"visit_id": "x"}])
async def test_malformed_message_is_skipped(pool, body):
    await publish(pool, body, HEADERS)   # must not raise: TestKafkaBroker re-raises handler errors
    assert await count(pool) == 0

# tests/billing/test_api.py
async def test_list_billing_records(client, pool, make_event):
    e = make_event()
    await handle_visit_completed(pool, e)
    [rec] = (await client.get("/api/billing-records", params={"visit_id": str(e.visit_id)})).json()
    assert (rec["amount"], rec["status"]) == ("60.00", "created")

# tests/test_kafka_topic.py
async def test_ensure_topic_is_idempotent(kafka_bootstrap):
    await ensure_topic(kafka_bootstrap, "visits.events")
    await ensure_topic(kafka_bootstrap, "visits.events")
    # list the topics with AIOKafkaAdminClient
    assert "visits.events" in topics
```

- [x] **Step 2: Run the tests and confirm they fail**

  Run: `uv run pytest tests/billing/test_consumer.py tests/billing/test_api.py tests/test_kafka_topic.py -v`
  Expected: FAIL with `ModuleNotFoundError: No module named 'app.billing.consumer'`.

- [x] **Step 3: Implement the units specified above**

  The units are `ensure_topic`, `process_visits_message`, `build_broker`, `create_app` and `billing.api`.
  Also mount the billing router in `app.main.create_app`.

- [x] **Step 4: Run the tests and confirm they pass**

  Run: `uv run pytest -v && uv run lint-imports`
  Expected: all tests pass, and the contract is kept.

- [x] **Step 5: Commit**

```bash
git add app tests
git commit -m "Add Kafka billing consumer and billing-records endpoint"
```

---

### Task 7: Outbox relay

**Files:**
- Create: `app/visits/relay.py`
- Modify: `app/visits/outbox.py`
- Test: `tests/visits/test_relay.py`

**Interfaces:**
- Consumes: `outbox.insert_event` (Task 3); `ensure_topic` (Task 6); `VISITS_TOPIC` (Task 2); `Settings` and `create_pool` (Task 1).
- Produces:
  - **`outbox`:**
    - `fetch_unpublished(conn, limit: int) -> list[asyncpg.Record]` runs
      `SELECT id, aggregate_id, event_type, payload … WHERE published_at IS NULL ORDER BY seq LIMIT $1 FOR UPDATE SKIP LOCKED`.
    - `mark_published(conn, ids: Sequence[UUID]) -> None`
    - `record_failure(conn, outbox_id: UUID, error: str) -> None` sets `attempts = attempts + 1, last_error = $2`.
  - **`relay`:**
    - `Publish = Callable[[dict[str, Any], UUID, dict[str, str]], Awaitable[None]]`, whose arguments are `(payload, key, headers)`.
    - `@dataclass(frozen=True) class RelayResult(published: int, failed: bool)`
    - `async def relay_once(pool, publish: Publish, *, batch_size: int) -> RelayResult`. It does all its work in one transaction:
      1. Fetch a batch.
      2. Publish the rows in order with headers `{"event_id": str(id), "event_type": event_type}` and key = `aggregate_id`.
      3. At the first exception, call `record_failure` and stop the batch.
      4. Call `mark_published` for the rows that succeeded.
    - `async def run_relay(pool, publish, *, batch_size, poll_interval_s, backoff_initial_s, backoff_max_s, stop: asyncio.Event) -> None`.
      - After a failed pass it waits `backoff`, which doubles from `backoff_initial_s` up to `backoff_max_s`.
      - After a successful pass the backoff resets to `backoff_initial_s`.
      - If nothing was published it waits `poll_interval_s`.
      - After a full batch it loops at once.
      - Every wait is `asyncio.wait_for(stop.wait(), t)`, so setting `stop` ends the loop promptly.
    - `class KafkaPublisher`:
      - `__init__(self, bootstrap_servers: str, topic: str = VISITS_TOPIC, *, request_timeout_ms: int = 10_000)`.
      - `async __call__(payload, key, headers)` connects lazily: `ensure_topic`, then
        `KafkaBroker(bootstrap_servers, acks="all", request_timeout_ms=10_000)` and `await broker.start()`.
        It publishes with `key=str(key).encode()` and the headers. On any error it stops and drops the broker, so the
        next call reconnects from scratch, and then it re-raises.
      - `async close()`.
    - `def main() -> None` builds `Settings()`, a pool and a `KafkaPublisher`, then calls `run_relay` until SIGINT or SIGTERM.
      It is the entry point for `python -m app.visits.relay`.

- [x] **Step 1: Write the failing tests**

```python
class FakePublish:
    def __init__(self, fail_on=(), on_success=None):
        self.calls, self.fail_on, self.on_success = [], set(fail_on), on_success
    async def __call__(self, payload, key, headers):
        n = len(self.calls)
        self.calls.append((payload, key, headers))
        if n in self.fail_on:
            raise RuntimeError("boom")
        if self.on_success:
            self.on_success()

# fixture insert_events(n) -> list[(event_id, visit_id)], inserted in order via outbox.insert_event

async def test_relay_once_publishes_in_order_and_marks(pool, insert_events):
    ids = await insert_events(2)
    fake = FakePublish()
    assert await relay_once(pool, fake, batch_size=100) == RelayResult(published=2, failed=False)
    assert [c[1] for c in fake.calls] == [v for _, v in ids]
    assert fake.calls[0][2] == {"event_id": str(ids[0][0]), "event_type": "visits.visit_completed.v1"}
    assert await pool.fetchval("SELECT count(*) FROM visits.outbox WHERE published_at IS NULL") == 0

async def test_relay_once_stops_at_first_failure(pool, insert_events):
    await insert_events(3)
    assert await relay_once(pool, FakePublish(fail_on={1}), batch_size=100) == RelayResult(published=1, failed=True)
    rows = await pool.fetch("SELECT published_at IS NOT NULL AS pub, attempts, last_error FROM visits.outbox ORDER BY seq")
    assert [(r["pub"], r["attempts"]) for r in rows] == [(True, 0), (False, 1), (False, 0)]
    assert "boom" in rows[1]["last_error"]

async def test_run_relay_retries_until_publish_succeeds(pool, insert_events):
    await insert_events(1)
    stop = asyncio.Event()
    fake = FakePublish(fail_on={0, 1}, on_success=stop.set)
    await asyncio.wait_for(run_relay(pool, fake, batch_size=100, poll_interval_s=0.01, backoff_initial_s=0.01,
                                     backoff_max_s=0.05, stop=stop), timeout=5)
    row = await pool.fetchrow("SELECT published_at, attempts FROM visits.outbox")
    assert row["published_at"] is not None and row["attempts"] == 2

async def test_kafka_publisher_delivers_key_headers_payload(kafka_bootstrap):
    vid = uuid4()
    pub = KafkaPublisher(kafka_bootstrap, topic="relay-test")
    await pub({"visit_id": str(vid)}, vid, {"event_id": "e1", "event_type": "visits.visit_completed.v1"})
    await pub.close()
    # read one message from "relay-test" with AIOKafkaConsumer(auto_offset_reset="earliest", group_id=None), 30 s timeout
    assert msg.key == str(vid).encode()
    assert dict(msg.headers)["event_type"] == b"visits.visit_completed.v1"
    assert json.loads(msg.value) == {"visit_id": str(vid)}

async def test_kafka_publisher_raises_when_kafka_unreachable():
    pub = KafkaPublisher("127.0.0.1:1")
    with pytest.raises(KafkaConnectionError):
        await asyncio.wait_for(pub({}, uuid4(), {}), timeout=30)
```

- [x] **Step 2: Run the tests and confirm they fail**

  Run: `uv run pytest tests/visits/test_relay.py -v`
  Expected: FAIL with `ModuleNotFoundError: No module named 'app.visits.relay'`.

- [x] **Step 3: Implement the `outbox` additions and `relay` as specified above**

- [x] **Step 4: Run the tests and confirm they pass**

  Run: `uv run pytest tests/visits/test_relay.py -v && uv run lint-imports`
  Expected: 5 passed, and the contract is kept.

- [x] **Step 5: Commit**

```bash
git add app/visits tests/visits/test_relay.py
git commit -m "Add outbox relay with retry and backoff"
```

---

### Task 8: Runtime wiring and end-to-end tests

**Files:**
- Create: `Dockerfile`, `.dockerignore`, `docker-compose.yml`, `db/seed.sql`
- Modify: `pyproject.toml` (add the second import-linter contract)
- Test: `tests/test_e2e.py`

**Interfaces:**
- Consumes:
  - `create_app` from `app.main` (Task 3);
  - `build_broker` and `ensure_topic` (Task 6);
  - `run_relay` and `KafkaPublisher` (Task 7).

- [x] **Step 1: Add the import-linter contract that keeps Kafka out of the API**

```toml
[tool.importlinter]
root_package = "app"
include_external_packages = true

[[tool.importlinter.contracts]]
name = "The API never touches Kafka"
type = "forbidden"
source_modules = ["app.main", "app.visits.api", "app.visits.service", "app.billing.api"]
forbidden_modules = ["faststream", "aiokafka"]
```

  Run: `uv run lint-imports`
  Expected: `Contracts: 2 kept, 0 broken.`

- [x] **Step 2: Write the failing end-to-end tests in `tests/test_e2e.py`**

  Each test uses its own consumer `group_id=f"billing-{uuid4()}"`, so offsets never leak between tests.

```python
async def wait_for_records(client, visit_id, n=1, timeout=30.0): ...  # polls GET /api/billing-records every 0.2 s

async def test_completed_visit_is_billed_end_to_end(client, pool, make_visit, kafka_bootstrap):
    # given: ensure_topic; consumer = build_broker(kafka_bootstrap, lambda: pool, group_id=...) started with
    #        `async with consumer: await consumer.start()`; relay task = run_relay(pool, KafkaPublisher(kafka_bootstrap),
    #        batch_size=100, poll_interval_s=0.05, backoff_initial_s=0.05, backoff_max_s=0.2, stop=stop)
    vid = await make_visit("in_progress")
    assert (await client.post(f"/api/visits/{vid}/complete")).status_code == 200
    [rec] = await wait_for_records(client, vid)
    assert rec["amount"] == "60.00"
    # then: stop.set(); await the relay task; close the publisher

async def test_event_survives_kafka_outage(client, pool, make_visit, kafka_bootstrap):
    vid = await make_visit("in_progress")
    await client.post(f"/api/visits/{vid}/complete")
    # phase 1, Kafka down: run_relay with KafkaPublisher("127.0.0.1:1") until attempts >= 1, then stop it
    row = await pool.fetchrow("SELECT published_at, attempts FROM visits.outbox WHERE aggregate_id=$1", vid)
    assert row["published_at"] is None and row["attempts"] >= 1
    # phase 2, Kafka back: run_relay with KafkaPublisher(kafka_bootstrap) until published_at is set, then stop it
    # phase 3: only now start a consumer with a NEW group. Only auto_offset_reset="earliest" lets it see the event.
    [rec] = await wait_for_records(client, vid)
    assert rec["amount"] == "60.00"
```

- [x] **Step 3: Run the tests**

  Run: `uv run pytest tests/test_e2e.py -v`
  Expected: PASS, because every unit already exists. To prove that phase 3 pins `earliest`, temporarily set
  `auto_offset_reset="latest"` in `build_broker` and expect phase 3 to time out (FAIL). Then restore it.

- [x] **Step 4: Write `Dockerfile`, `.dockerignore`, `db/seed.sql` and `docker-compose.yml`**
  - **`Dockerfile`:**
    1. Start from `python:3.12-slim`.
    2. `COPY --from=ghcr.io/astral-sh/uv:0.9 /uv /bin/uv`.
    3. `WORKDIR /srv`.
    4. Copy `pyproject.toml` and `uv.lock`, then `uv sync --frozen --no-dev`.
    5. Copy `app/`.
    6. Set `ENV PATH="/srv/.venv/bin:$PATH"`.
  - **`db/seed.sql`:** five visits, each `2026-11-11T08:00Z`–`09:30Z` at `rate 40.00`, inserted with `ON CONFLICT (id) DO NOTHING`:
    - `00000000-0000-0000-0000-000000000001` is `in_progress`;
    - `…02` is `scheduled`;
    - `…03` is `completed`;
    - `…04` is `canceled`;
    - `…05` is `missed`.
  - **`docker-compose.yml`:**

| Service | Image or build | Command / key settings |
|---|---|---|
| `postgres` | `postgres:16-alpine` | user, password and db all `app`; healthcheck `pg_isready -U app`; port `5432:5432` |
| `kafka` | `apache/kafka:3.9.1` | the env block below; healthcheck `/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092` |
| `migrate-visits` | `ghcr.io/amacneil/dbmate:2` | `--wait --no-dump-schema -d /db/visits/migrations --migrations-table visits.schema_migrations up` |
| `migrate-billing` | `ghcr.io/amacneil/dbmate:2` | same, with `/db/billing/migrations` and `billing.schema_migrations` |
| `seed` | `postgres:16-alpine` | `psql postgresql://app:app@postgres:5432/app -v ON_ERROR_STOP=1 -f /seed/seed.sql`; after both migrations complete |
| `api` | build `.` | `uvicorn --factory app.main:create_app --host 0.0.0.0 --port 8000`; port `8000:8000`; does **not** depend on `kafka` |
| `outbox-relay` | build `.` | `python -m app.visits.relay`; `restart: unless-stopped`; does **not** wait for `kafka` |
| `billing-consumer` | build `.` | `faststream run --factory app.billing.consumer:create_app`; `restart: unless-stopped`; depends on `kafka` (healthy) |

  The dbmate services mount `./db:/db` and use `DATABASE_URL=postgres://app:app@postgres:5432/app?sslmode=disable`.
  The app services use `APP_DATABASE_URL=postgresql://app:app@postgres:5432/app` and `APP_KAFKA_BOOTSTRAP_SERVERS=kafka:9092`.
  Kafka env:

```yaml
KAFKA_NODE_ID: 1
KAFKA_PROCESS_ROLES: broker,controller
KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
```

- [x] **Step 5: Check the whole system by hand**

```bash
docker compose up -d --build
curl -s -XPOST localhost:8000/api/visits/00000000-0000-0000-0000-000000000001/complete   # 200, "completed"
curl -s -XPOST localhost:8000/api/visits/00000000-0000-0000-0000-000000000001/complete   # 409
curl -s "localhost:8000/api/billing-records?visit_id=00000000-0000-0000-0000-000000000001" # one record, "60.00"
docker compose stop kafka
# create an in_progress visit with POST /api/visits, then complete it                     # 200 while Kafka is down
docker compose exec postgres psql -U app -c "SELECT attempts, published_at FROM visits.outbox ORDER BY seq"
docker compose start kafka                                                                # within ~1 min: a record appears
```

  Expected: every comment above holds.

- [x] **Step 6: Run the full suite and commit**

  Run: `uv run pytest -v && uv run lint-imports`
  Expected: all tests pass, and `Contracts: 2 kept, 0 broken.`

```bash
git add Dockerfile .dockerignore docker-compose.yml db/seed.sql pyproject.toml tests/test_e2e.py
git commit -m "Add Docker Compose runtime and end-to-end tests"
```

---

### Task 9: README

**Files:**
- Create: `README.md`

- [x] **Step 1: Write `README.md`** with these sections, keeping the text short and linking the architecture doc
  instead of repeating it:
  1. **What it is.** One paragraph and a link to `docs/design/2026-11-11-architecture.md`.
  2. **Run it.** `docker compose up -d --build` and the curl session from Task 8, Step 5.
  3. **Test it.** `uv run pytest` (needs Docker) and `uv run lint-imports`.
  4. **Guarantees.** The guarantees table from the architecture doc, plus the test that proves each row.
  5. **Assumptions:**
     - only `in_progress` can be completed;
     - amount = rate × scheduled hours, rounded half-up to cents, in one currency;
     - the status changes in the request transaction, and billing follows later;
     - visits come from the seed and dev endpoints;
     - no time-based checks;
     - status values are snake_case.
  6. **What's left:**
     - retry topics and a dead-letter topic for messages that keep failing (today poison messages are logged and skipped);
     - outbox cleanup or partitioning;
     - LISTEN/NOTIFY or Debezium CDC instead of polling;
     - per-module database roles;
     - metrics and tracing;
     - an `Idempotency-Key` header;
     - ordering when several relays run (one relay keeps per-visit order);
     - the reversal flow.

- [x] **Step 2: Check every command in the README against the running stack**

  Run each command from sections 2–3 by hand.
  Expected: the output matches the README.

- [x] **Step 3: Commit**

```bash
git add README.md
git commit -m "Add README"
```
