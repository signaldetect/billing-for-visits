# Architecture

A modular monolith with two modules, **Visits** and **Billing**. Completing a visit always produces
exactly one billing record, even when Kafka is down, an event arrives twice, or two requests race.

## Overview

```mermaid
flowchart LR
    client(["Client"])

    subgraph V["Visits module"]
        api["api<br/>FastAPI"]
        relay["outbox-relay"]
        vdb[("schema visits<br/>visits · outbox")]
    end

    kafka[["Kafka<br/>visits.events"]]

    subgraph B["Billing module"]
        consumer["billing-consumer<br/>FastStream"]
        bdb[("schema billing<br/>billing_records<br/>processed_events")]
    end

    client -->|"POST /api/visits/{id}/complete"| api
    api -->|"1 tx: UPDATE visit<br/>+ INSERT event"| vdb
    relay -.->|"poll unpublished"| vdb
    relay -->|"publish"| kafka
    kafka -->|"VisitCompleted"| consumer
    consumer -->|"1 tx: INSERT inbox<br/>+ record"| bdb
```

One PostgreSQL database with one schema per module. Neither module touches the other's schema; the
`VisitCompleted` event is the only link between them.

## Visit lifecycle

```mermaid
stateDiagram-v2
    direction LR
    state "In Progress" as InProgress
    [*] --> Scheduled
    Scheduled --> InProgress
    Scheduled --> Canceled
    Scheduled --> Missed
    InProgress --> Completed : POST complete
    Completed --> [*]
```

Only **In Progress → Completed** is implemented. The other arrows are there for context. Completing a
visit in any other status returns `409`.

## Completing a visit

```mermaid
sequenceDiagram
    autonumber
    actor C as Client
    participant API as api
    participant VDB as schema visits
    participant R as outbox-relay
    participant K as Kafka
    participant BC as billing-consumer
    participant BDB as schema billing

    C->>API: POST /api/visits/{id}/complete
    rect rgba(128, 128, 128, 0.12)
        Note over API,VDB: one transaction
        API->>VDB: UPDATE visits SET status = 'completed'<br/>WHERE id = $1 AND status = 'in_progress'
        API->>VDB: INSERT INTO outbox (VisitCompleted)
    end
    API-->>C: 200 OK

    R->>VDB: SELECT unpublished FOR UPDATE SKIP LOCKED
    R->>K: publish (key = visit_id, acks = all)
    K-->>R: ack
    R->>VDB: SET published_at = now()

    K->>BC: VisitCompleted
    rect rgba(128, 128, 128, 0.12)
        Note over BC,BDB: one transaction
        BC->>BDB: INSERT INTO processed_events (event_id)
        BC->>BDB: INSERT INTO billing_records (amount = rate × hours)
    end
    BC->>K: commit offset
```

## When things go wrong

### Two requests at once

```mermaid
sequenceDiagram
    participant A as Request A
    participant DB as visit row
    participant B as Request B
    A->>DB: UPDATE … WHERE status = 'in_progress'
    Note over DB: A holds the row lock
    B->>DB: UPDATE … WHERE status = 'in_progress'
    Note over B,DB: B waits for the lock
    A->>DB: INSERT INTO outbox, COMMIT
    DB-->>A: 1 row → 200
    DB-->>B: WHERE re-checked → 0 rows → 409
```

After A commits, Postgres re-checks B's `WHERE` against the committed row, so only one request writes
a `VisitCompleted` event.

### Kafka is down

```mermaid
sequenceDiagram
    participant O as visits.outbox
    participant R as outbox-relay
    participant K as Kafka
    loop every poll
        R->>O: SELECT unpublished FOR UPDATE SKIP LOCKED
        alt Kafka unavailable
            R-xK: publish
            R->>O: attempts + 1, last_error
            Note over R: back off 1 s → 2 s → … → 30 s
        else Kafka available
            R->>K: publish
            K-->>R: ack
            R->>O: published_at = now()
        end
    end
```

The event waits in the outbox, in the same database as the visit. When Kafka comes back, the relay
publishes it; nothing is lost and no one has to step in.

### An event arrives twice

```mermaid
sequenceDiagram
    participant K as Kafka
    participant BC as billing-consumer
    participant DB as schema billing
    K->>BC: VisitCompleted (event_id E)
    BC->>DB: INSERT INTO processed_events (E) ON CONFLICT DO NOTHING
    alt first delivery
        DB-->>BC: inserted
        BC->>DB: INSERT INTO billing_records
    else redelivery
        DB-->>BC: conflict → skip
    end
    BC->>DB: COMMIT
    BC->>K: commit offset
```

Kafka may deliver an event more than once, but the inbox lets it take effect only once. A partial
unique index on `billing_records` is the last line of defence.

### A message can't be processed

```mermaid
sequenceDiagram
    participant K as Kafka
    participant BC as billing-consumer
    participant DB as schema billing
    K->>BC: message
    alt poison: not JSON, breaks the contract, end ≤ start
        Note over BC: log the error, skip
        BC->>K: commit offset
    else database unavailable
        BC-xDB: INSERT fails
        BC->>K: nack, so the message comes again
    end
```

A message that can never succeed is logged and skipped, so it can't block the partition. Visits never produces one,
because the same bounds apply on both sides. A dead-letter topic is the next step.

## Data model

**Schema `visits`**

```mermaid
erDiagram
    VISITS ||--o{ OUTBOX : emits
    VISITS {
        uuid id PK
        timestamptz start_date_time
        timestamptz end_date_time
        text description
        numeric rate "per hour"
        text status "scheduled, in_progress, completed, canceled, missed"
        timestamptz updated_at
    }
    OUTBOX {
        uuid id PK "event_id"
        bigint seq "publish order"
        uuid aggregate_id "visit id"
        text event_type
        jsonb payload
        timestamptz created_at
        timestamptz published_at "NULL until published"
        int attempts
        text last_error
    }
```

**Schema `billing`**

```mermaid
erDiagram
    BILLING_RECORDS {
        uuid id PK
        uuid visit_id "no FK, unique where status = created"
        numeric amount
        text status "created, reversed"
        timestamptz created_at
    }
    PROCESSED_EVENTS {
        uuid event_id PK
        timestamptz processed_at
    }
```

`billing_records.visit_id` refers to a visit by its ID only. There is no foreign key between the modules.

## Event contract

Topic `visits.events`, key `visit_id`, headers `event_id` and `event_type`.

```json
{
  "event_id": "4f0c9a52-…",
  "event_type": "visits.visit_completed.v1",
  "occurred_at": "2026-11-11T10:00:00Z",
  "visit_id": "7e1b3d08-…",
  "start_date_time": "2026-11-11T08:00:00Z",
  "end_date_time": "2026-11-11T09:30:00Z",
  "rate": "40.00"
}
```

Billing computes the amount: 1.5 h × 40.00 = **60.00**. The `rate` travels as a string, so the
Decimal stays exact.

## Guarantees

| The task requires | Mechanism |
|---|---|
| A completed visit always gets a billing record, even if the broker is down | Transactional outbox; the relay retries with backoff; the consumer commits its offset only after its database commit |
| No second billing record when an event is delivered twice | Inbox (`processed_events`) written in the same transaction; partial unique index |
| No second billing record when two requests race | Conditional `UPDATE … WHERE status = 'in_progress'`, so there is one event per visit |
| Modules are separate and talk only through events | One schema per module; no foreign key between them; an import-linter contract |

## Key decisions

| Decision | Why |
|---|---|
| The API never connects to Kafka | Visits can still be completed while Kafka is down |
| `AckPolicy.NACK_ON_ERROR` | FastStream's default, `ACK_FIRST`, commits the offset before processing (at-most-once) |
| `auto_offset_reset = "earliest"` | Otherwise a new consumer group skips events published before it joined |
| No foreign key between modules | An FK is a hidden read, and a lock, on the other module's table |
| A transition table instead of the GoF State pattern | There is one transition, and the atomic `UPDATE` is the real guard |
| Duration in integer microseconds, converted to `Decimal` | Keeps floating-point rounding out of money |
