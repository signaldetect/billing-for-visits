-- migrate:up
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

-- migrate:down
DROP TABLE billing.processed_events;
DROP TABLE billing.billing_records;
DROP SCHEMA billing;
