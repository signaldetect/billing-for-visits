-- migrate:up
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

-- migrate:down
DROP TABLE visits.visits;
DROP SCHEMA visits;
