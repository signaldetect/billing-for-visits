-- migrate:up
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

-- migrate:down
DROP TABLE visits.outbox;
