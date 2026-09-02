CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);


CREATE TABLE IF NOT EXISTS outbox_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    event_type TEXT NOT NULL,

    aggregate_type TEXT NOT NULL,

    aggregate_id UUID NOT NULL,

    payload JSONB NOT NULL,

    created_at TIMESTAMP NOT NULL DEFAULT NOW(),

    processed_at TIMESTAMP NULL,

    attempts INTEGER NOT NULL DEFAULT 0,

    last_error TEXT NULL
);


CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed
ON outbox_events (created_at)
WHERE processed_at IS NULL;