-- Durable event correlation: inbox + wait registrations.
--
-- Canonical event identity is (tenant_id, event_name, producer_event_id):
-- the unique index makes re-ingestion of the same producer id a safe
-- no-op. Consumption is one-consumer: the conditional UPDATE from
-- 'pending' guarantees at-most-once even under concurrent matchers.
CREATE TABLE IF NOT EXISTS event_inbox (
    id                UUID PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    event_name        TEXT NOT NULL,
    producer_event_id TEXT NOT NULL,
    correlation_key   TEXT NOT NULL,
    payload           JSONB NOT NULL DEFAULT 'null',
    status            TEXT NOT NULL DEFAULT 'pending',
    consumed_by       UUID,
    received_at       TIMESTAMPTZ NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_event_inbox_identity
    ON event_inbox (tenant_id, event_name, producer_event_id);
-- Correlation lookup: pending events for a (tenant, name, key).
CREATE INDEX IF NOT EXISTS idx_event_inbox_correlation
    ON event_inbox (tenant_id, correlation_key, status);
-- Expiry scans.
CREATE INDEX IF NOT EXISTS idx_event_inbox_received
    ON event_inbox (status, received_at);

CREATE TABLE IF NOT EXISTS event_waits (
    id                UUID PRIMARY KEY,
    tenant_id         TEXT NOT NULL,
    instance_id       UUID NOT NULL,
    block_id          TEXT NOT NULL,
    event_names       JSONB NOT NULL,
    correlation_key   TEXT NOT NULL,
    join_mode         JSONB NOT NULL,
    status            TEXT NOT NULL DEFAULT 'waiting',
    matched_names     JSONB NOT NULL DEFAULT '[]',
    matched_event_ids JSONB NOT NULL DEFAULT '[]',
    created_at        TIMESTAMPTZ NOT NULL
);

-- One wait per (instance, block): re-execution re-registers idempotently.
CREATE UNIQUE INDEX IF NOT EXISTS idx_event_waits_block
    ON event_waits (instance_id, block_id);
-- Matching lookup.
CREATE INDEX IF NOT EXISTS idx_event_waits_correlation
    ON event_waits (tenant_id, correlation_key, status);
