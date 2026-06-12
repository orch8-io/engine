-- Worker registry: one row per (worker_id, handler_name) pair, upserted on
-- every poll. Powers GET /workers (fleet liveness, version skew) and
-- GET /handlers (which handler names are served by external workers).
--
-- last_seen_at — bumped on every poll for the pair; liveness = recency.
-- version      — optional worker-reported build/deploy version string.
CREATE TABLE IF NOT EXISTS worker_registrations (
    worker_id    TEXT NOT NULL,
    handler_name TEXT NOT NULL,
    queue_name   TEXT,
    version      TEXT,
    tenant_id    TEXT,
    last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (worker_id, handler_name)
);

CREATE INDEX IF NOT EXISTS idx_worker_registrations_last_seen
    ON worker_registrations (last_seen_at);
