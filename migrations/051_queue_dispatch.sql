-- Per-queue dispatch config: poll (default) or push. In push mode the engine
-- POSTs a signed task envelope to push_url at enqueue instead of waiting for a
-- worker to poll. Keyed by (tenant_id, queue_name).
CREATE TABLE IF NOT EXISTS queue_dispatch (
    tenant_id  TEXT NOT NULL,
    queue_name TEXT NOT NULL,
    mode       TEXT NOT NULL DEFAULT 'poll',
    push_url   TEXT,
    secret     TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, queue_name)
);
