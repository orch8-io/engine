CREATE TABLE IF NOT EXISTS rate_limits (
    id              UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    resource_key    TEXT NOT NULL,
    max_count       INTEGER NOT NULL,
    window_seconds  INTEGER NOT NULL,
    current_count   INTEGER NOT NULL DEFAULT 0,
    window_start    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (tenant_id, resource_key)
);
