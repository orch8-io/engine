CREATE TABLE IF NOT EXISTS live_migrations (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    state TEXT NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);

CREATE INDEX IF NOT EXISTS idx_live_migrations_execution
    ON live_migrations (tenant_id, continuity_id, created_at DESC, id DESC);
