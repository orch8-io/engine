CREATE TABLE IF NOT EXISTS what_if_runs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);

CREATE INDEX IF NOT EXISTS idx_what_if_runs_execution
    ON what_if_runs (tenant_id, continuity_id, created_at DESC, id DESC);
