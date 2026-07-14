CREATE TABLE IF NOT EXISTS compensation_runs (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    continuity_id UUID NOT NULL,
    state TEXT NOT NULL,
    version BIGINT NOT NULL CHECK (version >= 0),
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    FOREIGN KEY (tenant_id, continuity_id)
        REFERENCES continuity_executions(tenant_id, continuity_id)
);

CREATE INDEX IF NOT EXISTS idx_compensation_runs_execution
    ON compensation_runs (tenant_id, continuity_id, created_at DESC, id DESC);
