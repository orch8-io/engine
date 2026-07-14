CREATE TABLE IF NOT EXISTS incident_reproductions (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    fingerprint TEXT NOT NULL,
    record JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_incident_reproductions_group
    ON incident_reproductions (tenant_id, fingerprint, created_at DESC, id DESC);
