CREATE TABLE IF NOT EXISTS sequences (
    id          UUID PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    namespace   TEXT NOT NULL,
    name        TEXT NOT NULL,
    definition  JSONB NOT NULL,
    version     INTEGER NOT NULL DEFAULT 1,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sequences_unique
    ON sequences (tenant_id, namespace, name, version);
