-- Event-driven trigger definitions (persisted).
CREATE TABLE IF NOT EXISTS triggers (
    slug        TEXT PRIMARY KEY,
    sequence_name TEXT NOT NULL,
    version     INTEGER,
    tenant_id   TEXT NOT NULL,
    namespace   TEXT NOT NULL DEFAULT 'default',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    secret      TEXT,
    trigger_type TEXT NOT NULL DEFAULT 'webhook',
    config      TEXT NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_triggers_tenant ON triggers (tenant_id);
