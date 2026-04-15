-- Sessions for cross-instance shared state.
CREATE TABLE IF NOT EXISTS sessions (
    id              UUID PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    session_key     TEXT NOT NULL,
    data            JSONB NOT NULL DEFAULT '{}',
    state           TEXT NOT NULL DEFAULT 'active',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at      TIMESTAMPTZ
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_sessions_tenant_key ON sessions(tenant_id, session_key);

-- Add session_id and parent_instance_id to task_instances.
ALTER TABLE task_instances ADD COLUMN IF NOT EXISTS session_id UUID REFERENCES sessions(id);
ALTER TABLE task_instances ADD COLUMN IF NOT EXISTS parent_instance_id UUID REFERENCES task_instances(id);

CREATE INDEX IF NOT EXISTS idx_instances_session ON task_instances(session_id) WHERE session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_instances_parent ON task_instances(parent_instance_id) WHERE parent_instance_id IS NOT NULL;
