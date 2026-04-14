-- Append-only audit log for state transitions and lifecycle events.
CREATE TABLE IF NOT EXISTS audit_log (
    id              UUID PRIMARY KEY,
    instance_id     UUID NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    tenant_id       TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    from_state      TEXT,
    to_state        TEXT,
    block_id        TEXT,
    details         JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_instance ON audit_log(instance_id, created_at);
CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON audit_log(tenant_id, created_at);
