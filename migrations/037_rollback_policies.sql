-- Error budget / auto-rollback policies and history.

CREATE TABLE IF NOT EXISTS rollback_policies (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    sequence_name TEXT NOT NULL,
    error_rate_threshold REAL NOT NULL DEFAULT 0.05,
    time_window_secs INTEGER NOT NULL DEFAULT 300,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(tenant_id, sequence_name)
);

CREATE INDEX IF NOT EXISTS idx_rollback_policies_tenant ON rollback_policies(tenant_id);
CREATE INDEX IF NOT EXISTS idx_rollback_policies_enabled ON rollback_policies(enabled);

CREATE TABLE IF NOT EXISTS rollback_history (
    id BIGSERIAL PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    sequence_name TEXT NOT NULL,
    triggered_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    error_rate REAL NOT NULL,
    threshold REAL NOT NULL,
    previous_manifest_version TEXT,
    reason TEXT NOT NULL DEFAULT 'threshold_breach',
    alert_sent INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_rollback_history_tenant ON rollback_history(tenant_id, sequence_name);
CREATE INDEX IF NOT EXISTS idx_rollback_history_triggered ON rollback_history(triggered_at);
