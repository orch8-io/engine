-- Built-in operational alert rules (tenant-scoped) and the evaluator's
-- per-rule state. State is updated with a version compare-and-swap so that
-- exactly one engine node emits each firing/resolve transition. State rows
-- are keyed by rule id without a foreign key because rules declared in the
-- server config have state but no alert_rules row.
CREATE TABLE IF NOT EXISTS alert_rules (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    condition JSONB NOT NULL,
    destination JSONB NOT NULL,
    cooldown_secs BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alert_rules_tenant ON alert_rules (tenant_id, created_at);

CREATE TABLE IF NOT EXISTS alert_rule_state (
    rule_id UUID PRIMARY KEY,
    state JSONB NOT NULL,
    version BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
