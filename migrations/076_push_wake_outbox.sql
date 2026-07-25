-- Durable push wake attempts correlated to mobile command outcomes.
CREATE TABLE IF NOT EXISTS push_wake_outbox (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    device_id TEXT NOT NULL,
    command_id TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL DEFAULT 'pending',
    next_attempt_at TIMESTAMPTZ,
    lease_until TIMESTAMPTZ,
    last_error TEXT,
    terminal_reason TEXT,
    delivered_at TIMESTAMPTZ,
    command_acked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, device_id, command_id)
);
CREATE INDEX IF NOT EXISTS idx_push_wake_due
    ON push_wake_outbox(next_attempt_at) WHERE status = 'pending';
