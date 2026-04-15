CREATE TABLE cron_schedules (
    id          UUID PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    namespace   TEXT NOT NULL,
    sequence_id UUID NOT NULL REFERENCES sequences(id),
    cron_expr   TEXT NOT NULL,
    timezone    TEXT NOT NULL DEFAULT 'UTC',
    enabled     BOOLEAN NOT NULL DEFAULT TRUE,
    metadata    JSONB NOT NULL DEFAULT '{}',
    last_triggered_at TIMESTAMPTZ,
    next_fire_at      TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_cron_next_fire ON cron_schedules (next_fire_at)
    WHERE enabled = TRUE;
