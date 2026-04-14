CREATE TABLE IF NOT EXISTS task_instances (
    id              UUID PRIMARY KEY,
    sequence_id     UUID NOT NULL REFERENCES sequences(id),
    tenant_id       TEXT NOT NULL,
    namespace       TEXT NOT NULL,
    state           TEXT NOT NULL DEFAULT 'scheduled',
    next_fire_at    TIMESTAMPTZ,
    priority        SMALLINT NOT NULL DEFAULT 1,
    timezone        TEXT NOT NULL DEFAULT 'UTC',
    metadata        JSONB NOT NULL DEFAULT '{}',
    context         JSONB NOT NULL DEFAULT '{}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
