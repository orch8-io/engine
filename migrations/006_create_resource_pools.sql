CREATE TABLE IF NOT EXISTS resource_pools (
    id          UUID PRIMARY KEY,
    tenant_id   TEXT NOT NULL,
    name        TEXT NOT NULL,
    rotation    TEXT NOT NULL DEFAULT 'round_robin',
    resources   JSONB NOT NULL DEFAULT '[]',

    UNIQUE (tenant_id, name)
);
