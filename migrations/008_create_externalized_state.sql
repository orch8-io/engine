CREATE TABLE IF NOT EXISTS externalized_state (
    id          UUID PRIMARY KEY,
    instance_id UUID NOT NULL REFERENCES task_instances(id),
    ref_key     TEXT NOT NULL UNIQUE,
    payload     JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    expires_at  TIMESTAMPTZ
);
