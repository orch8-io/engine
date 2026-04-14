CREATE TABLE IF NOT EXISTS signal_inbox (
    id              UUID PRIMARY KEY,
    instance_id     UUID NOT NULL REFERENCES task_instances(id),
    signal_type     TEXT NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}',
    delivered       BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    delivered_at    TIMESTAMPTZ
);
