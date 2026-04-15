CREATE TABLE IF NOT EXISTS block_outputs (
    id              UUID PRIMARY KEY,
    instance_id     UUID NOT NULL REFERENCES task_instances(id),
    block_id        TEXT NOT NULL,
    output          JSONB,
    output_ref      TEXT,
    output_size     INTEGER NOT NULL DEFAULT 0,
    attempt         SMALLINT NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (instance_id, block_id)
);
