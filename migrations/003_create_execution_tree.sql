CREATE TABLE IF NOT EXISTS execution_tree (
    id              UUID PRIMARY KEY,
    instance_id     UUID NOT NULL REFERENCES task_instances(id),
    block_id        TEXT NOT NULL,
    parent_id       UUID REFERENCES execution_tree(id),
    block_type      TEXT NOT NULL,
    branch_index    SMALLINT,
    state           TEXT NOT NULL DEFAULT 'pending',
    started_at      TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ
);
