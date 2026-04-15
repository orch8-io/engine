-- Checkpoints for long-running instances.
-- Captures execution state snapshots for efficient recovery.
CREATE TABLE IF NOT EXISTS checkpoints (
    id              UUID PRIMARY KEY,
    instance_id     UUID NOT NULL REFERENCES task_instances(id) ON DELETE CASCADE,
    checkpoint_data JSONB NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_checkpoints_instance_id ON checkpoints(instance_id);
