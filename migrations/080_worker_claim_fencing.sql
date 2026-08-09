-- Fence stale external-worker attempts and retain an append-only lifecycle.
ALTER TABLE worker_tasks
    ADD COLUMN IF NOT EXISTS claim_epoch BIGINT NOT NULL DEFAULT 0
    CHECK (claim_epoch >= 0);

CREATE TABLE IF NOT EXISTS worker_task_attempt_events (
    id          UUID PRIMARY KEY,
    -- Intentionally no foreign key: worker task rows are deleted on retries and
    -- loop resets, while attempt evidence must remain available for incidents.
    task_id     UUID NOT NULL,
    claim_epoch BIGINT NOT NULL CHECK (claim_epoch >= 0),
    worker_id   TEXT,
    event       TEXT NOT NULL CHECK (event IN (
                    'claimed', 'reclaimed', 'completed', 'failed',
                    'timed_out', 'cancelled', 'stale_mutation_rejected'
                )),
    reason      TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_worker_task_attempt_events_task
    ON worker_task_attempt_events (task_id, created_at, id);
