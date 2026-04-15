-- External worker task queue for polyglot handler execution.
CREATE TABLE IF NOT EXISTS worker_tasks (
    id              UUID PRIMARY KEY,
    instance_id     UUID NOT NULL REFERENCES task_instances(id),
    block_id        TEXT NOT NULL,
    handler_name    TEXT NOT NULL,
    params          JSONB NOT NULL DEFAULT '{}',
    context         JSONB NOT NULL DEFAULT '{}',
    attempt         SMALLINT NOT NULL DEFAULT 0,
    timeout_ms      BIGINT,
    state           TEXT NOT NULL DEFAULT 'pending',
    worker_id       TEXT,
    claimed_at      TIMESTAMPTZ,
    heartbeat_at    TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    output          JSONB,
    error_message   TEXT,
    error_retryable BOOLEAN,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    UNIQUE (instance_id, block_id)
);

-- Fast poll: workers query by handler_name where state = 'pending'.
CREATE INDEX idx_worker_tasks_poll
    ON worker_tasks (handler_name, created_at)
    WHERE state = 'pending';

-- Fast reaper: find stale claimed tasks by heartbeat age.
CREATE INDEX idx_worker_tasks_heartbeat
    ON worker_tasks (heartbeat_at)
    WHERE state = 'claimed';
