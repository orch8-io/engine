-- Concurrency control: limit parallel running instances by key.
ALTER TABLE task_instances ADD COLUMN concurrency_key TEXT;
ALTER TABLE task_instances ADD COLUMN max_concurrency INTEGER;

-- Idempotency: prevent duplicate instance creation.
ALTER TABLE task_instances ADD COLUMN idempotency_key TEXT;

-- Index for concurrency checks: count running instances by key.
CREATE INDEX idx_instances_concurrency
    ON task_instances (concurrency_key)
    WHERE concurrency_key IS NOT NULL AND state = 'running';

-- Unique index for idempotency deduplication.
CREATE UNIQUE INDEX idx_instances_idempotency
    ON task_instances (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;
