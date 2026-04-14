-- Add queue_name to worker_tasks for routing to dedicated worker pools.
ALTER TABLE worker_tasks ADD COLUMN IF NOT EXISTS queue_name TEXT;

CREATE INDEX IF NOT EXISTS idx_worker_tasks_queue ON worker_tasks(queue_name, state) WHERE queue_name IS NOT NULL;
