-- Partial index for waiting-instance scans. The scheduler periodically
-- queries `WHERE state = 'waiting' ORDER BY updated_at` to detect stale
-- instances; without this index the query degrades to a full table scan
-- as instance count grows.
CREATE INDEX IF NOT EXISTS idx_task_instances_waiting_updated
    ON task_instances(updated_at) WHERE state = 'waiting';
