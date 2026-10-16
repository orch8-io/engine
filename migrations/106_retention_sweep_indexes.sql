-- L1-02 / L1-15: indexes for the bounded auxiliary retention sweep
-- (`delete_expired_auxiliary`, run from the engine GC loop). Every other
-- table it touches already has an index on its age column.
--
-- Deliberately NOT `CONCURRENTLY` (see 084 / migrations/README.md). On large
-- installs pre-create them online before upgrading:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_worker_task_attempt_events_created
--       ON worker_task_attempt_events (created_at);
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mobile_instance_status_terminal_updated
--       ON mobile_instance_status (updated_at)
--       WHERE state IN ('completed', 'failed', 'cancelled');
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mobile_approval_requests_settled_created
--       ON mobile_approval_requests (created_at) WHERE state <> 'pending';
CREATE INDEX IF NOT EXISTS idx_worker_task_attempt_events_created
    ON worker_task_attempt_events (created_at);
CREATE INDEX IF NOT EXISTS idx_mobile_instance_status_terminal_updated
    ON mobile_instance_status (updated_at)
    WHERE state IN ('completed', 'failed', 'cancelled');
CREATE INDEX IF NOT EXISTS idx_mobile_approval_requests_settled_created
    ON mobile_approval_requests (created_at) WHERE state <> 'pending';
