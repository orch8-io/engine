-- Revert the auxiliary retention sweep indexes.
DROP INDEX IF EXISTS idx_mobile_approval_requests_settled_created;
DROP INDEX IF EXISTS idx_mobile_instance_status_terminal_updated;
DROP INDEX IF EXISTS idx_worker_task_attempt_events_created;
