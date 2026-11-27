-- Remove the index added for paging a session's executions.
DROP INDEX IF EXISTS idx_task_instances_session_page;
