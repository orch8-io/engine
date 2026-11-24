-- Rollback of 157: Remove callback tokens, the worker wait mode, and its update guard.
DROP TRIGGER IF EXISTS worker_callback_mode_update ON worker_tasks;
DROP FUNCTION IF EXISTS preserve_worker_callback_mode();
DROP TABLE IF EXISTS worker_callback_tokens;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS wait_for_callback;
