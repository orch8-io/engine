DROP TRIGGER IF EXISTS worker_heartbeat_timeout_update ON worker_tasks;
DROP FUNCTION IF EXISTS preserve_worker_heartbeat_timeout();
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS heartbeat_timeout_secs;
