-- Undo 095: drop distributed-execution worker task columns and indexes.
DROP INDEX IF EXISTS idx_continuity_locations_instance;
DROP INDEX IF EXISTS idx_worker_tasks_instance_state;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS claimed_runtime_kind;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS carries_credentials;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS lease_secs;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS continuity_epoch;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS effect_id;
