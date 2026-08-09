-- Roll back worker claim-generation fencing and attempt evidence.
DROP INDEX IF EXISTS idx_worker_task_attempt_events_task;
DROP TABLE IF EXISTS worker_task_attempt_events;
ALTER TABLE worker_tasks DROP COLUMN IF EXISTS claim_epoch;

