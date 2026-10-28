-- Refuse deleting retained failure/lease evidence, including concurrent writers.
LOCK TABLE worker_resolution_outcomes IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF current_setting('transaction_isolation') <> 'read committed' THEN
  RAISE EXCEPTION 'worker resolution downgrade requires read committed isolation';
 END IF;
 IF EXISTS(SELECT 1 FROM worker_resolution_outcomes) THEN
  RAISE EXCEPTION 'worker resolution outcomes require the current protocol';
 END IF;
END $$;
DROP TRIGGER worker_identity_insert ON worker_tasks;
DROP FUNCTION refuse_worker_task_recreation();
DROP TRIGGER worker_identity_inserted ON worker_tasks;
DROP FUNCTION refuse_retained_worker_task_insertion();
DROP TRIGGER worker_claim_identity ON worker_tasks;
DROP FUNCTION preserve_worker_task_claim_identity();
DROP TABLE worker_resolution_outcomes;
DROP FUNCTION require_worker_resolution_encoding();
DROP FUNCTION preserve_worker_resolution_outcome();
