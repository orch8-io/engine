-- Roll back failure application only before pending or applied evidence exists.
LOCK TABLE worker_resolution_outcomes,worker_resolution_recovery,worker_resolution_integrations IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF current_setting('transaction_isolation') <> 'read committed' THEN
  RAISE EXCEPTION 'worker failure application downgrade requires read committed isolation';
 END IF;
 IF EXISTS(SELECT 1 FROM worker_resolution_recovery) OR EXISTS(SELECT 1 FROM worker_resolution_integrations) THEN
  RAISE EXCEPTION 'worker failure application evidence requires the current protocol';
 END IF;
END $$;
DROP TRIGGER worker_resolution_cut_insert ON capsule_source_cuts;
DROP TRIGGER worker_resolution_cut_update ON capsule_source_cuts;
DROP FUNCTION refuse_pending_worker_resolution_cut();
DROP TRIGGER worker_resolution_recovery_insert ON worker_resolution_outcomes;
DROP FUNCTION enqueue_worker_resolution();
DROP TABLE worker_resolution_recovery;
DROP TABLE worker_resolution_integrations;
DROP INDEX worker_resolution_outcomes_physical;
