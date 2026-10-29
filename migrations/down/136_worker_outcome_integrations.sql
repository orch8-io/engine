-- Revert native application receipts only while none have been written.
DO $$ BEGIN
 IF current_setting('transaction_isolation') <> 'read committed' THEN
  RAISE EXCEPTION 'worker outcome downgrade requires read committed isolation';
 END IF;
END $$;
-- Fence acceptance before checking recovery, including writers that commit
-- while this lock waits. An older snapshot cannot safely authorize deletion.
LOCK TABLE worker_accepted_outcomes IN ACCESS EXCLUSIVE MODE;
LOCK TABLE worker_outcome_integrations IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM worker_outcome_integrations) OR EXISTS(SELECT 1 FROM worker_outcome_recovery) THEN
  RAISE EXCEPTION 'worker outcomes require the current application protocol';
 END IF;
END $$;
DROP TRIGGER worker_outcome_recovery_insert ON worker_accepted_outcomes;
DROP FUNCTION queue_accepted_worker_outcome();
DROP TRIGGER worker_outcome_native_delete ON task_instances;
DROP FUNCTION retain_unapplied_worker_outcome_source();
DROP TRIGGER worker_outcome_cut_insert ON capsule_source_cuts;
DROP FUNCTION refuse_cut_with_unapplied_worker_outcome();
DROP TABLE worker_outcome_recovery;
DROP TABLE worker_outcome_recovery_clock;
DROP TABLE worker_outcome_integrations;
DROP INDEX worker_accepted_outcomes_physical;
