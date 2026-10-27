-- An empty installation can roll back; never remove live security evidence.
-- Serialize the emptiness check with capture, publication and history writers.
LOCK TABLE continuity_executions, task_instances, capsule_source_cuts, physical_instance_retirements IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 -- Resolve dependencies in the schema of the locked source-cut table.
 -- A later search_path schema may contain an independent installation.
 IF EXISTS (SELECT 1 FROM pg_proc dependency
            JOIN pg_class source ON source.oid='capsule_source_cuts'::regclass
            WHERE dependency.pronamespace=source.relnamespace
              AND dependency.proname='enforce_captured_native_authority'
              AND dependency.pronargs=0) THEN
  RAISE EXCEPTION 'Roll back captured-source authority migration first';
 END IF;
 IF EXISTS(SELECT 1 FROM physical_instance_retirements LIMIT 1) THEN
  RAISE EXCEPTION 'Cannot remove permanent physical instance retirement evidence';
 END IF;
END $$;
DROP TRIGGER physical_source_worker_enqueue ON worker_tasks;
DROP FUNCTION enforce_source_worker_enqueue();
DROP TRIGGER physical_source_worker_claim ON worker_tasks;
DROP TRIGGER physical_retirement_native_update ON task_instances;
DROP TRIGGER physical_retirement_freeze_native ON physical_instance_retirements;
DROP FUNCTION enforce_source_worker_admission();
DROP FUNCTION enforce_retired_native_state();
DROP FUNCTION freeze_retired_native_instance();
DROP TRIGGER physical_retirement_native_insert ON task_instances;
DROP TRIGGER physical_retirement_owner_write ON continuity_executions;
DROP TRIGGER physical_retirement_from_cut ON capsule_source_cuts;
DROP TRIGGER physical_retirement_immutable ON physical_instance_retirements;
DROP FUNCTION enforce_physical_instance_retirement();
DROP FUNCTION preserve_physical_instance_retirement();
DROP FUNCTION record_physical_instance_retirement();
DROP TABLE physical_instance_retirements;
