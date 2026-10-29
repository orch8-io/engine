-- Remove captured-source authority guards only after all active source cuts settle.
-- Serialize the emptiness check with capture, publication and history writers.
LOCK TABLE continuity_executions, task_instances, capsule_source_cuts IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM capsule_source_cuts WHERE state IN ('captured','sealed') LIMIT 1) THEN
  RAISE EXCEPTION 'Cannot remove captured-source guards while a source is frozen';
 END IF;
END $$;
DROP TRIGGER captured_source_native_delete ON task_instances;
DROP FUNCTION preserve_active_source_native();
DROP TRIGGER captured_source_cut_delete ON capsule_source_cuts;
DROP FUNCTION preserve_active_source_cut();
DROP TRIGGER captured_source_owner_write ON continuity_executions;
DROP FUNCTION enforce_captured_owner_authority();
DROP TRIGGER captured_source_native_insert ON task_instances;
DROP TRIGGER captured_source_native_update ON task_instances;
DROP FUNCTION enforce_captured_native_authority();
