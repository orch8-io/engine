-- Encoded native values cannot be interpreted by an older storage reader.
LOCK TABLE task_instances, block_outputs, worker_tasks, checkpoints, capsule_source_cuts, worker_accepted_outcomes IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF current_setting('transaction_isolation') <> 'read committed' THEN
  RAISE EXCEPTION 'native JSON downgrade requires read committed isolation';
 END IF;
 IF EXISTS(SELECT 1 FROM task_instances WHERE context ? '__orch8_context_encoding')
 OR EXISTS(SELECT 1 FROM block_outputs WHERE output_encoding IS NOT NULL)
 OR EXISTS(SELECT 1 FROM worker_tasks WHERE params_encoding IS NOT NULL OR context_encoding IS NOT NULL OR output_encoding IS NOT NULL OR checkpoint_encoding IS NOT NULL)
 OR EXISTS(SELECT 1 FROM checkpoints WHERE data_encoding IS NOT NULL)
 OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE snapshot_encoding IS NOT NULL)
 OR EXISTS(SELECT 1 FROM worker_accepted_outcomes) THEN
  RAISE EXCEPTION 'native JSON values require the current encoding protocol';
 END IF;
END $$;
DROP TRIGGER context_json_encoding ON task_instances;
DROP FUNCTION validate_context_json_encoding();
DROP TRIGGER cut_json_encoding ON capsule_source_cuts;
DROP FUNCTION validate_cut_json_encoding();
DROP TRIGGER checkpoint_json_encoding ON checkpoints;
DROP FUNCTION validate_checkpoint_json_encoding();
DROP TRIGGER output_json_encoding ON block_outputs;
DROP FUNCTION validate_output_json_encoding();
DROP TRIGGER worker_json_encoding ON worker_tasks;
DROP FUNCTION validate_worker_json_encoding();
DROP FUNCTION orch8_validate_native_json_text(JSONB,TEXT);
DROP FUNCTION orch8_require_native_json_protocol(TEXT);
ALTER TABLE capsule_source_cuts DROP COLUMN snapshot_encoding;
ALTER TABLE checkpoints DROP COLUMN data_encoding;
ALTER TABLE worker_tasks DROP COLUMN checkpoint_encoding;
ALTER TABLE worker_tasks DROP COLUMN output_encoding;
ALTER TABLE worker_tasks DROP COLUMN context_encoding;
ALTER TABLE worker_tasks DROP COLUMN params_encoding;
ALTER TABLE block_outputs DROP COLUMN output_encoding;
CREATE OR REPLACE FUNCTION require_worker_outcome_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.record->>'output_encoding' IS DISTINCT FROM 'json_text_v1'
    OR jsonb_typeof(NEW.record->'output') IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION 'worker outcome requires json_text_v1 writer';
 END IF;
 PERFORM (NEW.record->>'output')::jsonb;
 RETURN NEW;
END $$;
