-- Native application must preserve the original JSON, including NUL and floats.
ALTER TABLE block_outputs ADD COLUMN output_encoding TEXT
 CHECK(output_encoding IS NULL OR output_encoding='json_text_v1');
ALTER TABLE worker_tasks ADD COLUMN params_encoding TEXT
 CHECK(params_encoding IS NULL OR params_encoding='json_text_v1');
ALTER TABLE worker_tasks ADD COLUMN context_encoding TEXT
 CHECK(context_encoding IS NULL OR context_encoding='json_text_v1');
ALTER TABLE worker_tasks ADD COLUMN output_encoding TEXT
 CHECK(output_encoding IS NULL OR output_encoding='json_text_v1');
ALTER TABLE worker_tasks ADD COLUMN checkpoint_encoding TEXT
 CHECK(checkpoint_encoding IS NULL OR checkpoint_encoding='json_text_v1');
-- JSON (rather than JSONB) validates the serialized output without rejecting NUL.
CREATE OR REPLACE FUNCTION require_worker_outcome_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.record->>'output_encoding' IS DISTINCT FROM 'json_text_v1'
    OR jsonb_typeof(NEW.record->'output') IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION 'worker outcome requires json_text_v1 writer';
 END IF;
 PERFORM (NEW.record->>'output')::json;
 RETURN NEW;
END $$;
ALTER TABLE checkpoints ADD COLUMN data_encoding TEXT
 CHECK(data_encoding IS NULL OR data_encoding='json_text_v1');
ALTER TABLE capsule_source_cuts ADD COLUMN snapshot_encoding TEXT
 CHECK(snapshot_encoding IS NULL OR snapshot_encoding='json_text_v1');

-- Live older coordinators must not claim, rewrite or delete encoded work.
-- This is an encoding reader floor, not authentication of database principals.
CREATE FUNCTION orch8_require_native_json_protocol(codec TEXT) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 IF codec IS NOT NULL AND current_setting('orch8.native_json_protocol',true) IS DISTINCT FROM 'json_text_v1' THEN
  RAISE EXCEPTION 'native JSON encoding requires a current protocol reader';
 END IF;
END $$;
CREATE FUNCTION orch8_validate_native_json_text(body JSONB, codec TEXT) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
 IF codec IS NULL THEN RETURN; END IF;
 PERFORM orch8_require_native_json_protocol(codec);
 IF codec <> 'json_text_v1' OR jsonb_typeof(body) IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION 'invalid native JSON text encoding';
 END IF;
 PERFORM (body #>> '{}')::json;
END $$;
CREATE FUNCTION validate_worker_json_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP <> 'INSERT' THEN
  PERFORM orch8_require_native_json_protocol(OLD.params_encoding);
  PERFORM orch8_require_native_json_protocol(OLD.context_encoding);
  PERFORM orch8_require_native_json_protocol(OLD.output_encoding);
  PERFORM orch8_require_native_json_protocol(OLD.checkpoint_encoding);
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 END IF;
 PERFORM orch8_validate_native_json_text(NEW.params,NEW.params_encoding);
 PERFORM orch8_validate_native_json_text(NEW.context,NEW.context_encoding);
 PERFORM orch8_validate_native_json_text(NEW.output,NEW.output_encoding);
 PERFORM orch8_validate_native_json_text(NEW.resume_checkpoint,NEW.checkpoint_encoding);
 IF TG_OP='UPDATE' THEN
  IF (NEW.params_encoding IS DISTINCT FROM OLD.params_encoding OR NEW.context_encoding IS DISTINCT FROM OLD.context_encoding)
   AND EXISTS(SELECT 1 FROM worker_task_admissions WHERE task_id=OLD.id) THEN
   RAISE EXCEPTION 'admitted worker payload encoding is immutable';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_json_encoding BEFORE INSERT OR UPDATE OR DELETE ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION validate_worker_json_encoding();
CREATE FUNCTION validate_output_json_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP <> 'INSERT' THEN
  PERFORM orch8_require_native_json_protocol(OLD.output_encoding);
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 END IF;
 PERFORM orch8_validate_native_json_text(NEW.output,NEW.output_encoding);
 RETURN NEW;
END $$;
CREATE TRIGGER output_json_encoding BEFORE INSERT OR UPDATE OR DELETE ON block_outputs
 FOR EACH ROW EXECUTE FUNCTION validate_output_json_encoding();
CREATE FUNCTION validate_checkpoint_json_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP <> 'INSERT' THEN
  PERFORM orch8_require_native_json_protocol(OLD.data_encoding);
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 END IF;
 PERFORM orch8_validate_native_json_text(NEW.checkpoint_data,NEW.data_encoding);
 RETURN NEW;
END $$;
CREATE TRIGGER checkpoint_json_encoding BEFORE INSERT OR UPDATE OR DELETE ON checkpoints
 FOR EACH ROW EXECUTE FUNCTION validate_checkpoint_json_encoding();
CREATE FUNCTION validate_cut_json_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF TG_OP <> 'INSERT' THEN
  PERFORM orch8_require_native_json_protocol(OLD.snapshot_encoding);
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 END IF;
 PERFORM orch8_validate_native_json_text(NEW.snapshot,NEW.snapshot_encoding);
 IF TG_OP='UPDATE' THEN
  IF OLD.snapshot IS NOT NULL AND NEW.snapshot IS NOT NULL
   AND NEW.snapshot_encoding IS DISTINCT FROM OLD.snapshot_encoding THEN
   RAISE EXCEPTION 'captured snapshot encoding is immutable';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER cut_json_encoding BEFORE INSERT OR UPDATE OR DELETE ON capsule_source_cuts
 FOR EACH ROW EXECUTE FUNCTION validate_cut_json_encoding();
CREATE FUNCTION validate_context_json_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE encoding JSONB; section TEXT; sections TEXT[] := '{}';
BEGIN
 IF TG_OP <> 'INSERT' THEN
  IF OLD.context ? '__orch8_context_encoding' THEN
   PERFORM orch8_require_native_json_protocol('json_text_v1');
  END IF;
  IF TG_OP='DELETE' THEN RETURN OLD; END IF;
 END IF;
 encoding := NEW.context->'__orch8_context_encoding';
 IF encoding IS NULL THEN RETURN NEW; END IF;
 PERFORM orch8_require_native_json_protocol('json_text_v1');
 IF encoding->>'codec' IS DISTINCT FROM 'json_text_v1'
  OR jsonb_typeof(encoding->'sections') IS DISTINCT FROM 'array' THEN
  RAISE EXCEPTION 'invalid native context encoding';
 END IF;
 FOR section IN SELECT jsonb_array_elements_text(encoding->'sections') LOOP
  IF section NOT IN ('data','config','audit') OR section=ANY(sections) THEN
   RAISE EXCEPTION 'invalid native context section';
  END IF;
  sections := array_append(sections,section);
  PERFORM orch8_validate_native_json_text(NEW.context->section,'json_text_v1');
 END LOOP;
 IF cardinality(sections)=0 THEN RAISE EXCEPTION 'empty native context encoding'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER context_json_encoding BEFORE INSERT OR UPDATE OR DELETE ON task_instances
 FOR EACH ROW EXECUTE FUNCTION validate_context_json_encoding();
