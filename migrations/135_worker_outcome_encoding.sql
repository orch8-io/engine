-- Versioned output text preserves JSON number representations and fingerprints.
-- Existing unencoded records remain readable; older writers must not add more.
CREATE FUNCTION require_worker_outcome_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.record->>'output_encoding' IS DISTINCT FROM 'json_text_v1'
    OR jsonb_typeof(NEW.record->'output') IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION 'worker outcome requires json_text_v1 writer';
 END IF;
 PERFORM (NEW.record->>'output')::jsonb;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_outcome_encoding_insert BEFORE INSERT ON worker_accepted_outcomes
 FOR EACH ROW EXECUTE FUNCTION require_worker_outcome_encoding();
