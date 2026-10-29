-- Remove the encoding floor only before records require its newer reader.
LOCK TABLE worker_accepted_outcomes IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM worker_accepted_outcomes WHERE record ? 'output_encoding') THEN
  RAISE EXCEPTION 'versioned accepted outcomes require the current reader';
 END IF;
END $$;
DROP TRIGGER worker_outcome_encoding_insert ON worker_accepted_outcomes;
DROP FUNCTION require_worker_outcome_encoding();
