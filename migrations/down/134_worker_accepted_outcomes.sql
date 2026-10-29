-- Remove an empty accepted-outcome journal; refuse to erase accepted results.
LOCK TABLE worker_accepted_outcomes IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM worker_accepted_outcomes) THEN
  RAISE EXCEPTION 'accepted worker outcomes must be retained';
 END IF;
END $$;
DROP TABLE worker_accepted_outcomes;
DROP FUNCTION preserve_worker_accepted_outcome();
