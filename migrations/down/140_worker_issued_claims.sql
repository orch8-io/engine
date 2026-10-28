-- Revert only before issued claim evidence has been retained.
LOCK TABLE worker_tasks,worker_issued_claims IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed' OR EXISTS(SELECT 1 FROM worker_issued_claims) THEN
  RAISE EXCEPTION 'issued worker claims require the current protocol';
 END IF;
END $$;
DROP TRIGGER worker_issued_identity_before ON worker_tasks;
DROP TRIGGER worker_issued_identity_after ON worker_tasks;
DROP FUNCTION refuse_issued_worker_identity_recreation();
DROP TABLE worker_issued_claims;
DROP FUNCTION preserve_worker_issued_claim();
