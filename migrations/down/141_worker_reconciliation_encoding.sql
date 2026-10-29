-- An older reader must not silently discard a retained reconciliation decision.
LOCK TABLE worker_accepted_outcomes IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 IF current_setting('transaction_isolation')<>'read committed'
    OR EXISTS(SELECT 1 FROM worker_accepted_outcomes
       WHERE record->>'output_encoding'='json_text_reconciled_v1' OR record ? 'reconciliation') THEN
  RAISE EXCEPTION 'reconciled worker outcomes require the current reader';
 END IF;
END $$;
DROP INDEX uq_worker_reconciliation_approval;
CREATE OR REPLACE FUNCTION require_worker_outcome_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.record->>'output_encoding' IS DISTINCT FROM 'json_text_v1'
    OR jsonb_typeof(NEW.record->'output') IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION 'worker outcome requires json_text_v1 writer';
 END IF;
 PERFORM (NEW.record->>'output')::json;
 RETURN NEW;
END $$;
