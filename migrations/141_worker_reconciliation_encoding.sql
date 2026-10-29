-- Retained operator decisions require a reader that understands their attribution.
CREATE OR REPLACE FUNCTION require_worker_outcome_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.record->>'output_encoding' NOT IN ('json_text_v1','json_text_reconciled_v1')
    OR NEW.record->>'output_encoding' IS NULL
    OR jsonb_typeof(NEW.record->'output') IS DISTINCT FROM 'string'
    OR (NEW.record->>'output_encoding'='json_text_reconciled_v1'
        AND jsonb_typeof(NEW.record->'reconciliation') IS DISTINCT FROM 'object')
    OR (NEW.record->>'output_encoding'='json_text_v1'
        AND COALESCE(jsonb_typeof(NEW.record->'reconciliation'),'null')<>'null') THEN
  RAISE EXCEPTION 'worker outcome requires json_text_v1 writer or explicit reconciled reader';
 END IF;
 PERFORM (NEW.record->>'output')::json;
 RETURN NEW;
END $$;
-- One acknowledgement ID names one permanent report, even across task/tenant scopes.
CREATE UNIQUE INDEX uq_worker_reconciliation_approval
 ON worker_accepted_outcomes ((record #>> '{reconciliation,approval,approval_id}'))
 WHERE record->>'output_encoding'='json_text_reconciled_v1';
