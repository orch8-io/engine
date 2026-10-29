DROP TRIGGER IF EXISTS worker_outcome_encoding_insert;
CREATE TRIGGER worker_outcome_encoding_insert BEFORE INSERT ON worker_accepted_outcomes
 WHEN json_extract(NEW.record,'$.output_encoding') IS NULL
 OR json_extract(NEW.record,'$.output_encoding') NOT IN ('json_text_v1','json_text_reconciled_v1')
 OR json_type(NEW.record,'$.output') IS NOT 'text'
 OR json_valid(json_extract(NEW.record,'$.output')) IS NOT 1
 OR (json_extract(NEW.record,'$.output_encoding')='json_text_reconciled_v1'
     AND json_type(NEW.record,'$.reconciliation') IS NOT 'object')
 OR (json_extract(NEW.record,'$.output_encoding')='json_text_v1'
     AND COALESCE(json_type(NEW.record,'$.reconciliation'),'null')<>'null')
 BEGIN SELECT RAISE(ABORT,'worker outcome requires json_text_v1 writer or explicit reconciled reader'); END;
CREATE UNIQUE INDEX IF NOT EXISTS uq_worker_reconciliation_approval
 ON worker_accepted_outcomes(json_extract(record,'$.reconciliation.approval.approval_id'))
 WHERE json_extract(record,'$.output_encoding')='json_text_reconciled_v1';
