CREATE TRIGGER IF NOT EXISTS worker_outcome_encoding_insert BEFORE INSERT ON worker_accepted_outcomes
 WHEN json_extract(NEW.record,'$.output_encoding') IS NOT 'json_text_v1'
 OR json_type(NEW.record,'$.output') IS NOT 'text'
 OR json_valid(json_extract(NEW.record,'$.output')) IS NOT 1
 BEGIN SELECT RAISE(ABORT,'worker outcome requires json_text_v1 writer'); END;
