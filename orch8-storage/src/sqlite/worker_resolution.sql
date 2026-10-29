-- Keep report/lease evidence after retry deletes hot tasks and native GC.
CREATE TABLE IF NOT EXISTS worker_resolution_outcomes (
 id TEXT PRIMARY KEY,
 task_id TEXT NOT NULL,
 tenant_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,
 claim_epoch BIGINT NOT NULL CHECK(claim_epoch>=0),
 worker_id TEXT,
 kind TEXT NOT NULL,
 record TEXT NOT NULL,
 created_at TEXT NOT NULL,
 UNIQUE(task_id,claim_epoch,kind)
);
CREATE INDEX IF NOT EXISTS worker_resolution_outcomes_claim ON worker_resolution_outcomes(tenant_id,task_id,claim_epoch,worker_id);
CREATE TRIGGER IF NOT EXISTS worker_resolution_update BEFORE UPDATE ON worker_resolution_outcomes
 BEGIN SELECT RAISE(ABORT,'worker resolution outcomes must be retained unchanged'); END;
CREATE TRIGGER IF NOT EXISTS worker_resolution_delete BEFORE DELETE ON worker_resolution_outcomes
 BEGIN SELECT RAISE(ABORT,'worker resolution outcomes must be retained unchanged'); END;
CREATE TRIGGER IF NOT EXISTS worker_resolution_encoding_insert BEFORE INSERT ON worker_resolution_outcomes
 WHEN json_extract(NEW.record,'$.reason_encoding') IS NOT 'json_text_v1'
 OR json_type(NEW.record,'$.reason') IS NOT 'text'
 OR json_valid(json_extract(NEW.record,'$.reason')) IS NOT 1
 BEGIN SELECT RAISE(ABORT,'worker resolution requires json_text_v1 writer'); END;
CREATE TRIGGER IF NOT EXISTS worker_identity_insert BEFORE INSERT ON worker_tasks
 BEGIN
  SELECT CASE WHEN typeof(NEW.claim_epoch)!='integer' OR NEW.claim_epoch<0
   THEN RAISE(ABORT,'invalid worker claim epoch') END;
  SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM worker_tasks WHERE id=NEW.id) AND (
   EXISTS(SELECT 1 FROM worker_task_invocations WHERE task_id=NEW.id)
   OR EXISTS(SELECT 1 FROM worker_accepted_outcomes WHERE task_id=NEW.id)
   OR EXISTS(SELECT 1 FROM worker_resolution_outcomes WHERE task_id=NEW.id))
   THEN RAISE(ABORT,'worker task identity is retained and cannot be recreated') END;
 END;
CREATE TRIGGER IF NOT EXISTS worker_identity_inserted AFTER INSERT ON worker_tasks
 WHEN EXISTS(SELECT 1 FROM worker_task_invocations WHERE task_id=NEW.id)
 OR EXISTS(SELECT 1 FROM worker_accepted_outcomes WHERE task_id=NEW.id)
 OR EXISTS(SELECT 1 FROM worker_resolution_outcomes WHERE task_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'worker task identity is retained and cannot be recreated'); END;
CREATE TRIGGER IF NOT EXISTS worker_claim_identity BEFORE UPDATE ON worker_tasks
 BEGIN
  SELECT CASE WHEN NEW.id IS NOT OLD.id OR NEW.instance_id IS NOT OLD.instance_id
   OR NEW.block_id IS NOT OLD.block_id OR NEW.attempt IS NOT OLD.attempt
   THEN RAISE(ABORT,'worker task identity cannot be reassigned') END;
  SELECT CASE WHEN typeof(NEW.claim_epoch)!='integer' OR NEW.claim_epoch<OLD.claim_epoch OR NEW.claim_epoch<0
   OR (NEW.state='claimed' AND OLD.state!='claimed' AND NEW.claim_epoch<=OLD.claim_epoch)
   OR (NEW.claim_epoch=OLD.claim_epoch AND OLD.worker_id IS NOT NULL AND NEW.worker_id IS NOT NULL AND NEW.worker_id IS NOT OLD.worker_id)
   THEN RAISE(ABORT,'worker claim generation cannot be reused') END;
 END;
