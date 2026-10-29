-- Compact security evidence has no parent FK and contains no workflow payload.
CREATE TRIGGER IF NOT EXISTS physical_retirement_from_cut
 AFTER UPDATE OF state ON capsule_source_cuts
 WHEN NEW.state IN ('sealed','retired','acknowledged')
 BEGIN
  INSERT OR IGNORE INTO physical_instance_retirements
   VALUES(NEW.source_instance_id,NEW.tenant_id,NEW.continuity_id,NEW.capsule_id,NEW.created_at);
 END;
CREATE TRIGGER IF NOT EXISTS physical_retirement_from_inserted_cut
 AFTER INSERT ON capsule_source_cuts
 WHEN NEW.state IN ('sealed','retired','acknowledged')
 BEGIN
  INSERT OR IGNORE INTO physical_instance_retirements
   VALUES(NEW.source_instance_id,NEW.tenant_id,NEW.continuity_id,NEW.capsule_id,NEW.created_at);
 END;
CREATE TRIGGER IF NOT EXISTS physical_retirement_no_delete
 BEFORE DELETE ON physical_instance_retirements
 BEGIN SELECT RAISE(ABORT,'physical instance retirement is permanent'); END;
CREATE TRIGGER IF NOT EXISTS physical_retirement_no_update
 BEFORE UPDATE ON physical_instance_retirements
 BEGIN SELECT RAISE(ABORT,'physical instance retirement is immutable'); END;
CREATE TRIGGER IF NOT EXISTS physical_retirement_native_insert
 AFTER INSERT ON task_instances
 WHEN EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'physical instance identity is retired'); END;
CREATE TRIGGER IF NOT EXISTS physical_retirement_owner_insert
 AFTER INSERT ON continuity_executions
 WHEN EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=json_extract(NEW.record,'$.current_instance_id'))
 BEGIN SELECT RAISE(ABORT,'physical instance identity is retired'); END;
CREATE TRIGGER IF NOT EXISTS physical_retirement_owner_update
 AFTER UPDATE ON continuity_executions
 WHEN (json_extract(NEW.record,'$.current_instance_id') IS NOT json_extract(OLD.record,'$.current_instance_id') OR NEW.state IN ('owned','completed'))
 AND EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=json_extract(NEW.record,'$.current_instance_id'))
 BEGIN SELECT RAISE(ABORT,'physical instance identity is retired'); END;

-- Pausing does not certify that an independent/offline callback has stopped.
-- Keep its context, worker lease and outcome evidence for later reconciliation.
CREATE TRIGGER IF NOT EXISTS physical_retirement_freeze_native
 AFTER INSERT ON physical_instance_retirements
 BEGIN
  UPDATE task_instances SET state='paused',claimed_at=NULL,next_fire_at=NULL,updated_at=strftime('%Y-%m-%dT%H:%M:%fZ','now')
   WHERE id=NEW.instance_id AND state IN ('scheduled','running','waiting','paused')
   AND (state!='paused' OR claimed_at IS NOT NULL OR next_fire_at IS NOT NULL);
 END;
CREATE TRIGGER IF NOT EXISTS physical_retirement_native_update
 AFTER UPDATE OF state,next_fire_at,claimed_at ON task_instances
 WHEN NEW.state IN ('scheduled','running','waiting')
 AND EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'physical instance forward execution is retired'); END;
CREATE TRIGGER IF NOT EXISTS physical_source_worker_insert
 AFTER INSERT ON worker_tasks
 WHEN NEW.state='claimed' AND (
  EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.instance_id)
  OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.instance_id AND state IN ('captured','sealed')))
 BEGIN SELECT RAISE(ABORT,'physical source worker admission is frozen'); END;
CREATE TRIGGER IF NOT EXISTS physical_source_worker_claim
 AFTER UPDATE OF state,worker_id,claim_epoch,instance_id ON worker_tasks
 WHEN NEW.state='claimed' AND (
  EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.instance_id)
  OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.instance_id AND state IN ('captured','sealed')))
 BEGIN SELECT RAISE(ABORT,'physical source worker admission is frozen'); END;

-- SQLite's immediate writer transaction already serializes capture/enqueue.
CREATE TRIGGER IF NOT EXISTS physical_source_worker_enqueue
 BEFORE INSERT ON worker_tasks
 WHEN NEW.state IN ('pending','claimed') AND (
  EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.instance_id)
  OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.instance_id AND state IN ('captured','sealed')))
 BEGIN SELECT RAISE(ABORT,'physical source worker enqueue is frozen'); END;
