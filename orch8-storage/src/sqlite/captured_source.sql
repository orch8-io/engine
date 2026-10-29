-- Repeat the partial-index predicate explicitly: SQLite does not infer that
-- state='captured' implies the existing captured/sealed partial indexes. Keep
-- the three owner identity lookups separate to avoid an active-index scan.
-- Source capture can be undone only by explicit unsealed abort. Terminal
-- outcomes and a same-owner compensation fence remain admissible evidence.
CREATE TRIGGER IF NOT EXISTS captured_source_native_insert
 AFTER INSERT ON task_instances
 WHEN EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.id AND state IN ('captured','sealed') AND state='captured')
 BEGIN SELECT RAISE(ABORT,'captured physical identity cannot be recreated'); END;
CREATE TRIGGER IF NOT EXISTS captured_source_native_update
 AFTER UPDATE OF id,state,next_fire_at,claimed_at ON task_instances
 WHEN (NEW.id IS NOT OLD.id OR NEW.state IN ('scheduled','running','waiting')
  OR (NEW.claimed_at IS NOT NULL AND NEW.claimed_at IS NOT OLD.claimed_at)
  OR (NEW.next_fire_at IS NOT NULL AND NEW.next_fire_at IS NOT OLD.next_fire_at))
 AND EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.id AND state IN ('captured','sealed') AND state='captured')
 BEGIN SELECT RAISE(ABORT,'captured source forward authority is frozen'); END;
CREATE TRIGGER IF NOT EXISTS captured_source_owner_insert
 AFTER INSERT ON continuity_executions
 WHEN EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=json_extract(NEW.record,'$.current_instance_id') AND state IN ('captured','sealed') AND state='captured')
 BEGIN SELECT RAISE(ABORT,'captured source cannot be adopted by another owner'); END;
CREATE TRIGGER IF NOT EXISTS captured_source_owner_update
 AFTER UPDATE ON continuity_executions
 WHEN (NEW.tenant_id IS NOT OLD.tenant_id OR NEW.continuity_id IS NOT OLD.continuity_id
  OR NEW.epoch IS NOT OLD.epoch OR NEW.owner_runtime_id IS NOT OLD.owner_runtime_id
  OR NEW.state NOT IN ('transferring','compensating') OR json_extract(NEW.record,'$.state') IS NOT NEW.state
  OR json_extract(NEW.record,'$.tenant_id') IS NOT json_extract(OLD.record,'$.tenant_id')
  OR json_extract(NEW.record,'$.continuity_id') IS NOT json_extract(OLD.record,'$.continuity_id')
  OR json_extract(NEW.record,'$.current_instance_id') IS NOT json_extract(OLD.record,'$.current_instance_id')
  OR json_extract(NEW.record,'$.owner_runtime_id') IS NOT json_extract(OLD.record,'$.owner_runtime_id')
  OR json_extract(NEW.record,'$.epoch') IS NOT json_extract(OLD.record,'$.epoch'))
 AND (EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=json_extract(NEW.record,'$.current_instance_id') AND state IN ('captured','sealed') AND state='captured')
  OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE tenant_id=OLD.tenant_id AND continuity_id=OLD.continuity_id AND state IN ('captured','sealed') AND state='captured')
  OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE tenant_id=NEW.tenant_id AND continuity_id=NEW.continuity_id AND state IN ('captured','sealed') AND state='captured'))
 BEGIN SELECT RAISE(ABORT,'captured ownership authority is frozen'); END;
CREATE TRIGGER IF NOT EXISTS captured_source_cut_delete
 BEFORE DELETE ON capsule_source_cuts WHEN OLD.state IN ('captured','sealed')
 BEGIN SELECT RAISE(ABORT,'active source cut requires explicit abort or acknowledgement'); END;

-- Captured snapshots do not yet transport every local suspension/ingress row.
CREATE TRIGGER IF NOT EXISTS captured_source_native_delete
 BEFORE DELETE ON task_instances
 WHEN EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=OLD.id AND state IN ('captured','sealed'))
 BEGIN SELECT RAISE(ABORT,'active source native history requires explicit abort or acknowledgement'); END;
