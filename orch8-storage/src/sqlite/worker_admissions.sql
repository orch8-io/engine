CREATE TABLE IF NOT EXISTS worker_task_admissions (
 task_id TEXT PRIMARY KEY,
 invocation_key TEXT NOT NULL UNIQUE CHECK(length(invocation_key)=64),
 tenant_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,
 record TEXT NOT NULL CHECK(json_valid(record))
);
CREATE INDEX IF NOT EXISTS worker_task_admissions_instance ON worker_task_admissions(tenant_id,instance_id);
CREATE TRIGGER IF NOT EXISTS worker_admission_update BEFORE UPDATE ON worker_task_admissions
 BEGIN SELECT RAISE(ABORT,'worker dispatch admissions are immutable'); END;
CREATE TRIGGER IF NOT EXISTS worker_admission_delete BEFORE DELETE ON worker_task_admissions
 BEGIN SELECT RAISE(ABORT,'worker dispatch admissions are immutable'); END;

CREATE TRIGGER IF NOT EXISTS worker_dispatch_spec_update BEFORE UPDATE ON worker_tasks
 WHEN EXISTS(SELECT 1 FROM worker_task_admissions a WHERE a.task_id=OLD.id)
 AND (NEW.id IS NOT OLD.id OR NEW.tenant_id IS NOT OLD.tenant_id OR NEW.instance_id IS NOT OLD.instance_id
 OR NEW.block_id IS NOT OLD.block_id OR NEW.handler_name IS NOT OLD.handler_name
 OR NEW.queue_name IS NOT OLD.queue_name OR NEW.requirements IS NOT OLD.requirements
 OR NEW.params IS NOT OLD.params OR NEW.context IS NOT OLD.context
 OR NEW.attempt IS NOT OLD.attempt OR NEW.timeout_ms IS NOT OLD.timeout_ms
 OR NEW.carries_credentials IS NOT OLD.carries_credentials OR NEW.effect_id IS NOT OLD.effect_id
 OR NEW.continuity_epoch IS NOT OLD.continuity_epoch)
 BEGIN SELECT RAISE(ABORT,'admitted worker dispatch specification is immutable'); END;
CREATE TRIGGER IF NOT EXISTS worker_dispatch_spec_insert BEFORE INSERT ON worker_tasks
 WHEN EXISTS(SELECT 1 FROM worker_task_admissions a WHERE a.task_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'admitted worker task cannot be recreated'); END;
