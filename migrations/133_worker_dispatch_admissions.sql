-- A prepared retry reserves invocation scope; this separately pins the actual
-- admitted payload, handler and receipt. Evidence survives hot task/native GC.
CREATE TABLE worker_task_admissions (
 task_id UUID PRIMARY KEY,
 invocation_key TEXT NOT NULL UNIQUE CHECK(length(invocation_key)=64),
 tenant_id TEXT NOT NULL,
 instance_id UUID NOT NULL,
 record JSONB NOT NULL
);
CREATE INDEX worker_task_admissions_instance ON worker_task_admissions(tenant_id,instance_id);
CREATE TRIGGER worker_admission_update BEFORE UPDATE ON worker_task_admissions
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_invocation();
CREATE TRIGGER worker_admission_delete BEFORE DELETE ON worker_task_admissions
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_invocation();

CREATE FUNCTION preserve_worker_dispatch_spec() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM worker_task_admissions a WHERE a.task_id=OLD.id)
 AND ROW(NEW.id,NEW.tenant_id,NEW.instance_id,NEW.block_id,NEW.handler_name,NEW.queue_name,
         NEW.requirements::text,NEW.params::text,NEW.context::text,NEW.attempt,
         NEW.timeout_ms,NEW.carries_credentials,NEW.effect_id,NEW.continuity_epoch)
 IS DISTINCT FROM
     ROW(OLD.id,OLD.tenant_id,OLD.instance_id,OLD.block_id,OLD.handler_name,OLD.queue_name,
         OLD.requirements::text,OLD.params::text,OLD.context::text,OLD.attempt,
         OLD.timeout_ms,OLD.carries_credentials,OLD.effect_id,OLD.continuity_epoch) THEN
  RAISE EXCEPTION 'admitted worker dispatch specification is immutable';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_dispatch_spec_update BEFORE UPDATE ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_dispatch_spec();
CREATE FUNCTION refuse_worker_dispatch_recreation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM worker_task_admissions a WHERE a.task_id=NEW.id) THEN
  RAISE EXCEPTION 'admitted worker task cannot be recreated';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_dispatch_spec_insert BEFORE INSERT ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION refuse_worker_dispatch_recreation();
