-- Physical identity retirement survives all native/continuity history collection.
CREATE TABLE physical_instance_retirements (
 instance_id UUID PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 continuity_id UUID NOT NULL,
 capsule_id UUID NOT NULL,
 retired_at TIMESTAMPTZ NOT NULL
);
CREATE FUNCTION record_physical_instance_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state IN ('sealed','retired','acknowledged') THEN
  INSERT INTO physical_instance_retirements VALUES(NEW.source_instance_id,NEW.tenant_id,NEW.continuity_id,NEW.capsule_id,NEW.created_at) ON CONFLICT DO NOTHING;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER physical_retirement_from_cut AFTER INSERT OR UPDATE OF state ON capsule_source_cuts
 FOR EACH ROW EXECUTE FUNCTION record_physical_instance_retirement();
CREATE FUNCTION preserve_physical_instance_retirement() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 RAISE EXCEPTION 'physical instance retirement is permanent and immutable' USING ERRCODE='23514';
END $$;
CREATE TRIGGER physical_retirement_immutable BEFORE UPDATE OR DELETE ON physical_instance_retirements
 FOR EACH ROW EXECUTE FUNCTION preserve_physical_instance_retirement();
CREATE FUNCTION enforce_physical_instance_retirement() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
DECLARE physical_id UUID;
BEGIN
 IF TG_TABLE_NAME='task_instances' THEN
  physical_id := NEW.id;
 ELSE
  physical_id := (NEW.record->>'current_instance_id')::uuid;
  IF TG_OP='UPDATE' THEN
   IF physical_id IS NOT DISTINCT FROM (OLD.record->>'current_instance_id')::uuid AND NEW.state NOT IN ('owned','completed') THEN
    RETURN NEW;
   END IF;
  END IF;
 END IF;
 IF EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=physical_id) THEN
  RAISE EXCEPTION 'physical instance identity is retired' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
-- AFTER INSERT checks after any native/owner uniqueness wait as well.
CREATE TRIGGER physical_retirement_native_insert AFTER INSERT ON task_instances
 FOR EACH ROW EXECUTE FUNCTION enforce_physical_instance_retirement();
CREATE TRIGGER physical_retirement_owner_write AFTER INSERT OR UPDATE ON continuity_executions
 FOR EACH ROW EXECUTE FUNCTION enforce_physical_instance_retirement();

CREATE FUNCTION freeze_retired_native_instance() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 UPDATE task_instances SET state='paused',claimed_at=NULL,next_fire_at=NULL,updated_at=NOW()
  WHERE id=NEW.instance_id AND state IN ('scheduled','running','waiting','paused')
  AND (state!='paused' OR claimed_at IS NOT NULL OR next_fire_at IS NOT NULL);
 RETURN NEW;
END $$;
CREATE TRIGGER physical_retirement_freeze_native AFTER INSERT ON physical_instance_retirements
 FOR EACH ROW EXECUTE FUNCTION freeze_retired_native_instance();
CREATE FUNCTION enforce_retired_native_state() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF NEW.state IN ('scheduled','running','waiting') AND EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.id) THEN
  RAISE EXCEPTION 'physical instance forward execution is retired' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER physical_retirement_native_update AFTER UPDATE OF state,next_fire_at,claimed_at ON task_instances
 FOR EACH ROW EXECUTE FUNCTION enforce_retired_native_state();
CREATE FUNCTION enforce_source_worker_admission() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF NEW.state='claimed' AND (
  EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.instance_id)
  OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.instance_id AND state IN ('captured','sealed'))) THEN
  RAISE EXCEPTION 'physical source worker admission is frozen' USING ERRCODE='23514';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER physical_source_worker_claim AFTER INSERT OR UPDATE OF state,worker_id,claim_epoch,instance_id ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION enforce_source_worker_admission();
-- Touch the parent's MVCC version before taking a worker row/index lock.
-- A repeatable-read capture that waited on this enqueue must restart rather
-- than omit the just-committed worker using its earlier snapshot.
CREATE FUNCTION enforce_source_worker_enqueue() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF NEW.state IN ('pending','claimed') THEN
  UPDATE task_instances SET updated_at=updated_at WHERE id=NEW.instance_id;
  IF EXISTS(SELECT 1 FROM physical_instance_retirements WHERE instance_id=NEW.instance_id)
   OR EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.instance_id AND state IN ('captured','sealed')) THEN
   RAISE EXCEPTION 'physical source worker enqueue is frozen' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER physical_source_worker_enqueue BEFORE INSERT ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION enforce_source_worker_enqueue();
-- Install all guards before backfill quarantines inconsistent legacy native rows.
INSERT INTO physical_instance_retirements
 SELECT DISTINCT ON (source_instance_id) source_instance_id,tenant_id,continuity_id,capsule_id,created_at
 FROM capsule_source_cuts WHERE state IN ('sealed','retired','acknowledged')
 ORDER BY source_instance_id,created_at,capsule_id;
