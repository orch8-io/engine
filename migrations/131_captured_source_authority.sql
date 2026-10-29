-- A captured source is frozen until explicit abort or durable publication.
-- AFTER guards see evidence committed while an older writer waited on a row.
CREATE FUNCTION enforce_captured_native_authority() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=NEW.id AND state='captured') THEN
  IF TG_OP='INSERT' THEN
   RAISE EXCEPTION 'captured physical identity cannot be recreated' USING ERRCODE='23514';
  END IF;
  IF NEW.id IS DISTINCT FROM OLD.id OR NEW.state IN ('scheduled','running','waiting')
   OR (NEW.claimed_at IS NOT NULL AND NEW.claimed_at IS DISTINCT FROM OLD.claimed_at)
   OR (NEW.next_fire_at IS NOT NULL AND NEW.next_fire_at IS DISTINCT FROM OLD.next_fire_at) THEN
   RAISE EXCEPTION 'captured source forward authority is frozen' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER captured_source_native_insert AFTER INSERT ON task_instances
 FOR EACH ROW EXECUTE FUNCTION enforce_captured_native_authority();
CREATE TRIGGER captured_source_native_update AFTER UPDATE OF id,state,next_fire_at,claimed_at ON task_instances
 FOR EACH ROW EXECUTE FUNCTION enforce_captured_native_authority();

CREATE FUNCTION enforce_captured_owner_authority() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF TG_OP='INSERT' THEN
  IF EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=(NEW.record->>'current_instance_id')::uuid AND state='captured') THEN
   RAISE EXCEPTION 'captured source cannot be adopted by another owner' USING ERRCODE='23514';
  END IF;
 ELSIF EXISTS(SELECT 1 FROM capsule_source_cuts WHERE state='captured' AND (
   source_instance_id=(NEW.record->>'current_instance_id')::uuid
   OR (tenant_id=OLD.tenant_id AND continuity_id=OLD.continuity_id)
   OR (tenant_id=NEW.tenant_id AND continuity_id=NEW.continuity_id))) THEN
  IF NEW.tenant_id IS DISTINCT FROM OLD.tenant_id OR NEW.continuity_id IS DISTINCT FROM OLD.continuity_id
   OR NEW.epoch IS DISTINCT FROM OLD.epoch OR NEW.owner_runtime_id IS DISTINCT FROM OLD.owner_runtime_id
   OR NEW.state NOT IN ('transferring','compensating') OR NEW.record->>'state' IS DISTINCT FROM NEW.state
   OR (NEW.record - 'updated_at' - 'state') IS DISTINCT FROM (OLD.record - 'updated_at' - 'state') THEN
   RAISE EXCEPTION 'captured ownership authority is frozen' USING ERRCODE='23514';
  END IF;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER captured_source_owner_write AFTER INSERT OR UPDATE ON continuity_executions
 FOR EACH ROW EXECUTE FUNCTION enforce_captured_owner_authority();

-- An ordinary history sweep must not discard an unacknowledged recovery point.
CREATE FUNCTION preserve_active_source_cut() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF OLD.state IN ('captured','sealed') THEN
  RAISE EXCEPTION 'active source cut requires explicit abort or acknowledgement' USING ERRCODE='23514';
 END IF;
 RETURN OLD;
END $$;
CREATE TRIGGER captured_source_cut_delete BEFORE DELETE ON capsule_source_cuts
 FOR EACH ROW EXECUTE FUNCTION preserve_active_source_cut();

-- Retain native suspension/input/outcome evidence until the transfer is
-- explicitly aborted, retired or acknowledged; a cut is not transport proof.
CREATE FUNCTION preserve_active_source_native() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM capsule_source_cuts WHERE source_instance_id=OLD.id AND state IN ('captured','sealed')) THEN
  RAISE EXCEPTION 'active source native history requires explicit abort or acknowledgement' USING ERRCODE='23514';
 END IF;
 RETURN OLD;
END $$;
CREATE TRIGGER captured_source_native_delete BEFORE DELETE ON task_instances
 FOR EACH ROW EXECUTE FUNCTION preserve_active_source_native();

-- Quarantine inconsistent pre-guard native state once, without claiming that
-- an independent callback stopped or deleting any outcome/context evidence.
UPDATE task_instances SET state='paused',claimed_at=NULL,next_fire_at=NULL,updated_at=NOW()
 WHERE state IN ('scheduled','running','waiting','paused')
 AND (state!='paused' OR claimed_at IS NOT NULL OR next_fire_at IS NOT NULL)
 AND EXISTS(SELECT 1 FROM capsule_source_cuts cut WHERE cut.source_instance_id=task_instances.id AND cut.state='captured');
