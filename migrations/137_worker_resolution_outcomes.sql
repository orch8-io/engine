-- Keep report/lease evidence after retry deletes hot tasks and native GC.
CREATE TABLE worker_resolution_outcomes (
 id UUID PRIMARY KEY,
 task_id UUID NOT NULL,
 tenant_id TEXT NOT NULL,
 instance_id UUID NOT NULL,
 claim_epoch BIGINT NOT NULL CHECK(claim_epoch>=0),
 worker_id TEXT,
 kind TEXT NOT NULL,
 record JSONB NOT NULL,
 created_at TIMESTAMPTZ NOT NULL,
 UNIQUE(task_id,claim_epoch,kind)
);
CREATE INDEX worker_resolution_outcomes_claim ON worker_resolution_outcomes(tenant_id,task_id,claim_epoch,worker_id);
CREATE FUNCTION preserve_worker_resolution_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'worker resolution outcomes must be retained unchanged'; END $$;
CREATE TRIGGER worker_resolution_update BEFORE UPDATE ON worker_resolution_outcomes
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_resolution_outcome();
CREATE TRIGGER worker_resolution_delete BEFORE DELETE ON worker_resolution_outcomes
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_resolution_outcome();
-- Exact error bodies use JSON text so even U+0000 remains representable.
CREATE FUNCTION require_worker_resolution_encoding() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.record->>'reason_encoding' IS DISTINCT FROM 'json_text_v1'
  OR jsonb_typeof(NEW.record->'reason') IS DISTINCT FROM 'string' THEN
  RAISE EXCEPTION 'worker resolution requires json_text_v1 writer';
 END IF;
 PERFORM (NEW.record->>'reason')::json;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_resolution_encoding_insert BEFORE INSERT ON worker_resolution_outcomes
 FOR EACH ROW EXECUTE FUNCTION require_worker_resolution_encoding();

-- Retained acknowledgements bind a permanently unique task identity and generation.
CREATE FUNCTION refuse_worker_task_recreation() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.claim_epoch < 0 THEN RAISE EXCEPTION 'invalid worker claim epoch'; END IF;
 IF NOT EXISTS(SELECT 1 FROM worker_tasks WHERE id=NEW.id) AND (
  EXISTS(SELECT 1 FROM worker_task_invocations WHERE task_id=NEW.id)
  OR EXISTS(SELECT 1 FROM worker_accepted_outcomes WHERE task_id=NEW.id)
  OR EXISTS(SELECT 1 FROM worker_resolution_outcomes WHERE task_id=NEW.id)) THEN
  RAISE EXCEPTION 'worker task identity is retained and cannot be recreated';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_identity_insert BEFORE INSERT ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION refuse_worker_task_recreation();
-- BEFORE INSERT can see a row whose concurrent DELETE has not committed yet.
-- Recheck after the unique-key wait; upserts of an existing row do not fire this.
CREATE FUNCTION refuse_retained_worker_task_insertion() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM worker_task_invocations WHERE task_id=NEW.id)
  OR EXISTS(SELECT 1 FROM worker_accepted_outcomes WHERE task_id=NEW.id)
  OR EXISTS(SELECT 1 FROM worker_resolution_outcomes WHERE task_id=NEW.id) THEN
  RAISE EXCEPTION 'worker task identity is retained and cannot be recreated';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_identity_inserted AFTER INSERT ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION refuse_retained_worker_task_insertion();
CREATE FUNCTION preserve_worker_task_claim_identity() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.id IS DISTINCT FROM OLD.id OR NEW.instance_id IS DISTINCT FROM OLD.instance_id
  OR NEW.block_id IS DISTINCT FROM OLD.block_id OR NEW.attempt IS DISTINCT FROM OLD.attempt THEN
  RAISE EXCEPTION 'worker task identity cannot be reassigned';
 END IF;
 IF NEW.claim_epoch < OLD.claim_epoch OR NEW.claim_epoch < 0
  OR (NEW.state='claimed' AND OLD.state<>'claimed' AND NEW.claim_epoch <= OLD.claim_epoch)
  OR (NEW.claim_epoch=OLD.claim_epoch AND OLD.worker_id IS NOT NULL AND NEW.worker_id IS NOT NULL
   AND NEW.worker_id IS DISTINCT FROM OLD.worker_id) THEN
  RAISE EXCEPTION 'worker claim generation cannot be reused';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_claim_identity BEFORE UPDATE ON worker_tasks
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_task_claim_identity();
