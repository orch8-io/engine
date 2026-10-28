-- Claim provenance survives lease/task/native cleanup without retaining inputs.
CREATE TABLE worker_issued_claims (
 task_id UUID NOT NULL, claim_epoch BIGINT NOT NULL CHECK(claim_epoch>0),
 tenant_id TEXT NOT NULL, instance_id UUID NOT NULL, worker_id TEXT NOT NULL,
 record JSONB NOT NULL, issued_at TIMESTAMPTZ NOT NULL,
 PRIMARY KEY(task_id,claim_epoch)
);
CREATE INDEX worker_issued_claims_instance ON worker_issued_claims(tenant_id,instance_id);
CREATE FUNCTION preserve_worker_issued_claim() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'issued worker claims must be retained unchanged'; END $$;
CREATE TRIGGER worker_issued_claim_update BEFORE UPDATE ON worker_issued_claims FOR EACH ROW EXECUTE FUNCTION preserve_worker_issued_claim();
CREATE TRIGGER worker_issued_claim_delete BEFORE DELETE ON worker_issued_claims FOR EACH ROW EXECUTE FUNCTION preserve_worker_issued_claim();
CREATE FUNCTION refuse_issued_worker_identity_recreation() RETURNS trigger LANGUAGE plpgsql VOLATILE AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM worker_issued_claims WHERE task_id=NEW.id)
 AND (TG_WHEN='AFTER' OR NOT EXISTS(SELECT 1 FROM worker_tasks WHERE id=NEW.id)) THEN
  RAISE EXCEPTION 'issued worker task identity must not be reused';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_issued_identity_before BEFORE INSERT ON worker_tasks FOR EACH ROW EXECUTE FUNCTION refuse_issued_worker_identity_recreation();
CREATE TRIGGER worker_issued_identity_after AFTER INSERT ON worker_tasks FOR EACH ROW EXECUTE FUNCTION refuse_issued_worker_identity_recreation();
