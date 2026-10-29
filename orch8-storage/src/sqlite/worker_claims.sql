CREATE TABLE IF NOT EXISTS worker_issued_claims (
 task_id TEXT NOT NULL,claim_epoch BIGINT NOT NULL CHECK(claim_epoch>0),
 tenant_id TEXT NOT NULL,instance_id TEXT NOT NULL,worker_id TEXT NOT NULL,
 record TEXT NOT NULL,issued_at TEXT NOT NULL,PRIMARY KEY(task_id,claim_epoch)
);
CREATE INDEX IF NOT EXISTS worker_issued_claims_instance ON worker_issued_claims(tenant_id,instance_id);
CREATE TRIGGER IF NOT EXISTS worker_issued_claim_update BEFORE UPDATE ON worker_issued_claims BEGIN SELECT RAISE(ABORT,'issued worker claims must be retained unchanged'); END;
CREATE TRIGGER IF NOT EXISTS worker_issued_claim_delete BEFORE DELETE ON worker_issued_claims BEGIN SELECT RAISE(ABORT,'issued worker claims must be retained unchanged'); END;
CREATE TRIGGER IF NOT EXISTS worker_issued_identity_before BEFORE INSERT ON worker_tasks
 WHEN EXISTS(SELECT 1 FROM worker_issued_claims WHERE task_id=NEW.id) AND NOT EXISTS(SELECT 1 FROM worker_tasks WHERE id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'issued worker task identity must not be reused'); END;
CREATE TRIGGER IF NOT EXISTS worker_issued_identity_after AFTER INSERT ON worker_tasks
 WHEN EXISTS(SELECT 1 FROM worker_issued_claims WHERE task_id=NEW.id)
 BEGIN SELECT RAISE(ABORT,'issued worker task identity must not be reused'); END;
