CREATE TABLE IF NOT EXISTS worker_accepted_outcomes (
 task_id TEXT PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,
 record TEXT NOT NULL CHECK(json_valid(record))
);
CREATE INDEX IF NOT EXISTS worker_accepted_outcomes_instance ON worker_accepted_outcomes(tenant_id,instance_id);
CREATE TRIGGER IF NOT EXISTS worker_outcome_update BEFORE UPDATE ON worker_accepted_outcomes
 BEGIN SELECT RAISE(ABORT,'accepted worker outcomes must be retained unchanged'); END;
CREATE TRIGGER IF NOT EXISTS worker_outcome_delete BEFORE DELETE ON worker_accepted_outcomes
 BEGIN SELECT RAISE(ABORT,'accepted worker outcomes must be retained unchanged'); END;
