-- No hot-task/native foreign keys: a lost acknowledgement must survive their GC.
CREATE TABLE worker_accepted_outcomes (
 task_id UUID PRIMARY KEY,
 tenant_id TEXT NOT NULL,
 instance_id UUID NOT NULL,
 record JSONB NOT NULL
);
CREATE INDEX worker_accepted_outcomes_instance ON worker_accepted_outcomes(tenant_id,instance_id);
CREATE FUNCTION preserve_worker_accepted_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN RAISE EXCEPTION 'accepted worker outcomes must be retained unchanged'; END $$;
CREATE TRIGGER worker_outcome_update BEFORE UPDATE ON worker_accepted_outcomes
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_accepted_outcome();
CREATE TRIGGER worker_outcome_delete BEFORE DELETE ON worker_accepted_outcomes
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_accepted_outcome();
