LOCK TABLE worker_accepted_outcomes IN SHARE ROW EXCLUSIVE MODE;
-- Separate from acceptance: proof that this exact result advanced its native invocation.
CREATE INDEX worker_accepted_outcomes_physical ON worker_accepted_outcomes(instance_id,task_id);
CREATE TABLE worker_outcome_integrations (
 task_id UUID PRIMARY KEY REFERENCES worker_accepted_outcomes(task_id),
 tenant_id TEXT NOT NULL,
 instance_id UUID NOT NULL,
 output_id UUID NOT NULL,
 applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX worker_outcome_integrations_instance ON worker_outcome_integrations(tenant_id,instance_id);
CREATE TRIGGER worker_integration_update BEFORE UPDATE ON worker_outcome_integrations
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_accepted_outcome();
CREATE TRIGGER worker_integration_delete BEFORE DELETE ON worker_outcome_integrations
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_accepted_outcome();

CREATE TABLE worker_outcome_recovery (
 task_id UUID PRIMARY KEY REFERENCES worker_accepted_outcomes(task_id),
 turn BIGINT NOT NULL DEFAULT 0 CHECK(turn >= 0)
);
CREATE INDEX worker_outcome_recovery_order ON worker_outcome_recovery(turn,task_id);
INSERT INTO worker_outcome_recovery(task_id,turn)
 SELECT a.task_id,0 FROM worker_accepted_outcomes a
 WHERE NOT EXISTS(SELECT 1 FROM worker_outcome_integrations i WHERE i.task_id=a.task_id) ON CONFLICT DO NOTHING;
-- Recovery rotation must not depend on a mobile device's wall clock.
CREATE TABLE worker_outcome_recovery_clock (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 turn BIGINT NOT NULL CHECK(turn >= 0)
);
INSERT INTO worker_outcome_recovery_clock(singleton,turn) VALUES(1,0) ON CONFLICT DO NOTHING;

-- Queue population is part of journal insertion, including supported older writers.
CREATE FUNCTION queue_accepted_worker_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 INSERT INTO worker_outcome_recovery(task_id,turn)
 SELECT NEW.task_id,turn FROM worker_outcome_recovery_clock WHERE singleton=1;
 IF NOT FOUND THEN RAISE EXCEPTION 'worker outcome recovery clock missing'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_outcome_recovery_insert AFTER INSERT ON worker_accepted_outcomes
 FOR EACH ROW EXECUTE FUNCTION queue_accepted_worker_outcome();
CREATE FUNCTION retain_unapplied_worker_outcome_source() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM worker_accepted_outcomes a WHERE a.instance_id=OLD.id
  AND NOT EXISTS(SELECT 1 FROM worker_outcome_integrations i WHERE i.task_id=a.task_id)) THEN
  RAISE EXCEPTION 'unapplied worker outcome retains native history';
 END IF;
 RETURN OLD;
END $$;
CREATE TRIGGER worker_outcome_native_delete BEFORE DELETE ON task_instances
 FOR EACH ROW EXECUTE FUNCTION retain_unapplied_worker_outcome_source();
CREATE FUNCTION refuse_cut_with_unapplied_worker_outcome() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state IN ('captured','sealed') AND EXISTS(
  SELECT 1 FROM worker_accepted_outcomes a WHERE a.instance_id=NEW.source_instance_id
   AND NOT EXISTS(SELECT 1 FROM worker_outcome_integrations i WHERE i.task_id=a.task_id)) THEN
  RAISE EXCEPTION 'unapplied worker outcome prevents capsule cut';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_outcome_cut_insert BEFORE INSERT OR UPDATE OF state ON capsule_source_cuts
 FOR EACH ROW EXECUTE FUNCTION refuse_cut_with_unapplied_worker_outcome();
