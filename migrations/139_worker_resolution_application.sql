-- Retained failures must apply once after pause/resume, independently of hot tasks.
CREATE INDEX worker_resolution_outcomes_physical ON worker_resolution_outcomes(instance_id,id);
CREATE TABLE worker_resolution_integrations (
 outcome_id UUID PRIMARY KEY REFERENCES worker_resolution_outcomes(id),
 tenant_id TEXT NOT NULL, instance_id UUID NOT NULL, task_id UUID NOT NULL,
 action JSONB NOT NULL, applied_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX worker_resolution_integrations_instance ON worker_resolution_integrations(tenant_id,instance_id);
CREATE TRIGGER worker_resolution_integration_update BEFORE UPDATE ON worker_resolution_integrations
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_resolution_outcome();
CREATE TRIGGER worker_resolution_integration_delete BEFORE DELETE ON worker_resolution_integrations
 FOR EACH ROW EXECUTE FUNCTION preserve_worker_resolution_outcome();
CREATE TABLE worker_resolution_recovery (
 outcome_id UUID PRIMARY KEY REFERENCES worker_resolution_outcomes(id),
 turn BIGINT NOT NULL CHECK(turn>=0)
);
CREATE INDEX worker_resolution_recovery_order ON worker_resolution_recovery(turn,outcome_id);
CREATE FUNCTION enqueue_worker_resolution() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.kind='failed' AND NEW.record->'action'->>'kind'='fail_task_only'
  AND NEW.record->'invocation'->>'handler_name' IS DISTINCT FROM 'orch8.delegation'
  AND EXISTS(SELECT 1 FROM task_instances WHERE id=NEW.instance_id AND state IN ('paused','scheduled','waiting','running')) THEN
  IF NOT EXISTS(SELECT 1 FROM worker_outcome_recovery_clock WHERE singleton=1) THEN RAISE EXCEPTION 'worker recovery clock missing'; END IF;
  INSERT INTO worker_resolution_recovery(outcome_id,turn)
   SELECT NEW.id,turn FROM worker_outcome_recovery_clock WHERE singleton=1;
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_resolution_recovery_insert AFTER INSERT ON worker_resolution_outcomes
 FOR EACH ROW EXECUTE FUNCTION enqueue_worker_resolution();
INSERT INTO worker_resolution_recovery(outcome_id,turn)
 SELECT r.id,0 FROM task_instances n CROSS JOIN LATERAL (
  SELECT id FROM worker_resolution_outcomes r WHERE r.instance_id=n.id AND r.kind='failed'
   AND r.record->'action'->>'kind'='fail_task_only'
   AND r.record->'invocation'->>'handler_name' IS DISTINCT FROM 'orch8.delegation' OFFSET 0
 ) r WHERE n.state IN ('paused','scheduled','waiting','running');
CREATE FUNCTION refuse_pending_worker_resolution_cut() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
 IF NEW.state IN ('captured','sealed') AND EXISTS(
  SELECT 1 FROM worker_resolution_outcomes r JOIN worker_resolution_recovery q ON q.outcome_id=r.id
   WHERE r.instance_id=NEW.source_instance_id) THEN
  RAISE EXCEPTION 'unapplied worker failure prevents capsule cut';
 END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER worker_resolution_cut_insert BEFORE INSERT ON capsule_source_cuts
 FOR EACH ROW EXECUTE FUNCTION refuse_pending_worker_resolution_cut();
CREATE TRIGGER worker_resolution_cut_update BEFORE UPDATE OF state ON capsule_source_cuts
 FOR EACH ROW EXECUTE FUNCTION refuse_pending_worker_resolution_cut();
