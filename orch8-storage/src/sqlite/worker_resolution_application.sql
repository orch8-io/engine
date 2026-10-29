CREATE INDEX IF NOT EXISTS worker_resolution_outcomes_physical ON worker_resolution_outcomes(instance_id,id);
CREATE TABLE IF NOT EXISTS worker_resolution_integrations (
 outcome_id TEXT PRIMARY KEY REFERENCES worker_resolution_outcomes(id),tenant_id TEXT NOT NULL,instance_id TEXT NOT NULL,task_id TEXT NOT NULL,action TEXT NOT NULL,applied_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_resolution_integrations_instance ON worker_resolution_integrations(tenant_id,instance_id);
CREATE TRIGGER IF NOT EXISTS worker_resolution_integration_update BEFORE UPDATE ON worker_resolution_integrations BEGIN SELECT RAISE(ABORT,'worker failure applications must be retained unchanged'); END;
CREATE TRIGGER IF NOT EXISTS worker_resolution_integration_delete BEFORE DELETE ON worker_resolution_integrations BEGIN SELECT RAISE(ABORT,'worker failure applications must be retained unchanged'); END;
CREATE TABLE IF NOT EXISTS worker_resolution_recovery(outcome_id TEXT PRIMARY KEY REFERENCES worker_resolution_outcomes(id),turn BIGINT NOT NULL CHECK(turn>=0));
CREATE INDEX IF NOT EXISTS worker_resolution_recovery_order ON worker_resolution_recovery(turn,outcome_id);
CREATE TRIGGER IF NOT EXISTS worker_resolution_recovery_insert AFTER INSERT ON worker_resolution_outcomes
 WHEN NEW.kind='failed' AND json_extract(NEW.record,'$.action.kind')='fail_task_only'
 AND json_extract(NEW.record,'$.invocation.handler_name') IS NOT 'orch8.delegation'
 AND EXISTS(SELECT 1 FROM task_instances WHERE id=NEW.instance_id AND state IN ('paused','scheduled','waiting','running'))
 BEGIN
  SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM worker_outcome_recovery_clock WHERE singleton=1) THEN RAISE(ABORT,'worker recovery clock missing') END;
  INSERT INTO worker_resolution_recovery(outcome_id,turn) SELECT NEW.id,turn FROM worker_outcome_recovery_clock WHERE singleton=1;
 END;
INSERT OR IGNORE INTO worker_resolution_recovery(outcome_id,turn)
 SELECT r.id,0 FROM task_instances n CROSS JOIN worker_resolution_outcomes r INDEXED BY worker_resolution_outcomes_physical
 WHERE r.instance_id=n.id AND n.state IN ('paused','scheduled','waiting','running') AND r.kind='failed'
 AND json_extract(r.record,'$.action.kind')='fail_task_only' AND json_extract(r.record,'$.invocation.handler_name') IS NOT 'orch8.delegation';
CREATE TRIGGER IF NOT EXISTS worker_resolution_cut_insert BEFORE INSERT ON capsule_source_cuts
 WHEN NEW.state IN ('captured','sealed') AND EXISTS(SELECT 1 FROM worker_resolution_outcomes r JOIN worker_resolution_recovery q ON q.outcome_id=r.id WHERE r.instance_id=NEW.source_instance_id)
 BEGIN SELECT RAISE(ABORT,'unapplied worker failure prevents capsule cut'); END;
CREATE TRIGGER IF NOT EXISTS worker_resolution_cut_update BEFORE UPDATE OF state ON capsule_source_cuts
 WHEN NEW.state IN ('captured','sealed') AND EXISTS(SELECT 1 FROM worker_resolution_outcomes r JOIN worker_resolution_recovery q ON q.outcome_id=r.id WHERE r.instance_id=NEW.source_instance_id)
 BEGIN SELECT RAISE(ABORT,'unapplied worker failure prevents capsule cut'); END;
