CREATE INDEX IF NOT EXISTS worker_accepted_outcomes_physical ON worker_accepted_outcomes(instance_id,task_id);
CREATE TABLE IF NOT EXISTS worker_outcome_integrations (
 task_id TEXT PRIMARY KEY REFERENCES worker_accepted_outcomes(task_id),
 tenant_id TEXT NOT NULL,
 instance_id TEXT NOT NULL,
 output_id TEXT NOT NULL,
 applied_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS worker_outcome_integrations_instance ON worker_outcome_integrations(tenant_id,instance_id);
CREATE TRIGGER IF NOT EXISTS worker_integration_update BEFORE UPDATE ON worker_outcome_integrations
 BEGIN SELECT RAISE(ABORT,'worker integrations must be retained unchanged'); END;
CREATE TRIGGER IF NOT EXISTS worker_integration_delete BEFORE DELETE ON worker_outcome_integrations
 BEGIN SELECT RAISE(ABORT,'worker integrations must be retained unchanged'); END;

CREATE TABLE IF NOT EXISTS worker_outcome_recovery (
 task_id TEXT PRIMARY KEY REFERENCES worker_accepted_outcomes(task_id),
 turn BIGINT NOT NULL DEFAULT 0 CHECK(turn >= 0)
);
CREATE INDEX IF NOT EXISTS worker_outcome_recovery_order ON worker_outcome_recovery(turn,task_id);
INSERT OR IGNORE INTO worker_outcome_recovery(task_id,turn)
 SELECT a.task_id,0 FROM worker_accepted_outcomes a
 WHERE NOT EXISTS(SELECT 1 FROM worker_outcome_integrations i WHERE i.task_id=a.task_id);
-- Recovery rotation must not depend on a mobile device's wall clock.
CREATE TABLE IF NOT EXISTS worker_outcome_recovery_clock (
 singleton INTEGER PRIMARY KEY CHECK(singleton=1),
 turn BIGINT NOT NULL CHECK(turn >= 0)
);
INSERT OR IGNORE INTO worker_outcome_recovery_clock(singleton,turn) VALUES(1,0);

CREATE TRIGGER IF NOT EXISTS worker_outcome_recovery_insert AFTER INSERT ON worker_accepted_outcomes
 BEGIN
  SELECT CASE WHEN NOT EXISTS(SELECT 1 FROM worker_outcome_recovery_clock WHERE singleton=1)
   THEN RAISE(ABORT,'worker outcome recovery clock missing') END;
  INSERT INTO worker_outcome_recovery(task_id,turn)
   SELECT NEW.task_id,turn FROM worker_outcome_recovery_clock WHERE singleton=1;
 END;
CREATE TRIGGER IF NOT EXISTS worker_outcome_native_delete BEFORE DELETE ON task_instances
 WHEN EXISTS(SELECT 1 FROM worker_accepted_outcomes a WHERE a.instance_id=OLD.id
  AND NOT EXISTS(SELECT 1 FROM worker_outcome_integrations i WHERE i.task_id=a.task_id))
 BEGIN SELECT RAISE(ABORT,'unapplied worker outcome retains native history'); END;
CREATE TRIGGER IF NOT EXISTS worker_outcome_cut_insert BEFORE INSERT ON capsule_source_cuts
 WHEN NEW.state IN ('captured','sealed') AND EXISTS(
  SELECT 1 FROM worker_accepted_outcomes a WHERE a.instance_id=NEW.source_instance_id
   AND NOT EXISTS(SELECT 1 FROM worker_outcome_integrations i WHERE i.task_id=a.task_id))
 BEGIN SELECT RAISE(ABORT,'unapplied worker outcome prevents capsule cut'); END;
CREATE TRIGGER IF NOT EXISTS worker_outcome_cut_update BEFORE UPDATE OF state ON capsule_source_cuts
 WHEN NEW.state IN ('captured','sealed') AND EXISTS(
  SELECT 1 FROM worker_accepted_outcomes a WHERE a.instance_id=NEW.source_instance_id
   AND NOT EXISTS(SELECT 1 FROM worker_outcome_integrations i WHERE i.task_id=a.task_id))
 BEGIN SELECT RAISE(ABORT,'unapplied worker outcome prevents capsule cut'); END;
