-- Remove durable source-cut tables only after explicit recovery reconciliation.
-- Serialize the emptiness check with capture, publication and history writers.
LOCK TABLE continuity_executions, task_instances, capsule_source_cuts, capsule_artifact_retirements IN ACCESS EXCLUSIVE MODE;
DO $$ BEGIN
 -- Resolve dependencies in the schema of the locked source-cut table.
 -- A later search_path schema may contain an independent installation.
 IF EXISTS (SELECT 1 FROM pg_class dependency
            JOIN pg_class source ON source.oid='capsule_source_cuts'::regclass
            WHERE dependency.relnamespace=source.relnamespace
              AND dependency.relname='physical_instance_retirements') THEN
  RAISE EXCEPTION 'Roll back physical instance retirement migration first; source recording guards must survive';
 END IF;
 IF EXISTS(SELECT 1 FROM capsule_source_cuts LIMIT 1) OR EXISTS(SELECT 1 FROM capsule_artifact_retirements LIMIT 1) THEN
  RAISE EXCEPTION 'Cannot roll back capsule source cuts while recovery or deduplication evidence exists; drain and explicitly reconcile exports first';
 END IF;
END $$;
DROP INDEX IF EXISTS idx_capsule_source_cut_instance;
DROP INDEX IF EXISTS idx_capsule_source_cut_active;
DROP TABLE IF EXISTS capsule_source_cuts;
DROP INDEX IF EXISTS idx_capsule_artifact_retirement_due;
DROP TABLE IF EXISTS capsule_artifact_retirements;
