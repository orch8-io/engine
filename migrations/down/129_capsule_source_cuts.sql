-- Remove durable source-cut tables only after explicit recovery reconciliation.
DO $$ BEGIN
 IF to_regclass('physical_instance_retirements') IS NOT NULL THEN
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
