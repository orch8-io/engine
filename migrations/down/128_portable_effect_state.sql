-- Never discard durable receipt/policy import evidence during downgrade.
DO $$ BEGIN
 IF EXISTS(SELECT 1 FROM capsule_effect_imports LIMIT 1) OR EXISTS(SELECT 1 FROM continuity_effect_guards LIMIT 1)
    OR EXISTS(SELECT 1 FROM effect_attempt_bindings b JOIN effect_receipts e ON e.id=b.effect_id WHERE b.instance_id<>e.instance_id OR b.epoch<>(e.record->>'epoch')::bigint) THEN
  RAISE EXCEPTION 'Cannot roll back portable effects while recovery proofs or frozen guards exist; drain and explicitly retire executions first';
 END IF;
END $$;
DROP INDEX IF EXISTS idx_capsule_effect_imports_instance;
DROP TABLE IF EXISTS capsule_effect_imports;
DROP TABLE IF EXISTS continuity_effect_guards;
