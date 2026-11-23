DROP TRIGGER IF EXISTS legacy_worker_effect_fence ON worker_tasks;
DROP FUNCTION IF EXISTS fence_legacy_worker_effect();
DROP FUNCTION IF EXISTS legacy_worker_effect_may_have_run(UUID, TEXT, SMALLINT);
-- Keep quarantined work fenced until its original effect evidence is resolved.
