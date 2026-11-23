-- A pre-binding worker task can still have a dispatched receipt. During
-- overlap the old reaper only checks timestamps and can otherwise put that
-- task back on the queue without resolving the effect.
CREATE FUNCTION legacy_worker_effect_may_have_run(native_id UUID, step_id TEXT, step_attempt SMALLINT)
RETURNS BOOLEAN LANGUAGE sql STABLE AS $$
    SELECT EXISTS (
        SELECT 1
        FROM effect_receipts receipt
        JOIN task_instances instance ON instance.id = native_id AND instance.tenant_id = receipt.tenant_id
        WHERE receipt.instance_id = native_id
          AND receipt.record->>'block_id' = step_id
          AND receipt.record->>'attempt' = step_attempt::text
          AND receipt.state IN ('dispatched', 'unknown')
    )
$$;

-- Do not offer an older unbound task that was already reset by a pre-upgrade
-- worker reaper. Keep the row and its evidence for explicit reconciliation.
-- Hold the trigger's table lock through the backfill so an old reaper cannot
-- reset another row between the check and installation of the fence.
LOCK TABLE worker_tasks IN SHARE ROW EXCLUSIVE MODE;
UPDATE worker_tasks
SET awaiting_dispatch = TRUE
WHERE state = 'pending' AND NOT awaiting_dispatch AND effect_id IS NULL
  AND legacy_worker_effect_may_have_run(instance_id, block_id, attempt);

CREATE FUNCTION fence_legacy_worker_effect() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.effect_id IS NOT NULL OR NEW.state NOT IN ('pending', 'claimed') THEN
        RETURN NEW;
    END IF;

    IF TG_OP = 'UPDATE' AND OLD.state = 'claimed' AND NEW.state = 'claimed' THEN
        RETURN NEW;
    END IF;

    IF NOT legacy_worker_effect_may_have_run(NEW.instance_id, NEW.block_id, NEW.attempt) THEN
        RETURN NEW;
    END IF;

    -- Refuse a new claim or unverified release while leaving the original
    -- authority available to report the recorded attempt's outcome.
    IF NEW.state = 'claimed' OR (TG_OP = 'UPDATE' AND OLD.state = 'claimed') THEN
        RETURN NULL;
    END IF;

    NEW.awaiting_dispatch := TRUE;
    RETURN NEW;
END;
$$;

CREATE TRIGGER legacy_worker_effect_fence
    BEFORE INSERT OR UPDATE OF state, effect_id, awaiting_dispatch ON worker_tasks
    FOR EACH ROW EXECUTE FUNCTION fence_legacy_worker_effect();
