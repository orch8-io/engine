-- This record is installed in the same transaction that withdraws the
-- predecessor's claim and binds the original effect to the resumable task.
CREATE TABLE legacy_worker_checkpoint_adoptions (
    task_id UUID PRIMARY KEY REFERENCES worker_tasks(id) ON DELETE CASCADE,
    effect_id UUID NOT NULL UNIQUE
);

-- A pre-epoch coordinator may match a reused worker ID. Once authority
-- transfers, that coordinator cannot update or delete the adopted dispatch.
CREATE FUNCTION fence_adopted_worker_claim() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('orch8.worker_claim_protocol', true) IS DISTINCT FROM 'epoch_v1'
       AND EXISTS (SELECT 1 FROM legacy_worker_checkpoint_adoptions WHERE task_id = OLD.id) THEN
        RETURN NULL;
    END IF;
    IF TG_OP = 'DELETE' THEN RETURN OLD; END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER adopted_worker_claim_fence
    BEFORE UPDATE OR DELETE ON worker_tasks
    FOR EACH ROW EXECUTE FUNCTION fence_adopted_worker_claim();

-- The predecessor writes the effect separately from the worker outcome.
CREATE FUNCTION fence_adopted_worker_effect() RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    IF current_setting('orch8.worker_claim_protocol', true) IS DISTINCT FROM 'epoch_v1'
       AND EXISTS (SELECT 1 FROM legacy_worker_checkpoint_adoptions WHERE effect_id = OLD.id) THEN
        RETURN NULL;
    END IF;
    RETURN NEW;
END;
$$;
CREATE TRIGGER adopted_worker_effect_fence BEFORE UPDATE ON effect_receipts
    FOR EACH ROW EXECUTE FUNCTION fence_adopted_worker_effect();
