DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM legacy_worker_checkpoint_adoptions) THEN
        RAISE EXCEPTION 'cannot remove the epoch fence while adopted legacy tasks are retained';
    END IF;
END;
$$;

DROP TRIGGER adopted_worker_effect_fence ON effect_receipts;
DROP FUNCTION fence_adopted_worker_effect();
DROP TRIGGER adopted_worker_claim_fence ON worker_tasks;
DROP FUNCTION fence_adopted_worker_claim();
DROP TABLE legacy_worker_checkpoint_adoptions;
