-- Never discard live idempotency bindings as part of a rollback.
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM effect_attempt_bindings LIMIT 1)
       OR EXISTS (SELECT 1 FROM effect_block_resets LIMIT 1) THEN
        RAISE EXCEPTION 'Cannot roll back effect identities while bindings exist; drain and explicitly retire the associated receipts first';
    END IF;
END $$;
DROP INDEX IF EXISTS idx_effect_attempt_bindings_receipt;
DROP INDEX IF EXISTS idx_effect_attempt_bindings_instance;
DROP TABLE IF EXISTS effect_attempt_bindings;

DROP TABLE IF EXISTS effect_block_resets;
