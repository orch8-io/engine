-- Retention checks and receipt locking must not scan a tenant's whole history.
-- Large installations may pre-create these online before the migration:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_effect_receipts_instance_gc
--     ON effect_receipts(tenant_id, instance_id, state);
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_effect_receipts_ambiguous_gc
--     ON effect_receipts(tenant_id, continuity_id)
--     WHERE state IN ('dispatched', 'unknown');
CREATE INDEX IF NOT EXISTS idx_effect_receipts_instance_gc
    ON effect_receipts(tenant_id, instance_id, state);
CREATE INDEX IF NOT EXISTS idx_effect_receipts_ambiguous_gc
    ON effect_receipts(tenant_id, continuity_id)
    WHERE state IN ('dispatched', 'unknown');
