-- Page saved reports without scanning unrelated execution provenance.
-- On a large live database, prebuild the same index before running migrations:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_residency_evaluation_inventory
--       ON provenance_entries (tenant_id, continuity_id, created_at DESC, id DESC)
--       WHERE jsonb_typeof(record -> 'residency_evaluation') = 'object';
CREATE INDEX IF NOT EXISTS idx_residency_evaluation_inventory
    ON provenance_entries (tenant_id, continuity_id, created_at DESC, id DESC)
    WHERE jsonb_typeof(record -> 'residency_evaluation') = 'object';
