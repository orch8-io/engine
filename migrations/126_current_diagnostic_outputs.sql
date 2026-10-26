-- Current snapshots retain retry diagnostics but skip obsolete loop payloads.
-- For large installations, prebuild matching leaf indexes concurrently using
-- scripts/sql/prebuild-current-output-indexes.psql before upgrading. PostgreSQL
-- cannot build a partitioned parent index concurrently; this parent statement
-- attaches the already-valid matching leaf indexes. See migrations/README.md.
CREATE INDEX IF NOT EXISTS idx_block_outputs_diagnostics
    ON block_outputs(instance_id, block_id, created_at DESC, id DESC)
    WHERE (output_ref IS NULL OR output_ref <> '__in_progress__')
      AND substr(block_id, 1, 13) <> '_interceptor:'
      AND substr(block_id, 1, 5) <> '_sla:';
