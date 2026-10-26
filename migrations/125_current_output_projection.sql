-- Seek one current substantive result per block without reading loop history.
-- Large installations can pre-create this identical index online:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_block_outputs_current
--     ON block_outputs(instance_id, block_id, created_at DESC, id DESC)
--     WHERE (output_ref IS NULL OR output_ref NOT IN ('__retry__', '__in_progress__'))
--       AND substr(block_id, 1, 13) <> '_interceptor:'
--       AND substr(block_id, 1, 5) <> '_sla:';
CREATE INDEX IF NOT EXISTS idx_block_outputs_current
    ON block_outputs(instance_id, block_id, created_at DESC, id DESC)
    WHERE (output_ref IS NULL OR output_ref NOT IN ('__retry__', '__in_progress__'))
      AND substr(block_id, 1, 13) <> '_interceptor:'
      AND substr(block_id, 1, 5) <> '_sla:';
