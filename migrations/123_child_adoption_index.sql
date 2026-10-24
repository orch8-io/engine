-- Narrow historical adoption lookup; cancelled/superseded children are excluded.
-- Not CONCURRENTLY inside sqlx (see migrations/README.md). Large installs may
-- pre-create this identical index online, outside a transaction:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_child_adoption
--     ON task_instances(parent_instance_id, md5(metadata->>'_parent_block_id'), md5(CASE WHEN strpos(metadata->>'_parent_activation', '/') = 0 THEN '' ELSE substr(metadata->>'_parent_activation', strpos(metadata->>'_parent_activation', '/') + 1) END), created_at DESC, id DESC)
--     WHERE parent_instance_id IS NOT NULL AND jsonb_typeof(metadata->'_parent_block_id') = 'string'
--       AND jsonb_typeof(metadata->'_parent_activation') = 'string'
--       AND NOT (metadata ? '_superseded') AND state <> 'cancelled';
CREATE INDEX IF NOT EXISTS idx_child_adoption
    ON task_instances(parent_instance_id, md5(metadata->>'_parent_block_id'), md5(CASE WHEN strpos(metadata->>'_parent_activation', '/') = 0 THEN '' ELSE substr(metadata->>'_parent_activation', strpos(metadata->>'_parent_activation', '/') + 1) END), created_at DESC, id DESC)
    WHERE parent_instance_id IS NOT NULL AND jsonb_typeof(metadata->'_parent_block_id') = 'string'
      AND jsonb_typeof(metadata->'_parent_activation') = 'string'
      AND NOT (metadata ? '_superseded') AND state <> 'cancelled';
