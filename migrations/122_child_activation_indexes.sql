-- Hash buckets bound index keys; reads also compare full values for correctness.
-- Bounded exact/legacy activation reads for long-running sub-sequence parents.
-- Do not use CONCURRENTLY inside sqlx migrations (see migrations/README.md).
-- Large installs can pre-create the identical indexes online, outside a
-- transaction and one statement at a time, before upgrading:
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_child_activation
--     ON task_instances(parent_instance_id, md5(metadata->>'_parent_activation'), created_at, id)
--     WHERE parent_instance_id IS NOT NULL AND jsonb_typeof(metadata->'_parent_activation') = 'string';
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_child_legacy_block
--     ON task_instances(parent_instance_id, md5(metadata->>'_parent_block_id'), created_at, id)
--     WHERE parent_instance_id IS NOT NULL AND NOT (metadata ? '_parent_activation')
--       AND jsonb_typeof(metadata->'_parent_block_id') = 'string';
CREATE INDEX IF NOT EXISTS idx_child_activation
    ON task_instances(parent_instance_id, md5(metadata->>'_parent_activation'), created_at, id)
    WHERE parent_instance_id IS NOT NULL AND jsonb_typeof(metadata->'_parent_activation') = 'string';
CREATE INDEX IF NOT EXISTS idx_child_legacy_block
    ON task_instances(parent_instance_id, md5(metadata->>'_parent_block_id'), created_at, id)
    WHERE parent_instance_id IS NOT NULL AND NOT (metadata ? '_parent_activation')
      AND jsonb_typeof(metadata->'_parent_block_id') = 'string';
