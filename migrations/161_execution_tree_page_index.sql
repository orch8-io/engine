-- The workbench seeks through execution nodes by ID within one instance.
-- Large installations can prebuild this index before upgrading:
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_execution_tree_instance_id
--       ON execution_tree(instance_id, id);
CREATE INDEX IF NOT EXISTS idx_execution_tree_instance_id
    ON execution_tree(instance_id, id);
