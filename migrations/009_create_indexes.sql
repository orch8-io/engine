-- Scheduling hot path
CREATE INDEX IF NOT EXISTS idx_instances_fire
    ON task_instances (next_fire_at)
    WHERE state = 'scheduled';

CREATE INDEX IF NOT EXISTS idx_instances_tenant
    ON task_instances (tenant_id, state);

CREATE INDEX IF NOT EXISTS idx_instances_sequence
    ON task_instances (sequence_id, state);

CREATE INDEX IF NOT EXISTS idx_instances_namespace
    ON task_instances (namespace, state);

-- Metadata queries (replaces Temporal search attributes)
CREATE INDEX IF NOT EXISTS idx_instances_metadata
    ON task_instances USING GIN (metadata jsonb_path_ops);

-- Orchestration
CREATE INDEX IF NOT EXISTS idx_exec_tree_instance
    ON execution_tree (instance_id, state);

CREATE INDEX IF NOT EXISTS idx_exec_tree_parent
    ON execution_tree (parent_id)
    WHERE parent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_block_outputs_instance
    ON block_outputs (instance_id, block_id);

CREATE INDEX IF NOT EXISTS idx_signal_inbox_pending
    ON signal_inbox (instance_id)
    WHERE delivered = FALSE;

-- Rate limiting
CREATE INDEX IF NOT EXISTS idx_rate_limits_key
    ON rate_limits (tenant_id, resource_key);

-- Externalized state cleanup
CREATE INDEX IF NOT EXISTS idx_externalized_expires
    ON externalized_state (expires_at)
    WHERE expires_at IS NOT NULL;
