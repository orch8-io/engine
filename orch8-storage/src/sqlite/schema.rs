pub(super) const SCHEMA: &str = r"
CREATE TABLE IF NOT EXISTS sequences (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    deprecated INTEGER NOT NULL DEFAULT 0,
    blocks TEXT NOT NULL,
    interceptors TEXT,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_instances (
    id TEXT PRIMARY KEY,
    sequence_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'scheduled',
    next_fire_at TEXT,
    priority INTEGER NOT NULL DEFAULT 1,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    metadata TEXT NOT NULL DEFAULT '{}',
    context TEXT NOT NULL DEFAULT '{}',
    concurrency_key TEXT,
    max_concurrency INTEGER,
    idempotency_key TEXT,
    session_id TEXT,
    parent_instance_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS execution_tree (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    block_id TEXT NOT NULL,
    parent_id TEXT,
    block_type TEXT NOT NULL,
    branch_index INTEGER,
    state TEXT NOT NULL DEFAULT 'pending',
    started_at TEXT,
    completed_at TEXT
);

CREATE TABLE IF NOT EXISTS block_outputs (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    block_id TEXT NOT NULL,
    output TEXT NOT NULL DEFAULT '{}',
    output_ref TEXT,
    output_size INTEGER NOT NULL DEFAULT 0,
    attempt INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS rate_limits (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    max_count INTEGER NOT NULL,
    window_seconds INTEGER NOT NULL,
    current_count INTEGER NOT NULL DEFAULT 0,
    window_start TEXT NOT NULL,
    UNIQUE(tenant_id, resource_key)
);

CREATE TABLE IF NOT EXISTS signal_inbox (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    signal_type TEXT NOT NULL,
    payload TEXT NOT NULL DEFAULT '{}',
    delivered INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    delivered_at TEXT
);

CREATE TABLE IF NOT EXISTS cron_schedules (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL,
    sequence_id TEXT NOT NULL,
    cron_expr TEXT NOT NULL,
    timezone TEXT NOT NULL DEFAULT 'UTC',
    enabled INTEGER NOT NULL DEFAULT 1,
    metadata TEXT NOT NULL DEFAULT '{}',
    next_fire_at TEXT,
    last_triggered_at TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS worker_tasks (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    block_id TEXT NOT NULL,
    handler_name TEXT NOT NULL,
    params TEXT NOT NULL DEFAULT '{}',
    context TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL DEFAULT 'pending',
    worker_id TEXT,
    queue_name TEXT,
    output TEXT,
    error_message TEXT,
    error_retryable INTEGER,
    attempt INTEGER NOT NULL DEFAULT 0,
    timeout_ms INTEGER,
    claimed_at TEXT,
    heartbeat_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL,
    UNIQUE(instance_id, block_id)
);

CREATE TABLE IF NOT EXISTS resource_pools (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    name TEXT NOT NULL,
    strategy TEXT NOT NULL DEFAULT 'round_robin',
    round_robin_index INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(tenant_id, name)
);

CREATE TABLE IF NOT EXISTS pool_resources (
    id TEXT PRIMARY KEY,
    pool_id TEXT NOT NULL,
    resource_key TEXT NOT NULL,
    name TEXT NOT NULL DEFAULT '',
    weight INTEGER NOT NULL DEFAULT 1,
    enabled INTEGER NOT NULL DEFAULT 1,
    daily_cap INTEGER NOT NULL DEFAULT 0,
    daily_usage INTEGER NOT NULL DEFAULT 0,
    daily_usage_date TEXT,
    warmup_start TEXT,
    warmup_days INTEGER NOT NULL DEFAULT 0,
    warmup_start_cap INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS checkpoints (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    checkpoint_data TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS externalized_state (
    ref_key TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    payload TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS audit_log (
    id TEXT PRIMARY KEY,
    instance_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    from_state TEXT,
    to_state TEXT,
    block_id TEXT,
    details TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT '{}',
    state TEXT NOT NULL DEFAULT 'active',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    expires_at TEXT,
    UNIQUE(tenant_id, session_key)
);

CREATE TABLE IF NOT EXISTS cluster_nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    registered_at TEXT NOT NULL,
    last_heartbeat_at TEXT NOT NULL,
    drain INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS injected_blocks (
    instance_id TEXT PRIMARY KEY,
    blocks TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS plugins (
    name TEXT PRIMARY KEY,
    plugin_type TEXT NOT NULL DEFAULT 'wasm',
    source TEXT NOT NULL,
    tenant_id TEXT NOT NULL DEFAULT '',
    enabled INTEGER NOT NULL DEFAULT 1,
    config TEXT NOT NULL DEFAULT '{}',
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS triggers (
    slug TEXT PRIMARY KEY,
    sequence_name TEXT NOT NULL,
    version INTEGER,
    tenant_id TEXT NOT NULL,
    namespace TEXT NOT NULL DEFAULT 'default',
    enabled INTEGER NOT NULL DEFAULT 1,
    secret TEXT,
    trigger_type TEXT NOT NULL DEFAULT 'webhook',
    config TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
";
