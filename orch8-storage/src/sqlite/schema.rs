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
    payload TEXT,                    -- raw JSON when compression IS NULL
    payload_bytes BLOB,              -- zstd-compressed JSON when compression = 'zstd'
    compression TEXT,                -- NULL or 'zstd'
    size_bytes INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT,
    -- Cascade deletes so instance teardown atomically drops its payloads.
    -- Only enforced when the connection has `PRAGMA foreign_keys = ON`
    -- (set in SqliteConnectOptions; without it SQLite silently ignores the
    -- constraint and we'd leak rows after instance deletion).
    FOREIGN KEY (instance_id) REFERENCES task_instances(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_externalized_state_instance
    ON externalized_state(instance_id);

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

CREATE TABLE IF NOT EXISTS credentials (
    id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL DEFAULT '',
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'api_key',
    value TEXT NOT NULL,
    expires_at TEXT,
    refresh_url TEXT,
    refresh_token TEXT,
    enabled INTEGER NOT NULL DEFAULT 1,
    description TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_credentials_tenant ON credentials(tenant_id);
CREATE INDEX IF NOT EXISTS idx_credentials_expires ON credentials(expires_at);

CREATE INDEX IF NOT EXISTS idx_task_instances_state_fire ON task_instances(state, next_fire_at);
CREATE UNIQUE INDEX IF NOT EXISTS idx_task_instances_tenant_idemp ON task_instances(tenant_id, idempotency_key) WHERE idempotency_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_task_instances_concurrency ON task_instances(concurrency_key, state);
CREATE INDEX IF NOT EXISTS idx_task_instances_parent ON task_instances(parent_instance_id);
CREATE INDEX IF NOT EXISTS idx_task_instances_session ON task_instances(session_id);
CREATE INDEX IF NOT EXISTS idx_signal_inbox_instance ON signal_inbox(instance_id, delivered);
CREATE INDEX IF NOT EXISTS idx_worker_tasks_handler ON worker_tasks(handler_name, state);
CREATE INDEX IF NOT EXISTS idx_block_outputs_instance ON block_outputs(instance_id);
CREATE INDEX IF NOT EXISTS idx_execution_tree_instance ON execution_tree(instance_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_instance ON audit_log(instance_id);
CREATE INDEX IF NOT EXISTS idx_audit_log_tenant ON audit_log(tenant_id);

-- R7: scope_kind + scope_value columns replace the old parent_instance_id
-- so the handler can pick per-parent or per-tenant dedupe at call time.
-- Both scopes live in one table → one GC sweeper covers both. scope_kind
-- is part of the PK so a 'parent' and a 'tenant' row with the same
-- scope_value and dedupe_key are distinct namespaces.
-- Only `Open` circuit breaker rows are persisted — a correctness backstop
-- so a crash mid-cooldown doesn't reset every tripped breaker. Per-tenant
-- isolation via the composite PK.
CREATE TABLE IF NOT EXISTS circuit_breakers (
    tenant_id TEXT NOT NULL,
    handler TEXT NOT NULL,
    state TEXT NOT NULL,
    failure_count INTEGER NOT NULL,
    failure_threshold INTEGER NOT NULL,
    cooldown_secs INTEGER NOT NULL,
    opened_at TEXT,
    PRIMARY KEY (tenant_id, handler)
);

CREATE TABLE IF NOT EXISTS emit_event_dedupe (
    scope_kind TEXT NOT NULL,
    scope_value TEXT NOT NULL,
    dedupe_key TEXT NOT NULL,
    child_instance_id TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    PRIMARY KEY (scope_kind, scope_value, dedupe_key)
);
CREATE INDEX IF NOT EXISTS idx_emit_event_dedupe_created_at
    ON emit_event_dedupe(created_at);

-- Ref#15: lightweight schema version registry. Full per-migration history
-- isn't implemented yet; this table at least records which version of the
-- bundled `SCHEMA` string was applied so downgrades / skew can be detected
-- and surfaced in logs at boot time.
CREATE TABLE IF NOT EXISTS schema_versions (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL DEFAULT (datetime('now'))
);
";

/// Current bundled schema version. Bump when the `SCHEMA` string above is
/// edited in a non-idempotent way (e.g. adding a new column whose default
/// matters for code that reads the column).
pub(super) const SCHEMA_VERSION: i64 = 1;
