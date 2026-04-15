//! In-memory SQLite implementation of `StorageBackend` for testing.
//!
//! Usage:
//! ```ignore
//! let storage = SqliteStorage::in_memory().await.unwrap();
//! // Use storage as Arc<dyn StorageBackend>
//! ```
#![allow(
    clippy::cast_possible_wrap,
    clippy::cast_sign_loss,
    clippy::cast_possible_truncation,
    clippy::cast_lossless,
    clippy::too_many_lines,
    clippy::wildcard_imports,
    clippy::doc_markdown,
    clippy::format_push_string,
    clippy::option_if_let_else
)]

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::{Row, SqlitePool};
use std::collections::HashMap;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::audit::AuditLogEntry;
use orch8_types::checkpoint::Checkpoint;
use orch8_types::cluster::{ClusterNode, NodeStatus};
use orch8_types::context::ExecutionContext;
use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::execution::{BlockType, ExecutionNode, NodeState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::*;
use orch8_types::instance::{InstanceState, Priority, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::pool::{PoolResource, ResourcePool};
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};
use orch8_types::session::{Session, SessionState};
use orch8_types::signal::Signal;
use orch8_types::worker::WorkerTask;

use crate::StorageBackend;

/// In-memory SQLite storage backend for testing.
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    /// Create a new in-memory SQLite storage with all tables.
    pub async fn in_memory() -> Result<Self, StorageError> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .create_if_missing(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1) // SQLite in-memory requires single connection
            .connect_with(opts)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let storage = Self { pool };
        storage.create_tables().await?;
        Ok(storage)
    }

    async fn create_tables(&self) -> Result<(), StorageError> {
        sqlx::query(SCHEMA)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }
}

const SCHEMA: &str = r"
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
";

// ─── Helpers ───────────────────────────────────────────────────

fn ts(dt: DateTime<Utc>) -> String {
    dt.to_rfc3339()
}

fn parse_ts(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s)
        .map(|dt| dt.with_timezone(&Utc))
        .unwrap_or_default()
}

fn parse_ts_opt(s: Option<String>) -> Option<DateTime<Utc>> {
    s.map(|v| parse_ts(&v))
}

fn parse_state(s: &str) -> InstanceState {
    match s {
        "running" => InstanceState::Running,
        "waiting" => InstanceState::Waiting,
        "paused" => InstanceState::Paused,
        "completed" => InstanceState::Completed,
        "failed" => InstanceState::Failed,
        "cancelled" => InstanceState::Cancelled,
        _ => InstanceState::Scheduled,
    }
}

fn parse_node_state(s: &str) -> NodeState {
    match s {
        "running" => NodeState::Running,
        "waiting" => NodeState::Waiting,
        "completed" => NodeState::Completed,
        "failed" => NodeState::Failed,
        "cancelled" => NodeState::Cancelled,
        "skipped" => NodeState::Skipped,
        _ => NodeState::Pending,
    }
}

fn parse_block_type(s: &str) -> BlockType {
    match s {
        "parallel" => BlockType::Parallel,
        "race" => BlockType::Race,
        "loop" => BlockType::Loop,
        "for_each" => BlockType::ForEach,
        "router" => BlockType::Router,
        "try_catch" => BlockType::TryCatch,
        "sub_sequence" => BlockType::SubSequence,
        "ab_split" => BlockType::ABSplit,
        _ => BlockType::Step,
    }
}

fn parse_priority(v: i64) -> Priority {
    match v {
        0 => Priority::Low,
        2 => Priority::High,
        3 => Priority::Critical,
        _ => Priority::Normal,
    }
}

fn row_to_instance(row: &sqlx::sqlite::SqliteRow) -> TaskInstance {
    TaskInstance {
        id: InstanceId(Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default()),
        sequence_id: SequenceId(Uuid::parse_str(row.get::<&str, _>("sequence_id")).unwrap_or_default()),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        namespace: Namespace(row.get::<String, _>("namespace")),
        state: parse_state(row.get::<&str, _>("state")),
        next_fire_at: parse_ts_opt(row.get::<Option<String>, _>("next_fire_at")),
        priority: parse_priority(row.get::<i64, _>("priority")),
        timezone: row.get::<String, _>("timezone"),
        metadata: serde_json::from_str(row.get::<&str, _>("metadata")).unwrap_or_default(),
        context: serde_json::from_str(row.get::<&str, _>("context")).unwrap_or_default(),
        concurrency_key: row.get::<Option<String>, _>("concurrency_key"),
        max_concurrency: row.get::<Option<i32>, _>("max_concurrency"),
        idempotency_key: row.get::<Option<String>, _>("idempotency_key"),
        session_id: row.get::<Option<String>, _>("session_id").and_then(|s| Uuid::parse_str(&s).ok()),
        parent_instance_id: row.get::<Option<String>, _>("parent_instance_id").and_then(|s| Uuid::parse_str(&s).ok()).map(InstanceId),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
    }
}

fn row_to_node(row: &sqlx::sqlite::SqliteRow) -> ExecutionNode {
    ExecutionNode {
        id: ExecutionNodeId(Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default()),
        instance_id: InstanceId(Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default()),
        block_id: BlockId(row.get::<String, _>("block_id")),
        parent_id: row.get::<Option<String>, _>("parent_id").and_then(|s| Uuid::parse_str(&s).ok()).map(ExecutionNodeId),
        block_type: parse_block_type(row.get::<&str, _>("block_type")),
        branch_index: row.get::<Option<i32>, _>("branch_index").map(|v| v as i16),
        state: parse_node_state(row.get::<&str, _>("state")),
        started_at: parse_ts_opt(row.get::<Option<String>, _>("started_at")),
        completed_at: parse_ts_opt(row.get::<Option<String>, _>("completed_at")),
    }
}

// ─── StorageBackend impl ───────────────────────────────────────

#[async_trait]
impl StorageBackend for SqliteStorage {
    // === Sequences ===

    async fn create_sequence(&self, seq: &orch8_types::sequence::SequenceDefinition) -> Result<(), StorageError> {
        let blocks = serde_json::to_string(&seq.blocks)?;
        let interceptors = seq.interceptors.as_ref().map(|i| serde_json::to_string(i).unwrap_or_default());
        sqlx::query(
            "INSERT INTO sequences (id, tenant_id, namespace, name, version, deprecated, blocks, interceptors, created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
        )
        .bind(seq.id.0.to_string())
        .bind(&seq.tenant_id.0)
        .bind(&seq.namespace.0)
        .bind(&seq.name)
        .bind(seq.version)
        .bind(seq.deprecated as i32)
        .bind(&blocks)
        .bind(&interceptors)
        .bind(ts(seq.created_at))
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_sequence(&self, id: SequenceId) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
        let row = sqlx::query("SELECT * FROM sequences WHERE id = ?1")
            .bind(id.0.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.map(|r| row_to_sequence(&r)))
    }

    async fn get_sequence_by_name(
        &self, tenant_id: &TenantId, namespace: &Namespace, name: &str, version: Option<i32>,
    ) -> Result<Option<orch8_types::sequence::SequenceDefinition>, StorageError> {
        let row = if let Some(v) = version {
            sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 AND version=?4")
                .bind(&tenant_id.0).bind(&namespace.0).bind(name).bind(v)
                .fetch_optional(&self.pool).await
        } else {
            sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 AND deprecated=0 ORDER BY version DESC LIMIT 1")
                .bind(&tenant_id.0).bind(&namespace.0).bind(name)
                .fetch_optional(&self.pool).await
        }.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.map(|r| row_to_sequence(&r)))
    }

    async fn list_sequence_versions(&self, tenant_id: &TenantId, namespace: &Namespace, name: &str) -> Result<Vec<orch8_types::sequence::SequenceDefinition>, StorageError> {
        let rows = sqlx::query("SELECT * FROM sequences WHERE tenant_id=?1 AND namespace=?2 AND name=?3 ORDER BY version DESC")
            .bind(&tenant_id.0).bind(&namespace.0).bind(name)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_sequence).collect())
    }

    async fn deprecate_sequence(&self, id: SequenceId) -> Result<(), StorageError> {
        sqlx::query("UPDATE sequences SET deprecated=1 WHERE id=?1")
            .bind(id.0.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    // === Task Instances ===

    async fn create_instance(&self, i: &TaskInstance) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO task_instances (id,sequence_id,tenant_id,namespace,state,next_fire_at,priority,timezone,metadata,context,concurrency_key,max_concurrency,idempotency_key,session_id,parent_instance_id,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17)"
        )
        .bind(i.id.0.to_string())
        .bind(i.sequence_id.0.to_string())
        .bind(&i.tenant_id.0)
        .bind(&i.namespace.0)
        .bind(i.state.to_string())
        .bind(i.next_fire_at.map(ts))
        .bind(i.priority as i16)
        .bind(&i.timezone)
        .bind(serde_json::to_string(&i.metadata).unwrap_or_default())
        .bind(serde_json::to_string(&i.context).unwrap_or_default())
        .bind(&i.concurrency_key)
        .bind(i.max_concurrency)
        .bind(&i.idempotency_key)
        .bind(i.session_id.map(|u| u.to_string()))
        .bind(i.parent_instance_id.map(|u| u.0.to_string()))
        .bind(ts(i.created_at))
        .bind(ts(i.updated_at))
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn create_instances_batch(&self, instances: &[TaskInstance]) -> Result<u64, StorageError> {
        for i in instances { self.create_instance(i).await?; }
        Ok(instances.len() as u64)
    }

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError> {
        let row = sqlx::query("SELECT * FROM task_instances WHERE id=?1")
            .bind(id.0.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_instance))
    }

    async fn claim_due_instances(&self, now: DateTime<Utc>, limit: u32, _max_per_tenant: u32) -> Result<Vec<TaskInstance>, StorageError> {
        let now_s = ts(now);
        let rows = sqlx::query(
            "SELECT * FROM task_instances WHERE state='scheduled' AND (next_fire_at IS NULL OR next_fire_at <= ?1) ORDER BY priority DESC, next_fire_at ASC LIMIT ?2"
        )
        .bind(&now_s).bind(limit as i64)
        .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;

        let instances: Vec<TaskInstance> = rows.iter().map(row_to_instance).collect();
        // Mark as running
        for inst in &instances {
            sqlx::query("UPDATE task_instances SET state='running', updated_at=?2 WHERE id=?1")
                .bind(inst.id.0.to_string()).bind(&now_s)
                .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        }
        Ok(instances)
    }

    async fn update_instance_state(&self, id: InstanceId, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError> {
        sqlx::query("UPDATE task_instances SET state=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
            .bind(id.0.to_string())
            .bind(new_state.to_string())
            .bind(next_fire_at.map(ts))
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn update_instance_context(&self, id: InstanceId, context: &ExecutionContext) -> Result<(), StorageError> {
        sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
            .bind(id.0.to_string())
            .bind(serde_json::to_string(context).unwrap_or_default())
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn update_instance_sequence(&self, id: InstanceId, new_sequence_id: SequenceId) -> Result<(), StorageError> {
        sqlx::query("UPDATE task_instances SET sequence_id=?2, updated_at=?3 WHERE id=?1")
            .bind(id.0.to_string())
            .bind(new_sequence_id.0.to_string())
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn merge_context_data(&self, id: InstanceId, key: &str, value: &serde_json::Value) -> Result<(), StorageError> {
        // Read-modify-write for SQLite (no JSONB).
        let row = sqlx::query("SELECT context FROM task_instances WHERE id=?1")
            .bind(id.0.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        if let Some(row) = row {
            let ctx_str: String = row.get("context");
            let mut ctx: ExecutionContext = serde_json::from_str(&ctx_str).unwrap_or_default();
            if let Some(obj) = ctx.data.as_object_mut() {
                obj.insert(key.to_string(), value.clone());
            }
            sqlx::query("UPDATE task_instances SET context=?2, updated_at=?3 WHERE id=?1")
                .bind(id.0.to_string())
                .bind(serde_json::to_string(&ctx).unwrap_or_default())
                .bind(ts(Utc::now()))
                .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        }
        Ok(())
    }

    async fn list_instances(&self, filter: &InstanceFilter, pagination: &Pagination) -> Result<Vec<TaskInstance>, StorageError> {
        let mut sql = String::from("SELECT * FROM task_instances WHERE 1=1");
        apply_filter_sql(&mut sql, filter);
        sql.push_str(&format!(" ORDER BY created_at DESC LIMIT {} OFFSET {}", pagination.limit, pagination.offset));
        let rows = sqlx::query(&sql)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_instance).collect())
    }

    async fn count_instances(&self, filter: &InstanceFilter) -> Result<u64, StorageError> {
        let mut sql = String::from("SELECT COUNT(*) as cnt FROM task_instances WHERE 1=1");
        apply_filter_sql(&mut sql, filter);
        let row = sqlx::query(&sql)
            .fetch_one(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.get::<i64, _>("cnt") as u64)
    }

    async fn bulk_update_state(&self, filter: &InstanceFilter, new_state: InstanceState) -> Result<u64, StorageError> {
        let mut sql = format!("UPDATE task_instances SET state='{}', updated_at='{}' WHERE 1=1", new_state, ts(Utc::now()));
        apply_filter_sql(&mut sql, filter);
        let result = sqlx::query(&sql)
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn bulk_reschedule(&self, filter: &InstanceFilter, offset_secs: i64) -> Result<u64, StorageError> {
        // SQLite: use datetime function to shift
        let mut sql = format!(
            "UPDATE task_instances SET next_fire_at=datetime(next_fire_at, '+{offset_secs} seconds'), updated_at='{}' WHERE state='scheduled'",
            ts(Utc::now())
        );
        apply_filter_sql(&mut sql, filter);
        let result = sqlx::query(&sql)
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    // === Execution Tree ===

    async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO execution_tree (id,instance_id,block_id,parent_id,block_type,branch_index,state,started_at,completed_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)"
        )
        .bind(node.id.0.to_string())
        .bind(node.instance_id.0.to_string())
        .bind(&node.block_id.0)
        .bind(node.parent_id.map(|p| p.0.to_string()))
        .bind(node.block_type.to_string())
        .bind(node.branch_index.map(|b| b as i32))
        .bind(node.state.to_string())
        .bind(node.started_at.map(ts))
        .bind(node.completed_at.map(ts))
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn create_execution_nodes_batch(&self, nodes: &[ExecutionNode]) -> Result<(), StorageError> {
        for node in nodes { self.create_execution_node(node).await?; }
        Ok(())
    }

    async fn get_execution_tree(&self, instance_id: InstanceId) -> Result<Vec<ExecutionNode>, StorageError> {
        let rows = sqlx::query("SELECT * FROM execution_tree WHERE instance_id=?1")
            .bind(instance_id.0.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_node).collect())
    }

    async fn update_node_state(&self, node_id: ExecutionNodeId, state: NodeState) -> Result<(), StorageError> {
        let now = ts(Utc::now());
        let (started, completed) = match state {
            NodeState::Running => (Some(now.clone()), None),
            NodeState::Completed | NodeState::Failed | NodeState::Cancelled | NodeState::Skipped => (None, Some(now.clone())),
            _ => (None, None),
        };
        if let Some(ref s) = started {
            sqlx::query("UPDATE execution_tree SET state=?2, started_at=?3 WHERE id=?1")
                .bind(node_id.0.to_string()).bind(state.to_string()).bind(s)
                .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        } else if let Some(ref c) = completed {
            sqlx::query("UPDATE execution_tree SET state=?2, completed_at=?3 WHERE id=?1")
                .bind(node_id.0.to_string()).bind(state.to_string()).bind(c)
                .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        } else {
            sqlx::query("UPDATE execution_tree SET state=?2 WHERE id=?1")
                .bind(node_id.0.to_string()).bind(state.to_string())
                .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        }
        Ok(())
    }

    async fn get_children(&self, parent_id: ExecutionNodeId) -> Result<Vec<ExecutionNode>, StorageError> {
        let rows = sqlx::query("SELECT * FROM execution_tree WHERE parent_id=?1")
            .bind(parent_id.0.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_node).collect())
    }

    // === Block Outputs ===

    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO block_outputs (id,instance_id,block_id,output,output_ref,output_size,attempt,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)"
        )
        .bind(output.id.to_string())
        .bind(output.instance_id.0.to_string())
        .bind(&output.block_id.0)
        .bind(serde_json::to_string(&output.output).unwrap_or_default())
        .bind(&output.output_ref)
        .bind(output.output_size as i64)
        .bind(output.attempt as i64)
        .bind(ts(output.created_at))
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_block_output(&self, instance_id: InstanceId, block_id: &BlockId) -> Result<Option<BlockOutput>, StorageError> {
        let row = sqlx::query("SELECT * FROM block_outputs WHERE instance_id=?1 AND block_id=?2 ORDER BY created_at DESC LIMIT 1")
            .bind(instance_id.0.to_string()).bind(&block_id.0)
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_output))
    }

    async fn get_all_outputs(&self, instance_id: InstanceId) -> Result<Vec<BlockOutput>, StorageError> {
        let rows = sqlx::query("SELECT * FROM block_outputs WHERE instance_id=?1 ORDER BY created_at")
            .bind(instance_id.0.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_output).collect())
    }

    async fn get_completed_block_ids(&self, instance_id: InstanceId) -> Result<Vec<BlockId>, StorageError> {
        let rows = sqlx::query("SELECT DISTINCT block_id FROM block_outputs WHERE instance_id=?1")
            .bind(instance_id.0.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(|r| BlockId(r.get::<String, _>("block_id"))).collect())
    }

    async fn get_completed_block_ids_batch(&self, instance_ids: &[InstanceId]) -> Result<HashMap<InstanceId, Vec<BlockId>>, StorageError> {
        let mut result = HashMap::new();
        for id in instance_ids {
            result.insert(*id, self.get_completed_block_ids(*id).await?);
        }
        Ok(result)
    }

    async fn save_output_and_transition(&self, output: &BlockOutput, instance_id: InstanceId, new_state: InstanceState, next_fire_at: Option<DateTime<Utc>>) -> Result<(), StorageError> {
        self.save_block_output(output).await?;
        self.update_instance_state(instance_id, new_state, next_fire_at).await?;
        Ok(())
    }

    // === Rate Limits ===

    async fn check_rate_limit(&self, tenant_id: &TenantId, resource_key: &ResourceKey, now: DateTime<Utc>) -> Result<RateLimitCheck, StorageError> {
        let row = sqlx::query("SELECT * FROM rate_limits WHERE tenant_id=?1 AND resource_key=?2")
            .bind(&tenant_id.0).bind(&resource_key.0)
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;

        if let Some(row) = row {
            let max_count: i64 = row.get("max_count");
            let window_seconds: i64 = row.get("window_seconds");
            let current: i64 = row.get("current_count");
            let window_start = parse_ts(row.get::<&str, _>("window_start"));

            let window_elapsed = (now - window_start).num_seconds();
            if window_elapsed >= window_seconds {
                // Reset window
                sqlx::query("UPDATE rate_limits SET current_count=1, window_start=?3 WHERE tenant_id=?1 AND resource_key=?2")
                    .bind(&tenant_id.0).bind(&resource_key.0).bind(ts(now))
                    .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
                return Ok(RateLimitCheck::Allowed);
            }

            if current < max_count {
                sqlx::query("UPDATE rate_limits SET current_count=current_count+1 WHERE tenant_id=?1 AND resource_key=?2")
                    .bind(&tenant_id.0).bind(&resource_key.0)
                    .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
                return Ok(RateLimitCheck::Allowed);
            }

            let retry_after = window_start + chrono::Duration::seconds(window_seconds);
            Ok(RateLimitCheck::Exceeded { retry_after })
        } else {
            Ok(RateLimitCheck::Allowed)
        }
    }

    async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO rate_limits (id,tenant_id,resource_key,max_count,window_seconds,current_count,window_start) VALUES (?1,?2,?3,?4,?5,?6,?7) ON CONFLICT(tenant_id,resource_key) DO UPDATE SET max_count=?4, window_seconds=?5"
        )
        .bind(limit.id.to_string())
        .bind(&limit.tenant_id.0)
        .bind(&limit.resource_key.0)
        .bind(i64::from(limit.max_count))
        .bind(i64::from(limit.window_seconds))
        .bind(i64::from(limit.current_count))
        .bind(ts(limit.window_start))
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    // === Signals ===

    async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO signal_inbox (id,instance_id,signal_type,payload,delivered,created_at) VALUES (?1,?2,?3,?4,0,?5)")
            .bind(signal.id.to_string())
            .bind(signal.instance_id.0.to_string())
            .bind(serde_json::to_string(&signal.signal_type).unwrap_or_default())
            .bind(serde_json::to_string(&signal.payload).unwrap_or_default())
            .bind(ts(signal.created_at))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_pending_signals(&self, instance_id: InstanceId) -> Result<Vec<Signal>, StorageError> {
        let rows = sqlx::query("SELECT * FROM signal_inbox WHERE instance_id=?1 AND delivered=0 ORDER BY created_at")
            .bind(instance_id.0.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_signal).collect())
    }

    async fn get_pending_signals_batch(&self, instance_ids: &[InstanceId]) -> Result<HashMap<InstanceId, Vec<Signal>>, StorageError> {
        let mut result = HashMap::new();
        for id in instance_ids {
            result.insert(*id, self.get_pending_signals(*id).await?);
        }
        Ok(result)
    }

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE signal_inbox SET delivered=1 WHERE id=?1")
            .bind(signal_id.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError> {
        for id in signal_ids { self.mark_signal_delivered(*id).await?; }
        Ok(())
    }

    // === Idempotency ===

    async fn find_by_idempotency_key(&self, tenant_id: &TenantId, idempotency_key: &str) -> Result<Option<TaskInstance>, StorageError> {
        let row = sqlx::query("SELECT * FROM task_instances WHERE tenant_id=?1 AND idempotency_key=?2 LIMIT 1")
            .bind(&tenant_id.0).bind(idempotency_key)
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_instance))
    }

    // === Concurrency ===

    async fn count_running_by_concurrency_key(&self, concurrency_key: &str) -> Result<i64, StorageError> {
        let row = sqlx::query("SELECT COUNT(*) as cnt FROM task_instances WHERE concurrency_key=?1 AND state='running'")
            .bind(concurrency_key)
            .fetch_one(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.get::<i64, _>("cnt"))
    }

    async fn concurrency_position(&self, instance_id: InstanceId, concurrency_key: &str) -> Result<i64, StorageError> {
        let rows = sqlx::query("SELECT id FROM task_instances WHERE concurrency_key=?1 AND state='running' ORDER BY id")
            .bind(concurrency_key)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        let id_str = instance_id.0.to_string();
        let pos = rows.iter().position(|r| r.get::<String, _>("id") == id_str).map_or(0, |p| p as i64 + 1);
        Ok(pos)
    }

    // === Recovery ===

    async fn recover_stale_instances(&self, _stale_threshold: Duration) -> Result<u64, StorageError> {
        let result = sqlx::query("UPDATE task_instances SET state='scheduled', updated_at=?1 WHERE state='running'")
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    // === Cron ===

    async fn create_cron_schedule(&self, s: &CronSchedule) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO cron_schedules (id,tenant_id,namespace,sequence_id,cron_expr,timezone,enabled,metadata,next_fire_at,last_triggered_at,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12)"
        )
        .bind(s.id.to_string())
        .bind(&s.tenant_id.0)
        .bind(&s.namespace.0)
        .bind(s.sequence_id.0.to_string())
        .bind(&s.cron_expr)
        .bind(&s.timezone)
        .bind(s.enabled as i32)
        .bind(serde_json::to_string(&s.metadata).unwrap_or_default())
        .bind(s.next_fire_at.map(ts))
        .bind(s.last_triggered_at.map(ts))
        .bind(ts(s.created_at))
        .bind(ts(s.updated_at))
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError> {
        let row = sqlx::query("SELECT * FROM cron_schedules WHERE id=?1")
            .bind(id.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_cron))
    }

    async fn list_cron_schedules(&self, tenant_id: Option<&TenantId>) -> Result<Vec<CronSchedule>, StorageError> {
        let rows = if let Some(tid) = tenant_id {
            sqlx::query("SELECT * FROM cron_schedules WHERE tenant_id=?1 ORDER BY created_at")
                .bind(&tid.0).fetch_all(&self.pool).await
        } else {
            sqlx::query("SELECT * FROM cron_schedules ORDER BY created_at")
                .fetch_all(&self.pool).await
        }.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_cron).collect())
    }

    async fn update_cron_schedule(&self, s: &CronSchedule) -> Result<(), StorageError> {
        sqlx::query("UPDATE cron_schedules SET cron_expr=?2, timezone=?3, enabled=?4, metadata=?5, next_fire_at=?6, updated_at=?7 WHERE id=?1")
            .bind(s.id.to_string())
            .bind(&s.cron_expr)
            .bind(&s.timezone)
            .bind(s.enabled as i32)
            .bind(serde_json::to_string(&s.metadata).unwrap_or_default())
            .bind(s.next_fire_at.map(ts))
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM cron_schedules WHERE id=?1")
            .bind(id.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn claim_due_cron_schedules(&self, now: DateTime<Utc>) -> Result<Vec<CronSchedule>, StorageError> {
        let rows = sqlx::query("SELECT * FROM cron_schedules WHERE enabled=1 AND next_fire_at <= ?1")
            .bind(ts(now))
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_cron).collect())
    }

    async fn update_cron_fire_times(&self, id: Uuid, last_triggered_at: DateTime<Utc>, next_fire_at: DateTime<Utc>) -> Result<(), StorageError> {
        sqlx::query("UPDATE cron_schedules SET last_triggered_at=?2, next_fire_at=?3, updated_at=?4 WHERE id=?1")
            .bind(id.to_string())
            .bind(ts(last_triggered_at))
            .bind(ts(next_fire_at))
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    // === Worker Tasks ===

    async fn create_worker_task(&self, t: &WorkerTask) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO worker_tasks (id,instance_id,block_id,handler_name,params,context,state,worker_id,queue_name,output,error_message,error_retryable,attempt,timeout_ms,claimed_at,heartbeat_at,completed_at,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18) ON CONFLICT(instance_id,block_id) DO NOTHING"
        )
        .bind(t.id.to_string())
        .bind(t.instance_id.0.to_string())
        .bind(&t.block_id.0)
        .bind(&t.handler_name)
        .bind(serde_json::to_string(&t.params).unwrap_or_default())
        .bind(serde_json::to_string(&t.context).unwrap_or_default())
        .bind(t.state.to_string())
        .bind(&t.worker_id)
        .bind(&t.queue_name)
        .bind(t.output.as_ref().map(|v| serde_json::to_string(v).unwrap_or_default()))
        .bind(&t.error_message)
        .bind(t.error_retryable.map(|b| b as i32))
        .bind(i64::from(t.attempt))
        .bind(t.timeout_ms)
        .bind(t.claimed_at.map(ts))
        .bind(t.heartbeat_at.map(ts))
        .bind(t.completed_at.map(ts))
        .bind(ts(t.created_at))
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<WorkerTask>, StorageError> {
        let row = sqlx::query("SELECT * FROM worker_tasks WHERE id=?1")
            .bind(task_id.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_worker_task))
    }

    async fn claim_worker_tasks(&self, handler_name: &str, worker_id: &str, limit: u32) -> Result<Vec<WorkerTask>, StorageError> {
        let now = ts(Utc::now());
        let rows = sqlx::query("SELECT * FROM worker_tasks WHERE handler_name=?1 AND state='pending' LIMIT ?2")
            .bind(handler_name).bind(limit as i64)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        let tasks: Vec<WorkerTask> = rows.iter().map(row_to_worker_task).collect();
        for t in &tasks {
            sqlx::query("UPDATE worker_tasks SET state='claimed', worker_id=?2, claimed_at=?3, heartbeat_at=?3 WHERE id=?1")
                .bind(t.id.to_string()).bind(worker_id).bind(&now)
                .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        }
        Ok(tasks)
    }

    async fn complete_worker_task(&self, task_id: Uuid, worker_id: &str, output: &serde_json::Value) -> Result<bool, StorageError> {
        let result = sqlx::query("UPDATE worker_tasks SET state='completed', output=?3, completed_at=?4 WHERE id=?1 AND worker_id=?2")
            .bind(task_id.to_string()).bind(worker_id)
            .bind(serde_json::to_string(output).unwrap_or_default())
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn fail_worker_task(&self, task_id: Uuid, worker_id: &str, message: &str, _retryable: bool) -> Result<bool, StorageError> {
        let result = sqlx::query("UPDATE worker_tasks SET state='failed', error_message=?3, completed_at=?4 WHERE id=?1 AND worker_id=?2")
            .bind(task_id.to_string()).bind(worker_id).bind(message).bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn heartbeat_worker_task(&self, task_id: Uuid, worker_id: &str) -> Result<bool, StorageError> {
        let result = sqlx::query("UPDATE worker_tasks SET heartbeat_at=?3 WHERE id=?1 AND worker_id=?2")
            .bind(task_id.to_string()).bind(worker_id).bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM worker_tasks WHERE id=?1")
            .bind(task_id.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn reap_stale_worker_tasks(&self, _stale_threshold: Duration) -> Result<u64, StorageError> {
        // Simplified: just reset all claimed tasks back to pending
        let result = sqlx::query("UPDATE worker_tasks SET state='pending', worker_id=NULL WHERE state='claimed'")
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn cancel_worker_tasks_for_block(&self, instance_id: Uuid, block_id: &str) -> Result<u64, StorageError> {
        let result = sqlx::query("DELETE FROM worker_tasks WHERE instance_id=?1 AND block_id=?2 AND state IN ('pending','claimed')")
            .bind(instance_id.to_string()).bind(block_id)
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    // === Resource Pools ===

    async fn create_resource_pool(&self, pool: &ResourcePool) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO resource_pools (id,tenant_id,name,strategy,round_robin_index,created_at,updated_at) VALUES (?1,?2,?3,?4,?5,?6,?7)")
            .bind(pool.id.to_string())
            .bind(&pool.tenant_id.0)
            .bind(&pool.name)
            .bind(serde_json::to_string(&pool.strategy).unwrap_or_default().trim_matches('"'))
            .bind(pool.round_robin_index as i64)
            .bind(ts(pool.created_at))
            .bind(ts(pool.updated_at))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_resource_pool(&self, id: Uuid) -> Result<Option<ResourcePool>, StorageError> {
        let row = sqlx::query("SELECT * FROM resource_pools WHERE id=?1")
            .bind(id.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_pool))
    }

    async fn list_resource_pools(&self, tenant_id: &TenantId) -> Result<Vec<ResourcePool>, StorageError> {
        let rows = sqlx::query("SELECT * FROM resource_pools WHERE tenant_id=?1")
            .bind(&tenant_id.0)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_pool).collect())
    }

    async fn update_pool_round_robin_index(&self, pool_id: Uuid, index: u32) -> Result<(), StorageError> {
        sqlx::query("UPDATE resource_pools SET round_robin_index=?2, updated_at=?3 WHERE id=?1")
            .bind(pool_id.to_string()).bind(index as i64).bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn delete_resource_pool(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM resource_pools WHERE id=?1")
            .bind(id.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn add_pool_resource(&self, r: &PoolResource) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO pool_resources (id,pool_id,resource_key,name,weight,enabled,daily_cap,daily_usage,daily_usage_date,warmup_start,warmup_days,warmup_start_cap,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13)")
            .bind(r.id.to_string())
            .bind(r.pool_id.to_string())
            .bind(&r.resource_key.0)
            .bind(&r.name)
            .bind(r.weight as i64)
            .bind(r.enabled as i32)
            .bind(r.daily_cap as i64)
            .bind(r.daily_usage as i64)
            .bind(r.daily_usage_date.map(|d| d.to_string()))
            .bind(r.warmup_start.map(|d| d.to_string()))
            .bind(r.warmup_days as i64)
            .bind(r.warmup_start_cap as i64)
            .bind(ts(r.created_at))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_pool_resources(&self, pool_id: Uuid) -> Result<Vec<PoolResource>, StorageError> {
        let rows = sqlx::query("SELECT * FROM pool_resources WHERE pool_id=?1")
            .bind(pool_id.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_pool_resource).collect())
    }

    async fn update_pool_resource(&self, r: &PoolResource) -> Result<(), StorageError> {
        sqlx::query("UPDATE pool_resources SET name=?2, weight=?3, enabled=?4, daily_cap=?5, warmup_start=?6, warmup_days=?7, warmup_start_cap=?8 WHERE id=?1")
            .bind(r.id.to_string())
            .bind(&r.name)
            .bind(r.weight as i64)
            .bind(r.enabled as i32)
            .bind(r.daily_cap as i64)
            .bind(r.warmup_start.map(|d| d.to_string()))
            .bind(r.warmup_days as i64)
            .bind(r.warmup_start_cap as i64)
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn delete_pool_resource(&self, id: Uuid) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM pool_resources WHERE id=?1")
            .bind(id.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn increment_resource_usage(&self, resource_id: Uuid, today: chrono::NaiveDate) -> Result<(), StorageError> {
        let today_str = today.to_string();
        sqlx::query("UPDATE pool_resources SET daily_usage = CASE WHEN daily_usage_date = ?2 THEN daily_usage + 1 ELSE 1 END, daily_usage_date = ?2 WHERE id = ?1")
            .bind(resource_id.to_string()).bind(&today_str)
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    // === Checkpoints ===

    async fn save_checkpoint(&self, cp: &Checkpoint) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO checkpoints (id,instance_id,checkpoint_data,created_at) VALUES (?1,?2,?3,?4)")
            .bind(cp.id.to_string())
            .bind(cp.instance_id.0.to_string())
            .bind(serde_json::to_string(&cp.checkpoint_data).unwrap_or_default())
            .bind(ts(cp.created_at))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_latest_checkpoint(&self, instance_id: InstanceId) -> Result<Option<Checkpoint>, StorageError> {
        let row = sqlx::query("SELECT * FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC LIMIT 1")
            .bind(instance_id.0.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_checkpoint))
    }

    async fn list_checkpoints(&self, instance_id: InstanceId) -> Result<Vec<Checkpoint>, StorageError> {
        let rows = sqlx::query("SELECT * FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC")
            .bind(instance_id.0.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_checkpoint).collect())
    }

    async fn prune_checkpoints(&self, instance_id: InstanceId, keep: u32) -> Result<u64, StorageError> {
        // Keep the latest N, delete the rest.
        let result = sqlx::query(
            "DELETE FROM checkpoints WHERE instance_id=?1 AND id NOT IN (SELECT id FROM checkpoints WHERE instance_id=?1 ORDER BY created_at DESC LIMIT ?2)"
        )
        .bind(instance_id.0.to_string()).bind(keep as i64)
        .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    // === Externalized State ===

    async fn save_externalized_state(&self, instance_id: InstanceId, ref_key: &str, payload: &serde_json::Value) -> Result<(), StorageError> {
        sqlx::query("INSERT OR REPLACE INTO externalized_state (ref_key,instance_id,payload,created_at) VALUES (?1,?2,?3,?4)")
            .bind(ref_key)
            .bind(instance_id.0.to_string())
            .bind(serde_json::to_string(payload).unwrap_or_default())
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_externalized_state(&self, ref_key: &str) -> Result<Option<serde_json::Value>, StorageError> {
        let row = sqlx::query("SELECT payload FROM externalized_state WHERE ref_key=?1")
            .bind(ref_key)
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.map(|r| serde_json::from_str(r.get::<&str, _>("payload")).unwrap_or_default()))
    }

    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM externalized_state WHERE ref_key=?1")
            .bind(ref_key)
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    // === Audit Log ===

    async fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO audit_log (id,instance_id,tenant_id,event_type,from_state,to_state,block_id,details,created_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9)")
            .bind(entry.id.to_string())
            .bind(entry.instance_id.0.to_string())
            .bind(&entry.tenant_id.0)
            .bind(&entry.event_type)
            .bind(&entry.from_state)
            .bind(&entry.to_state)
            .bind(&entry.block_id)
            .bind(serde_json::to_string(&entry.details).unwrap_or_default())
            .bind(ts(entry.created_at))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_audit_log(&self, instance_id: InstanceId, limit: u32) -> Result<Vec<AuditLogEntry>, StorageError> {
        let rows = sqlx::query("SELECT * FROM audit_log WHERE instance_id=?1 ORDER BY created_at DESC LIMIT ?2")
            .bind(instance_id.0.to_string()).bind(limit as i64)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_audit).collect())
    }

    async fn list_audit_log_by_tenant(&self, tenant_id: &TenantId, limit: u32) -> Result<Vec<AuditLogEntry>, StorageError> {
        let rows = sqlx::query("SELECT * FROM audit_log WHERE tenant_id=?1 ORDER BY created_at DESC LIMIT ?2")
            .bind(&tenant_id.0).bind(limit as i64)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_audit).collect())
    }

    // === Sessions ===

    async fn create_session(&self, s: &Session) -> Result<(), StorageError> {
        sqlx::query("INSERT INTO sessions (id,tenant_id,session_key,data,state,created_at,updated_at,expires_at) VALUES (?1,?2,?3,?4,?5,?6,?7,?8)")
            .bind(s.id.to_string())
            .bind(&s.tenant_id.0)
            .bind(&s.session_key)
            .bind(serde_json::to_string(&s.data).unwrap_or_default())
            .bind(s.state.to_string())
            .bind(ts(s.created_at))
            .bind(ts(s.updated_at))
            .bind(s.expires_at.map(ts))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_session(&self, id: Uuid) -> Result<Option<Session>, StorageError> {
        let row = sqlx::query("SELECT * FROM sessions WHERE id=?1")
            .bind(id.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_session))
    }

    async fn get_session_by_key(&self, tenant_id: &TenantId, session_key: &str) -> Result<Option<Session>, StorageError> {
        let row = sqlx::query("SELECT * FROM sessions WHERE tenant_id=?1 AND session_key=?2")
            .bind(&tenant_id.0).bind(session_key)
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.as_ref().map(row_to_session))
    }

    async fn update_session_data(&self, id: Uuid, data: &serde_json::Value) -> Result<(), StorageError> {
        sqlx::query("UPDATE sessions SET data=?2, updated_at=?3 WHERE id=?1")
            .bind(id.to_string())
            .bind(serde_json::to_string(data).unwrap_or_default())
            .bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn update_session_state(&self, id: Uuid, state: SessionState) -> Result<(), StorageError> {
        sqlx::query("UPDATE sessions SET state=?2, updated_at=?3 WHERE id=?1")
            .bind(id.to_string()).bind(state.to_string()).bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_session_instances(&self, session_id: Uuid) -> Result<Vec<TaskInstance>, StorageError> {
        let rows = sqlx::query("SELECT * FROM task_instances WHERE session_id=?1")
            .bind(session_id.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_instance).collect())
    }

    // === Sub-Sequences ===

    async fn get_child_instances(&self, parent_instance_id: InstanceId) -> Result<Vec<TaskInstance>, StorageError> {
        let rows = sqlx::query("SELECT * FROM task_instances WHERE parent_instance_id=?1")
            .bind(parent_instance_id.0.to_string())
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_instance).collect())
    }

    // === Task Queue Routing ===

    async fn claim_worker_tasks_from_queue(&self, queue_name: &str, handler_name: &str, worker_id: &str, limit: u32) -> Result<Vec<WorkerTask>, StorageError> {
        let now = ts(Utc::now());
        let rows = sqlx::query("SELECT * FROM worker_tasks WHERE queue_name=?1 AND handler_name=?2 AND state='pending' LIMIT ?3")
            .bind(queue_name).bind(handler_name).bind(limit as i64)
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        let tasks: Vec<WorkerTask> = rows.iter().map(row_to_worker_task).collect();
        for t in &tasks {
            sqlx::query("UPDATE worker_tasks SET state='claimed', worker_id=?2, claimed_at=?3, heartbeat_at=?3 WHERE id=?1")
                .bind(t.id.to_string()).bind(worker_id).bind(&now)
                .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        }
        Ok(tasks)
    }

    // === Dynamic Step Injection ===

    async fn inject_blocks(&self, instance_id: InstanceId, blocks_json: &serde_json::Value) -> Result<(), StorageError> {
        sqlx::query("INSERT OR REPLACE INTO injected_blocks (instance_id, blocks) VALUES (?1, ?2)")
            .bind(instance_id.0.to_string())
            .bind(serde_json::to_string(blocks_json).unwrap_or_default())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_injected_blocks(&self, instance_id: InstanceId) -> Result<Option<serde_json::Value>, StorageError> {
        let row = sqlx::query("SELECT blocks FROM injected_blocks WHERE instance_id=?1")
            .bind(instance_id.0.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.map(|r| serde_json::from_str(r.get::<&str, _>("blocks")).unwrap_or_default()))
    }

    // === Cluster ===

    async fn register_node(&self, node: &ClusterNode) -> Result<(), StorageError> {
        sqlx::query("INSERT OR REPLACE INTO cluster_nodes (id,name,status,registered_at,last_heartbeat_at,drain) VALUES (?1,?2,?3,?4,?5,?6)")
            .bind(node.id.to_string())
            .bind(&node.name)
            .bind(node.status.to_string())
            .bind(ts(node.registered_at))
            .bind(ts(node.last_heartbeat_at))
            .bind(node.drain as i32)
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE cluster_nodes SET last_heartbeat_at=?2 WHERE id=?1")
            .bind(node_id.to_string()).bind(ts(Utc::now()))
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE cluster_nodes SET drain=1, status='draining' WHERE id=?1")
            .bind(node_id.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        sqlx::query("UPDATE cluster_nodes SET status='stopped' WHERE id=?1")
            .bind(node_id.to_string())
            .execute(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<ClusterNode>, StorageError> {
        let rows = sqlx::query("SELECT * FROM cluster_nodes ORDER BY registered_at")
            .fetch_all(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(rows.iter().map(row_to_cluster_node).collect())
    }

    async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError> {
        let row = sqlx::query("SELECT drain FROM cluster_nodes WHERE id=?1")
            .bind(node_id.to_string())
            .fetch_optional(&self.pool).await.map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.is_some_and(|r| r.get::<i32, _>("drain") != 0))
    }

    async fn reap_stale_nodes(&self, _stale_threshold: Duration) -> Result<u64, StorageError> {
        // Simplified for SQLite test mode
        Ok(0)
    }

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError> {
        sqlx::query("SELECT 1").execute(&self.pool).await.map_err(|e| StorageError::Connection(e.to_string()))?;
        Ok(())
    }
}

// ─── Row conversion helpers ────────────────────────────────────

fn row_to_sequence(row: &sqlx::sqlite::SqliteRow) -> orch8_types::sequence::SequenceDefinition {
    orch8_types::sequence::SequenceDefinition {
        id: SequenceId(Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default()),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        namespace: Namespace(row.get::<String, _>("namespace")),
        name: row.get::<String, _>("name"),
        version: row.get::<i32, _>("version"),
        deprecated: row.get::<i32, _>("deprecated") != 0,
        blocks: serde_json::from_str(row.get::<&str, _>("blocks")).unwrap_or_default(),
        interceptors: row.get::<Option<String>, _>("interceptors").and_then(|s| serde_json::from_str(&s).ok()),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

fn row_to_output(row: &sqlx::sqlite::SqliteRow) -> BlockOutput {
    BlockOutput {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default()),
        block_id: BlockId(row.get::<String, _>("block_id")),
        output: serde_json::from_str(row.get::<&str, _>("output")).unwrap_or_default(),
        output_ref: row.get::<Option<String>, _>("output_ref"),
        output_size: row.get::<i64, _>("output_size") as i32,
        attempt: row.get::<i64, _>("attempt") as i16,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

fn row_to_signal(row: &sqlx::sqlite::SqliteRow) -> Signal {
    Signal {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default()),
        signal_type: serde_json::from_str(row.get::<&str, _>("signal_type")).unwrap_or(orch8_types::signal::SignalType::Resume),
        payload: serde_json::from_str(row.get::<&str, _>("payload")).unwrap_or_default(),
        delivered: row.get::<i32, _>("delivered") != 0,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        delivered_at: None,
    }
}

fn row_to_cron(row: &sqlx::sqlite::SqliteRow) -> CronSchedule {
    CronSchedule {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        namespace: Namespace(row.get::<String, _>("namespace")),
        sequence_id: SequenceId(Uuid::parse_str(row.get::<&str, _>("sequence_id")).unwrap_or_default()),
        cron_expr: row.get::<String, _>("cron_expr"),
        timezone: row.get::<String, _>("timezone"),
        enabled: row.get::<i32, _>("enabled") != 0,
        metadata: serde_json::from_str(row.get::<&str, _>("metadata")).unwrap_or_default(),
        next_fire_at: parse_ts_opt(row.get::<Option<String>, _>("next_fire_at")),
        last_triggered_at: parse_ts_opt(row.get::<Option<String>, _>("last_triggered_at")),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
    }
}

fn row_to_worker_task(row: &sqlx::sqlite::SqliteRow) -> WorkerTask {
    use orch8_types::worker::WorkerTaskState;
    let state = match row.get::<&str, _>("state") {
        "claimed" => WorkerTaskState::Claimed,
        "completed" => WorkerTaskState::Completed,
        "failed" => WorkerTaskState::Failed,
        _ => WorkerTaskState::Pending,
    };
    WorkerTask {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default()),
        block_id: BlockId(row.get::<String, _>("block_id")),
        handler_name: row.get::<String, _>("handler_name"),
        queue_name: row.get::<Option<String>, _>("queue_name"),
        params: serde_json::from_str(row.get::<&str, _>("params")).unwrap_or_default(),
        context: serde_json::from_str(row.get::<&str, _>("context")).unwrap_or_default(),
        attempt: row.get::<i64, _>("attempt") as i16,
        timeout_ms: row.get::<Option<i64>, _>("timeout_ms"),
        state,
        worker_id: row.get::<Option<String>, _>("worker_id"),
        claimed_at: parse_ts_opt(row.get::<Option<String>, _>("claimed_at")),
        heartbeat_at: parse_ts_opt(row.get::<Option<String>, _>("heartbeat_at")),
        completed_at: parse_ts_opt(row.get::<Option<String>, _>("completed_at")),
        output: row.get::<Option<String>, _>("output").and_then(|s| serde_json::from_str(&s).ok()),
        error_message: row.get::<Option<String>, _>("error_message"),
        error_retryable: row.get::<Option<i32>, _>("error_retryable").map(|v| v != 0),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

fn row_to_pool(row: &sqlx::sqlite::SqliteRow) -> ResourcePool {
    use orch8_types::pool::RotationStrategy;
    let strategy_str = row.get::<String, _>("strategy");
    let strategy = serde_json::from_str::<RotationStrategy>(&format!("\"{strategy_str}\"")).unwrap_or(RotationStrategy::RoundRobin);
    ResourcePool {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        name: row.get::<String, _>("name"),
        strategy,
        round_robin_index: row.get::<i64, _>("round_robin_index") as u32,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
    }
}

fn row_to_pool_resource(row: &sqlx::sqlite::SqliteRow) -> PoolResource {
    PoolResource {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        pool_id: Uuid::parse_str(row.get::<&str, _>("pool_id")).unwrap_or_default(),
        resource_key: ResourceKey(row.get::<String, _>("resource_key")),
        name: row.get::<String, _>("name"),
        weight: row.get::<i64, _>("weight") as u32,
        enabled: row.get::<i32, _>("enabled") != 0,
        daily_cap: row.get::<i64, _>("daily_cap") as u32,
        daily_usage: row.get::<i64, _>("daily_usage") as u32,
        daily_usage_date: row.get::<Option<String>, _>("daily_usage_date").and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
        warmup_start: row.get::<Option<String>, _>("warmup_start").and_then(|s| chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d").ok()),
        warmup_days: row.get::<i64, _>("warmup_days") as u32,
        warmup_start_cap: row.get::<i64, _>("warmup_start_cap") as u32,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

fn row_to_checkpoint(row: &sqlx::sqlite::SqliteRow) -> Checkpoint {
    Checkpoint {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default()),
        checkpoint_data: serde_json::from_str(row.get::<&str, _>("checkpoint_data")).unwrap_or_default(),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

fn row_to_session(row: &sqlx::sqlite::SqliteRow) -> Session {
    let state = match row.get::<&str, _>("state") {
        "completed" => SessionState::Completed,
        "expired" => SessionState::Expired,
        _ => SessionState::Active,
    };
    Session {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        session_key: row.get::<String, _>("session_key"),
        data: serde_json::from_str(row.get::<&str, _>("data")).unwrap_or_default(),
        state,
        created_at: parse_ts(row.get::<&str, _>("created_at")),
        updated_at: parse_ts(row.get::<&str, _>("updated_at")),
        expires_at: parse_ts_opt(row.get::<Option<String>, _>("expires_at")),
    }
}

fn row_to_audit(row: &sqlx::sqlite::SqliteRow) -> AuditLogEntry {
    AuditLogEntry {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        instance_id: InstanceId(Uuid::parse_str(row.get::<&str, _>("instance_id")).unwrap_or_default()),
        tenant_id: TenantId(row.get::<String, _>("tenant_id")),
        event_type: row.get::<String, _>("event_type"),
        from_state: row.get::<Option<String>, _>("from_state"),
        to_state: row.get::<Option<String>, _>("to_state"),
        block_id: row.get::<Option<String>, _>("block_id"),
        details: serde_json::from_str(row.get::<&str, _>("details")).unwrap_or_default(),
        created_at: parse_ts(row.get::<&str, _>("created_at")),
    }
}

fn row_to_cluster_node(row: &sqlx::sqlite::SqliteRow) -> ClusterNode {
    let status = match row.get::<&str, _>("status") {
        "draining" => NodeStatus::Draining,
        "stopped" => NodeStatus::Stopped,
        _ => NodeStatus::Active,
    };
    ClusterNode {
        id: Uuid::parse_str(row.get::<&str, _>("id")).unwrap_or_default(),
        name: row.get::<String, _>("name"),
        status,
        registered_at: parse_ts(row.get::<&str, _>("registered_at")),
        last_heartbeat_at: parse_ts(row.get::<&str, _>("last_heartbeat_at")),
        drain: row.get::<i32, _>("drain") != 0,
    }
}

/// Build a simple SQL filter string (no parameterized queries — SQLite test mode only).
fn apply_filter_sql(sql: &mut String, filter: &InstanceFilter) {
    if let Some(ref tid) = filter.tenant_id {
        sql.push_str(&format!(" AND tenant_id='{}'", tid.0.replace('\'', "''")));
    }
    if let Some(ref ns) = filter.namespace {
        sql.push_str(&format!(" AND namespace='{}'", ns.0.replace('\'', "''")));
    }
    if let Some(ref sid) = filter.sequence_id {
        sql.push_str(&format!(" AND sequence_id='{}'", sid.0));
    }
    if let Some(ref states) = filter.states {
        if !states.is_empty() {
            let state_strs: Vec<String> = states.iter().map(|s| format!("'{s}'")).collect();
            sql.push_str(&format!(" AND state IN ({})", state_strs.join(",")));
        }
    }
    if let Some(ref p) = filter.priority {
        sql.push_str(&format!(" AND priority={}", *p as i16));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::sequence::{BlockDefinition, StepDef};

    #[tokio::test]
    async fn sqlite_roundtrip_sequence() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let seq = orch8_types::sequence::SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("default".into()),
            name: "test_seq".into(),
            version: 1,
            deprecated: false,
            blocks: vec![BlockDefinition::Step(StepDef {
                id: BlockId("s1".into()),
                handler: "noop".into(),
                params: serde_json::Value::Null,
                delay: None, retry: None, timeout: None,
                rate_limit_key: None, send_window: None, context_access: None,
                cancellable: true, wait_for_input: None, queue_name: None,
                deadline: None, on_deadline_breach: None,
            })],
            interceptors: None,
            created_at: now,
        };
        storage.create_sequence(&seq).await.unwrap();
        let fetched = storage.get_sequence(seq.id).await.unwrap().unwrap();
        assert_eq!(fetched.name, "test_seq");
        assert_eq!(fetched.blocks.len(), 1);
    }

    #[tokio::test]
    async fn sqlite_roundtrip_instance() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let inst = TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("default".into()),
            state: InstanceState::Scheduled,
            next_fire_at: Some(now),
            priority: Priority::High,
            timezone: "UTC".into(),
            metadata: serde_json::json!({"key": "val"}),
            context: ExecutionContext::default(),
            concurrency_key: None, max_concurrency: None,
            idempotency_key: None, session_id: None, parent_instance_id: None,
            created_at: now, updated_at: now,
        };
        storage.create_instance(&inst).await.unwrap();
        let fetched = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(fetched.tenant_id.0, "t1");
        assert_eq!(fetched.priority, Priority::High);
    }

    #[tokio::test]
    async fn sqlite_claim_due_instances() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let inst = TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId("t1".into()),
            namespace: Namespace("default".into()),
            state: InstanceState::Scheduled,
            next_fire_at: Some(now - chrono::Duration::seconds(10)),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None, max_concurrency: None,
            idempotency_key: None, session_id: None, parent_instance_id: None,
            created_at: now, updated_at: now,
        };
        storage.create_instance(&inst).await.unwrap();
        let claimed = storage.claim_due_instances(now, 10, 0).await.unwrap();
        assert_eq!(claimed.len(), 1);
        // Should not be claimed again.
        let claimed2 = storage.claim_due_instances(now, 10, 0).await.unwrap();
        assert_eq!(claimed2.len(), 0);
    }

    #[tokio::test]
    async fn sqlite_ping() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        storage.ping().await.unwrap();
    }
}
