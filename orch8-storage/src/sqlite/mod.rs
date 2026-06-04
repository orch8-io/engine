//! SQLite implementation of `StorageBackend`.
//!
//! Supports both in-memory (for testing) and file-backed (for standalone deploys).
//!
//! Usage:
//! ```rust,no_run
//! use orch8_storage::sqlite::SqliteStorage;
//!
//! # async fn example() {
//! // In-memory (testing):
//! let storage = SqliteStorage::in_memory().await.unwrap();
//!
//! // File-backed (standalone):
//! let storage = SqliteStorage::file("./orch8.db").await.unwrap();
//! # }
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

mod api_keys;
mod audit;
mod checkpoints;
mod circuit_breakers;
mod cluster;
mod credentials;
mod cron;
mod execution_tree;
mod externalized;
mod helpers;
mod instances;
mod kv_state;
mod misc;
mod outputs;
mod plugins;
mod pools;
mod rate_limits;
mod schema;
mod sequences;
mod sessions;
mod signals;
mod triggers;
mod workers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
use sqlx::SqlitePool;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::cron::CronSchedule;
use orch8_types::error::StorageError;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{
    BlockId, ExecutionNodeId, InstanceId, Namespace, ResourceKey, SequenceId, TenantId,
};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};
use orch8_types::sequence::SequenceDefinition;
use orch8_types::signal::Signal;
use orch8_types::worker::WorkerTask;

/// SQLite storage backend. Supports in-memory (testing) and file-backed (standalone).
pub struct SqliteStorage {
    pub(crate) pool: SqlitePool,
    /// Optional durable artifact backend (local FS / S3). `None` → artifact
    /// methods return `Unsupported`.
    artifact_store: Option<std::sync::Arc<crate::artifacts::ObjectArtifactStore>>,
}

impl SqliteStorage {
    /// Access the underlying `SqlitePool`.
    pub fn pool(&self) -> &SqlitePool {
        &self.pool
    }

    /// Attach a durable artifact backend. Builder-style; call before boxing as
    /// `Arc<dyn StorageBackend>`.
    #[must_use]
    pub fn with_artifact_store(
        mut self,
        store: std::sync::Arc<crate::artifacts::ObjectArtifactStore>,
    ) -> Self {
        self.artifact_store = Some(store);
        self
    }

    /// Create a new in-memory SQLite storage with all tables.
    pub async fn in_memory() -> Result<Self, StorageError> {
        let opts = SqliteConnectOptions::from_str("sqlite::memory:")
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .create_if_missing(true)
            // FK enforcement is off by default in SQLite. We need it on so
            // `ON DELETE CASCADE` on externalized_state.instance_id actually
            // fires when a task_instances row is deleted. Must match file mode.
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(1) // SQLite in-memory requires single connection
            .connect_with(opts)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let storage = Self {
            pool,
            artifact_store: None,
        };
        storage.create_tables().await?;
        Ok(storage)
    }

    /// Create a file-backed SQLite storage with WAL mode for concurrent reads.
    pub async fn file(path: &str) -> Result<Self, StorageError> {
        let opts = SqliteConnectOptions::from_str(&format!("sqlite:{path}"))
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5))
            // FK enforcement is off by default in SQLite. Mirror in_memory().
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(8)
            .connect_with(opts)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let storage = Self {
            pool,
            artifact_store: None,
        };
        storage.create_tables().await?;
        Ok(storage)
    }

    /// Create a file-backed SQLite storage tuned for mobile use.
    ///
    /// Uses WAL journal mode for concurrent reads during writes, 5 connections
    /// (1 writer + 4 reader capacity under WAL), a 5-second busy timeout to
    /// handle contention gracefully, and foreign keys for cascade deletes.
    pub async fn file_mobile(path: &str) -> Result<Self, StorageError> {
        let opts = SqliteConnectOptions::from_str(&format!("sqlite:{path}"))
            .map_err(|e| StorageError::Connection(e.to_string()))?
            .create_if_missing(true)
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5))
            .foreign_keys(true);

        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(opts)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        let storage = Self {
            pool,
            artifact_store: None,
        };
        storage.create_tables().await?;
        Ok(storage)
    }

    async fn create_tables(&self) -> Result<(), StorageError> {
        sqlx::query(schema::SCHEMA).execute(&self.pool).await?;
        self.record_schema_version().await?;
        Ok(())
    }

    /// Record the bundled schema version in `schema_versions` and warn if the
    /// DB has a newer version applied (suggesting a downgrade). Ref#15.
    async fn record_schema_version(&self) -> Result<(), StorageError> {
        let current = schema::SCHEMA_VERSION;

        let max_version: Option<i64> =
            sqlx::query_scalar("SELECT MAX(version) FROM schema_versions")
                .fetch_optional(&self.pool)
                .await?
                .flatten();

        if let Some(existing) = max_version {
            if existing > current {
                tracing::warn!(
                    db_version = existing,
                    binary_version = current,
                    "sqlite schema: database was previously migrated past this binary's \
                     bundled schema version — this binary may be an older build"
                );
            }
        }

        // `INSERT OR IGNORE` so repeated boots at the same version don't
        // produce duplicate rows.
        sqlx::query("INSERT OR IGNORE INTO schema_versions (version) VALUES (?)")
            .bind(current)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

// ============================================================================
// Sub-trait 1: SequenceStore
// ============================================================================

#[async_trait]
impl crate::SequenceStore for SqliteStorage {
    // === Sequences ===

    async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError> {
        sequences::create(self, seq).await
    }

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        sequences::get(self, id).await
    }

    async fn get_sequence_by_name(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        sequences::get_by_name(self, tenant_id, namespace, name, version).await
    }

    async fn list_sequence_versions(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
    ) -> Result<Vec<SequenceDefinition>, StorageError> {
        sequences::list_versions(self, tenant_id, namespace, name).await
    }

    async fn list_sequences(
        &self,
        tenant_id: Option<&TenantId>,
        namespace: Option<&Namespace>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<SequenceDefinition>, StorageError> {
        sequences::list_all(self, tenant_id, namespace, limit, offset).await
    }

    async fn deprecate_sequence(&self, id: SequenceId) -> Result<(), StorageError> {
        sequences::deprecate(self, id).await
    }

    async fn update_sequence_status(
        &self,
        id: SequenceId,
        status: &str,
    ) -> Result<(), StorageError> {
        sequences::update_status(self, id, status).await
    }

    async fn delete_sequence(&self, id: SequenceId) -> Result<(), StorageError> {
        sequences::delete(self, id).await
    }
}

// ============================================================================
// Sub-trait 2: InstanceStore
// ============================================================================

#[async_trait]
impl crate::InstanceStore for SqliteStorage {
    // === Task Instances ===

    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError> {
        instances::create(self, instance).await
    }

    async fn create_instances_batch(
        &self,
        instances: &[TaskInstance],
    ) -> Result<u64, StorageError> {
        instances::create_batch(self, instances).await
    }

    async fn create_instance_externalized(
        &self,
        instance: &TaskInstance,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        instances::create_externalized(self, instance, threshold_bytes).await
    }

    async fn create_instances_batch_externalized(
        &self,
        instances: &[TaskInstance],
        threshold_bytes: u32,
    ) -> Result<u64, StorageError> {
        instances::create_batch_externalized(self, instances, threshold_bytes).await
    }

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError> {
        instances::get(self, id).await
    }

    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        max_per_tenant: u32,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        instances::claim_due(self, now, limit, max_per_tenant).await
    }

    async fn update_instance_state(
        &self,
        id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        instances::update_state(self, id, new_state, next_fire_at).await
    }

    async fn batch_reschedule_instances(
        &self,
        ids: &[InstanceId],
        fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        instances::batch_reschedule(self, ids, fire_at).await
    }

    async fn conditional_update_instance_state(
        &self,
        id: InstanceId,
        expected_state: InstanceState,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<bool, StorageError> {
        instances::conditional_update_state(self, id, expected_state, new_state, next_fire_at).await
    }

    async fn update_instance_context_externalized(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        instances::update_context_externalized(self, id, context, threshold_bytes).await
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError> {
        instances::update_context(self, id, context).await
    }

    async fn update_instance_context_cas(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        instances::update_context_cas(self, id, context, expected_updated_at).await
    }

    async fn update_instance_started_at(
        &self,
        id: InstanceId,
        started_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        instances::update_started_at(self, id, started_at).await
    }

    async fn increment_total_steps(&self, id: InstanceId) -> Result<u32, StorageError> {
        instances::increment_total_steps(self, id).await
    }

    async fn update_instance_current_step_started_at(
        &self,
        id: InstanceId,
        ts: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        instances::update_current_step_started_at(self, id, ts).await
    }

    async fn update_instance_sequence(
        &self,
        id: InstanceId,
        new_sequence_id: SequenceId,
    ) -> Result<(), StorageError> {
        instances::update_sequence(self, id, new_sequence_id).await
    }

    async fn merge_context_data(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        instances::merge_context_data(self, id, key, value).await
    }

    async fn list_instances(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        instances::list(self, filter, pagination).await
    }

    async fn list_waiting_with_trees(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<(TaskInstance, Vec<ExecutionNode>)>, StorageError> {
        instances::list_waiting_with_trees(self, filter, pagination).await
    }

    async fn count_instances(&self, filter: &InstanceFilter) -> Result<u64, StorageError> {
        instances::count(self, filter).await
    }

    async fn bulk_update_state(
        &self,
        filter: &InstanceFilter,
        new_state: InstanceState,
    ) -> Result<u64, StorageError> {
        instances::bulk_update_state(self, filter, new_state).await
    }

    async fn bulk_reschedule(
        &self,
        filter: &InstanceFilter,
        offset_secs: i64,
    ) -> Result<u64, StorageError> {
        instances::bulk_reschedule(self, filter, offset_secs).await
    }

    // === Idempotency ===

    async fn find_by_idempotency_key(
        &self,
        tenant_id: &TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        misc::find_by_idempotency_key(self, tenant_id, idempotency_key).await
    }

    // === Concurrency ===

    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        misc::count_running_by_concurrency_key(self, concurrency_key).await
    }

    async fn count_running_by_concurrency_keys(
        &self,
        concurrency_keys: &[&str],
    ) -> Result<std::collections::HashMap<String, i64>, StorageError> {
        misc::count_running_by_concurrency_keys(self, concurrency_keys).await
    }

    async fn concurrency_position(
        &self,
        instance_id: InstanceId,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        misc::concurrency_position(self, instance_id, concurrency_key).await
    }

    // === Recovery ===

    async fn recover_stale_instances(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError> {
        misc::recover_stale_instances(self, stale_threshold).await
    }

    // === Sub-Sequences ===

    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        misc::get_child_instances(self, parent_instance_id).await
    }

    // === Dynamic Step Injection ===

    async fn inject_blocks(
        &self,
        instance_id: InstanceId,
        blocks_json: &serde_json::Value,
    ) -> Result<(), StorageError> {
        misc::inject_blocks(self, instance_id, blocks_json).await
    }

    async fn inject_blocks_at_position(
        &self,
        instance_id: InstanceId,
        new_blocks_json: &serde_json::Value,
        position: Option<usize>,
    ) -> Result<serde_json::Value, StorageError> {
        misc::inject_blocks_at_position(self, instance_id, new_blocks_json, position).await
    }

    async fn get_injected_blocks(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        misc::get_injected_blocks(self, instance_id).await
    }

    // === Emit Event Dedupe ===

    async fn record_or_get_emit_dedupe(
        &self,
        scope: &crate::DedupeScope,
        key: &str,
        candidate_child: InstanceId,
    ) -> Result<crate::EmitDedupeOutcome, StorageError> {
        misc::record_or_get_emit_dedupe(self, scope, key, candidate_child).await
    }

    async fn create_instance_with_dedupe(
        &self,
        scope: &crate::DedupeScope,
        key: &str,
        instance: &TaskInstance,
    ) -> Result<crate::EmitDedupeOutcome, StorageError> {
        misc::create_instance_with_dedupe(self, scope, key, instance).await
    }

    async fn delete_expired_emit_event_dedupe(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<u64, StorageError> {
        misc::delete_expired_emit_event_dedupe(self, older_than, limit).await
    }

    // Bridge method for default externalization impls in InstanceStore trait.
    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        externalized::batch_save(self, instance_id, entries).await
    }
}

// ============================================================================
// Sub-trait 3: ExecutionTreeStore
// ============================================================================

#[async_trait]
impl crate::ExecutionTreeStore for SqliteStorage {
    async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError> {
        execution_tree::create_node(self, node).await
    }

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[ExecutionNode],
    ) -> Result<(), StorageError> {
        execution_tree::create_batch(self, nodes).await
    }

    async fn get_execution_tree(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<ExecutionNode>, StorageError> {
        execution_tree::get_tree(self, instance_id).await
    }

    async fn update_node_state(
        &self,
        node_id: ExecutionNodeId,
        state: NodeState,
    ) -> Result<(), StorageError> {
        execution_tree::update_node_state(self, node_id, state).await
    }

    async fn batch_activate_nodes(&self, node_ids: &[ExecutionNodeId]) -> Result<(), StorageError> {
        execution_tree::batch_activate_nodes(self, node_ids).await
    }

    async fn update_nodes_state(
        &self,
        node_ids: &[ExecutionNodeId],
        state: NodeState,
    ) -> Result<(), StorageError> {
        execution_tree::update_nodes_state(self, node_ids, state).await
    }

    async fn get_children(
        &self,
        parent_id: ExecutionNodeId,
    ) -> Result<Vec<ExecutionNode>, StorageError> {
        execution_tree::get_children(self, parent_id).await
    }

    async fn delete_execution_tree(&self, instance_id: InstanceId) -> Result<(), StorageError> {
        execution_tree::delete_tree(self, instance_id).await
    }
}

// ============================================================================
// Sub-trait 4: OutputStore
// ============================================================================

#[async_trait]
impl crate::OutputStore for SqliteStorage {
    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError> {
        outputs::save(self, output).await
    }

    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<Option<BlockOutput>, StorageError> {
        outputs::get(self, instance_id, block_id).await
    }

    async fn get_block_outputs_batch(
        &self,
        keys: &[(InstanceId, BlockId)],
    ) -> Result<std::collections::HashMap<(InstanceId, BlockId), BlockOutput>, StorageError> {
        outputs::get_batch(self, keys).await
    }

    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError> {
        outputs::get_all(self, instance_id).await
    }

    async fn get_outputs_after_created_at(
        &self,
        instance_id: InstanceId,
        after: Option<DateTime<Utc>>,
    ) -> Result<Vec<BlockOutput>, StorageError> {
        outputs::get_after_created_at(self, instance_id, after).await
    }

    async fn get_completed_block_ids(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockId>, StorageError> {
        outputs::get_completed_ids(self, instance_id).await
    }

    async fn get_completed_block_ids_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<BlockId>>, StorageError> {
        outputs::get_completed_ids_batch(self, instance_ids).await
    }

    async fn save_output_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        outputs::save_output_and_transition(self, output, instance_id, new_state, next_fire_at)
            .await
    }

    async fn save_output_merge_context_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        outputs::save_output_merge_context_and_transition(
            self,
            output,
            instance_id,
            context,
            new_state,
            next_fire_at,
        )
        .await
    }

    async fn save_output_complete_node_and_transition(
        &self,
        output: &BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
        instance_id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        outputs::save_output_complete_node_and_transition(
            self,
            output,
            node_id,
            instance_id,
            new_state,
            next_fire_at,
        )
        .await
    }

    async fn save_output_complete_node_merge_context_and_transition(
        &self,
        output: &BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
        instance_id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        outputs::save_output_complete_node_merge_context_and_transition(
            self,
            output,
            node_id,
            instance_id,
            context,
            new_state,
            next_fire_at,
        )
        .await
    }

    async fn delete_block_outputs(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<u64, StorageError> {
        outputs::delete_for_block(self, instance_id, block_id).await
    }

    async fn delete_block_outputs_batch(
        &self,
        instance_id: InstanceId,
        block_ids: &[BlockId],
    ) -> Result<u64, StorageError> {
        outputs::delete_for_blocks(self, instance_id, block_ids).await
    }

    async fn delete_all_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError> {
        outputs::delete_all_for_instance(self, instance_id).await
    }

    async fn delete_sentinel_block_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<u64, StorageError> {
        outputs::delete_sentinels_for_instance(self, instance_id).await
    }

    async fn delete_block_output_by_id(&self, id: Uuid) -> Result<(), StorageError> {
        outputs::delete_by_id(self, id).await
    }
}

// ============================================================================
// Sub-trait 5: SignalStore
// ============================================================================

#[async_trait]
impl crate::SignalStore for SqliteStorage {
    async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError> {
        signals::enqueue(self, signal).await
    }

    async fn enqueue_signal_if_active(&self, signal: &Signal) -> Result<(), StorageError> {
        signals::enqueue_if_active(self, signal).await
    }

    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<Signal>, StorageError> {
        signals::get_pending(self, instance_id).await
    }

    async fn get_pending_signals_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<Signal>>, StorageError> {
        signals::get_pending_batch(self, instance_ids).await
    }

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError> {
        signals::mark_delivered(self, signal_id).await
    }

    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError> {
        signals::mark_delivered_batch(self, signal_ids).await
    }

    async fn get_signalled_instance_ids(
        &self,
        limit: u32,
    ) -> Result<Vec<(InstanceId, InstanceState)>, StorageError> {
        signals::get_signalled_instance_ids(self, limit).await
    }
}

// ============================================================================
// Sub-trait 6: WorkerStore
// ============================================================================

#[async_trait]
impl crate::WorkerStore for SqliteStorage {
    async fn create_worker_task(&self, task: &WorkerTask) -> Result<(), StorageError> {
        workers::create(self, task).await
    }

    async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<WorkerTask>, StorageError> {
        workers::get(self, task_id).await
    }

    async fn claim_worker_tasks(
        &self,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::claim(self, handler_name, worker_id, limit).await
    }

    async fn claim_worker_tasks_for_tenant(
        &self,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &orch8_types::TenantId,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::claim_for_tenant(self, handler_name, worker_id, tenant_id, limit).await
    }

    async fn complete_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        output: &serde_json::Value,
    ) -> Result<bool, StorageError> {
        workers::complete(self, task_id, worker_id, output).await
    }

    async fn fail_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        message: &str,
        retryable: bool,
    ) -> Result<bool, StorageError> {
        workers::fail(self, task_id, worker_id, message, retryable).await
    }

    async fn heartbeat_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
    ) -> Result<bool, StorageError> {
        workers::heartbeat(self, task_id, worker_id).await
    }

    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError> {
        workers::delete(self, task_id).await
    }

    async fn retry_worker_task(
        &self,
        old_task_id: Uuid,
        new_task: &WorkerTask,
        node_id: Option<ExecutionNodeId>,
        instance_id: InstanceId,
        fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query("DELETE FROM worker_tasks WHERE id = ?1")
            .bind(old_task_id.to_string())
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            r"INSERT INTO worker_tasks
                (id, instance_id, block_id, handler_name, queue_name, params, context,
                 attempt, timeout_ms, state, created_at)
              VALUES (?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
        )
        .bind(new_task.id.to_string())
        .bind(new_task.instance_id.into_uuid().to_string())
        .bind(new_task.block_id.as_str())
        .bind(&new_task.handler_name)
        .bind(&new_task.queue_name)
        .bind(&new_task.params)
        .bind(&new_task.context)
        .bind(new_task.attempt as i64)
        .bind(new_task.timeout_ms)
        .bind(new_task.state.to_string())
        .bind(new_task.created_at.to_rfc3339())
        .execute(&mut *tx)
        .await?;

        if let Some(nid) = node_id {
            sqlx::query("UPDATE execution_tree SET state = 'pending' WHERE id = ?1")
                .bind(nid.to_string())
                .execute(&mut *tx)
                .await?;
        }

        sqlx::query("UPDATE task_instances SET state = 'scheduled', next_fire_at = ?2, updated_at = ?3 WHERE id = ?1")
            .bind(instance_id.into_uuid().to_string())
            .bind(fire_at.to_rfc3339())
            .bind(chrono::Utc::now().to_rfc3339())
            .execute(&mut *tx)
            .await?;

        tx.commit().await?;
        Ok(())
    }

    async fn reap_stale_worker_tasks(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError> {
        workers::reap_stale(self, stale_threshold).await
    }

    async fn expire_timed_out_worker_tasks(&self) -> Result<u64, StorageError> {
        workers::expire_timed_out(self).await
    }

    async fn cancel_worker_tasks_for_block(
        &self,
        instance_id: Uuid,
        block_id: &str,
    ) -> Result<u64, StorageError> {
        workers::cancel_for_block(self, instance_id, block_id).await
    }

    async fn cancel_worker_tasks_for_blocks(
        &self,
        instance_id: Uuid,
        block_ids: &[String],
    ) -> Result<u64, StorageError> {
        workers::cancel_for_blocks(self, instance_id, block_ids).await
    }

    async fn list_worker_tasks(
        &self,
        filter: &orch8_types::worker_filter::WorkerTaskFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::list(self, filter, pagination).await
    }

    async fn worker_task_stats(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
        workers::stats(self, tenant_id).await
    }

    // === Task Queue Routing ===

    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        misc::claim_worker_tasks_from_queue(self, queue_name, handler_name, worker_id, limit).await
    }

    async fn claim_worker_tasks_from_queue_for_tenant(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &orch8_types::TenantId,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        misc::claim_worker_tasks_from_queue_for_tenant(
            self,
            queue_name,
            handler_name,
            worker_id,
            tenant_id,
            limit,
        )
        .await
    }
}

// ============================================================================
// Sub-trait 7: SchedulingStore
// ============================================================================

#[async_trait]
impl crate::SchedulingStore for SqliteStorage {
    // === Cron Schedules ===

    async fn create_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError> {
        cron::create(self, schedule).await
    }

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError> {
        cron::get(self, id).await
    }

    async fn list_cron_schedules(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        cron::list(self, tenant_id, limit).await
    }

    async fn update_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError> {
        cron::update(self, schedule).await
    }

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError> {
        cron::delete(self, id).await
    }

    async fn claim_due_cron_schedules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        cron::claim_due(self, now).await
    }

    async fn update_cron_fire_times(
        &self,
        id: Uuid,
        last_triggered_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        cron::update_fire_times(self, id, last_triggered_at, next_fire_at).await
    }

    // === Rate Limits ===

    async fn check_rate_limit(
        &self,
        tenant_id: &TenantId,
        resource_key: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<RateLimitCheck, StorageError> {
        rate_limits::check_rate_limit(self, tenant_id, resource_key, now).await
    }

    async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError> {
        rate_limits::upsert_rate_limit(self, limit).await
    }
}

// ============================================================================
// Sub-trait 8: AdminStore
// ============================================================================

#[async_trait]
impl crate::AdminStore for SqliteStorage {
    // === Sessions ===

    async fn create_session(
        &self,
        session: &orch8_types::session::Session,
    ) -> Result<(), StorageError> {
        sessions::create(self, session).await
    }

    async fn get_session(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        sessions::get(self, id).await
    }

    async fn get_session_by_key(
        &self,
        tenant_id: &TenantId,
        session_key: &str,
    ) -> Result<Option<orch8_types::session::Session>, StorageError> {
        sessions::get_by_key(self, tenant_id, session_key).await
    }

    async fn update_session_data(
        &self,
        id: Uuid,
        data: &serde_json::Value,
    ) -> Result<(), StorageError> {
        sessions::update_data(self, id, data).await
    }

    async fn update_session_state(
        &self,
        id: Uuid,
        state: orch8_types::session::SessionState,
    ) -> Result<(), StorageError> {
        sessions::update_state(self, id, state).await
    }

    async fn list_session_instances(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        sessions::list_instances(self, session_id).await
    }

    // === Plugins ===

    async fn create_plugin(
        &self,
        plugin: &orch8_types::plugin::PluginDef,
    ) -> Result<(), StorageError> {
        plugins::create(self, plugin).await
    }

    async fn get_plugin(
        &self,
        name: &str,
    ) -> Result<Option<orch8_types::plugin::PluginDef>, StorageError> {
        plugins::get(self, name).await
    }

    async fn list_plugins(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<orch8_types::plugin::PluginDef>, StorageError> {
        plugins::list(self, tenant_id).await
    }

    async fn update_plugin(
        &self,
        plugin: &orch8_types::plugin::PluginDef,
    ) -> Result<(), StorageError> {
        plugins::update(self, plugin).await
    }

    async fn delete_plugin(&self, name: &str) -> Result<(), StorageError> {
        plugins::delete(self, name).await
    }

    // === Triggers ===

    async fn create_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        triggers::create(self, trigger).await
    }

    async fn get_trigger(
        &self,
        slug: &str,
    ) -> Result<Option<orch8_types::trigger::TriggerDef>, StorageError> {
        triggers::get(self, slug).await
    }

    async fn list_triggers(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::trigger::TriggerDef>, StorageError> {
        triggers::list(self, tenant_id, limit).await
    }

    async fn update_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        triggers::update(self, trigger).await
    }

    async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError> {
        triggers::delete(self, slug).await
    }

    // === Credentials ===

    async fn create_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        credentials::create(self, credential).await
    }

    async fn get_credential(
        &self,
        id: &str,
    ) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError> {
        credentials::get(self, id).await
    }

    async fn list_credentials(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        credentials::list(self, tenant_id, limit).await
    }

    async fn update_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        credentials::update(self, credential).await
    }

    async fn delete_credential(&self, id: &str) -> Result<(), StorageError> {
        credentials::delete(self, id).await
    }

    async fn list_credentials_due_for_refresh(
        &self,
        threshold: std::time::Duration,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        credentials::list_due_for_refresh(self, threshold).await
    }

    // === API keys ===

    async fn create_api_key(
        &self,
        key: &orch8_types::api_key::ApiKeyRecord,
    ) -> Result<(), StorageError> {
        api_keys::create(self, key).await
    }

    async fn lookup_api_key_by_hash(
        &self,
        key_hash: &str,
    ) -> Result<Option<orch8_types::api_key::ApiKeyRecord>, StorageError> {
        api_keys::lookup_by_hash(self, key_hash).await
    }

    async fn list_api_keys(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::api_key::ApiKeyRecord>, StorageError> {
        api_keys::list(self, tenant_id).await
    }

    async fn revoke_api_key(&self, id: &str) -> Result<bool, StorageError> {
        api_keys::revoke(self, id).await
    }

    async fn touch_api_key(
        &self,
        id: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError> {
        api_keys::touch(self, id, at).await
    }

    // === Cluster ===

    async fn register_node(
        &self,
        node: &orch8_types::cluster::ClusterNode,
    ) -> Result<(), StorageError> {
        cluster::register(self, node).await
    }

    async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        cluster::heartbeat(self, node_id).await
    }

    async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        cluster::drain(self, node_id).await
    }

    async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError> {
        cluster::deregister(self, node_id).await
    }

    async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError> {
        cluster::list(self).await
    }

    async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError> {
        cluster::should_drain(self, node_id).await
    }

    async fn reap_stale_nodes(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError> {
        cluster::reap_stale(self, stale_threshold).await
    }

    // === Circuit Breakers ===

    async fn upsert_circuit_breaker(
        &self,
        state: &orch8_types::circuit_breaker::CircuitBreakerState,
    ) -> Result<(), StorageError> {
        circuit_breakers::upsert(self, state).await
    }

    async fn list_open_circuit_breakers(
        &self,
    ) -> Result<Vec<orch8_types::circuit_breaker::CircuitBreakerState>, StorageError> {
        circuit_breakers::list_open(self).await
    }

    async fn delete_circuit_breaker(
        &self,
        tenant_id: &TenantId,
        handler: &str,
    ) -> Result<(), StorageError> {
        circuit_breakers::delete(self, tenant_id, handler).await
    }

    // === Audit Log ===

    async fn append_audit_log(
        &self,
        entry: &orch8_types::audit::AuditLogEntry,
    ) -> Result<(), StorageError> {
        audit::append(self, entry).await
    }

    async fn list_audit_log(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        audit::list_by_instance(self, instance_id, limit).await
    }

    async fn list_audit_log_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        audit::list_by_tenant(self, tenant_id, limit).await
    }

    // === Rollback policies ===

    async fn create_rollback_policy(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        error_rate_threshold: f64,
        time_window_secs: i32,
        cooldown_secs: Option<i32>,
        confirmation_window_secs: Option<i32>,
        webhook_url: Option<&str>,
    ) -> Result<(), StorageError> {
        let cooldown = cooldown_secs.unwrap_or(3600);
        let confirmation = confirmation_window_secs.unwrap_or(60);
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            "INSERT INTO rollback_policies (tenant_id, sequence_name, error_rate_threshold, time_window_secs, cooldown_secs, confirmation_window_secs, webhook_url)
             VALUES (?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(tenant_id, sequence_name) DO UPDATE SET
               error_rate_threshold = excluded.error_rate_threshold,
               time_window_secs = excluded.time_window_secs,
               cooldown_secs = excluded.cooldown_secs,
               confirmation_window_secs = excluded.confirmation_window_secs,
               webhook_url = excluded.webhook_url,
               enabled = 1,
               updated_at = datetime('now')"
        )
        .bind(tenant_id)
        .bind(sequence_name)
        .bind(error_rate_threshold)
        .bind(time_window_secs)
        .bind(cooldown)
        .bind(confirmation)
        .bind(webhook_url)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(())
    }

    async fn get_rollback_policy(
        &self,
        tenant_id: &str,
        sequence_name: &str,
    ) -> Result<Option<orch8_types::rollback::RollbackPolicy>, StorageError> {
        let row: Option<(i64, String, String, f64, i32, i32, i32, i32, Option<String>, String, String)> = sqlx::query_as(
            "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, cooldown_secs, confirmation_window_secs, webhook_url, created_at, updated_at
             FROM rollback_policies WHERE tenant_id = ? AND sequence_name = ?"
        )
        .bind(tenant_id)
        .bind(sequence_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(
            |(
                id,
                tenant_id,
                sequence_name,
                error_rate_threshold,
                time_window_secs,
                enabled,
                cooldown_secs,
                confirmation_window_secs,
                webhook_url,
                created_at,
                updated_at,
            )| {
                orch8_types::rollback::RollbackPolicy {
                    id,
                    tenant_id,
                    sequence_name,
                    error_rate_threshold,
                    time_window_secs,
                    enabled: enabled != 0,
                    cooldown_secs,
                    confirmation_window_secs,
                    webhook_url,
                    created_at: chrono::DateTime::parse_from_rfc3339(&created_at)
                        .map_or_else(|_| chrono::Utc::now(), |dt| dt.with_timezone(&chrono::Utc)),
                    updated_at: chrono::DateTime::parse_from_rfc3339(&updated_at)
                        .map_or_else(|_| chrono::Utc::now(), |dt| dt.with_timezone(&chrono::Utc)),
                }
            },
        ))
    }

    async fn list_rollback_policies(
        &self,
        tenant_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<orch8_types::rollback::RollbackPolicy>, StorageError> {
        let rows: Vec<(
            i64,
            String,
            String,
            f64,
            i32,
            i32,
            i32,
            i32,
            Option<String>,
            String,
            String,
        )> = if let Some(t) = tenant_id {
            sqlx::query_as(
                "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, cooldown_secs, confirmation_window_secs, webhook_url, created_at, updated_at
                 FROM rollback_policies WHERE tenant_id = ? LIMIT ?"
            )
            .bind(t)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                "SELECT id, tenant_id, sequence_name, error_rate_threshold, time_window_secs, enabled, cooldown_secs, confirmation_window_secs, webhook_url, created_at, updated_at
                 FROM rollback_policies LIMIT ?"
            )
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    tenant_id,
                    sequence_name,
                    error_rate_threshold,
                    time_window_secs,
                    enabled,
                    cooldown_secs,
                    confirmation_window_secs,
                    webhook_url,
                    created_at,
                    updated_at,
                )| {
                    orch8_types::rollback::RollbackPolicy {
                        id,
                        tenant_id,
                        sequence_name,
                        error_rate_threshold,
                        time_window_secs,
                        enabled: enabled != 0,
                        cooldown_secs,
                        confirmation_window_secs,
                        webhook_url,
                        created_at: chrono::DateTime::parse_from_rfc3339(&created_at).map_or_else(
                            |_| chrono::Utc::now(),
                            |dt| dt.with_timezone(&chrono::Utc),
                        ),
                        updated_at: chrono::DateTime::parse_from_rfc3339(&updated_at).map_or_else(
                            |_| chrono::Utc::now(),
                            |dt| dt.with_timezone(&chrono::Utc),
                        ),
                    }
                },
            )
            .collect())
    }

    async fn delete_rollback_policy(
        &self,
        tenant_id: &str,
        sequence_name: &str,
    ) -> Result<(), StorageError> {
        sqlx::query("DELETE FROM rollback_policies WHERE tenant_id = ? AND sequence_name = ?")
            .bind(tenant_id)
            .bind(sequence_name)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn record_rollback(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        error_rate: f64,
        threshold: f64,
        reason: &str,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO rollback_history (tenant_id, sequence_name, error_rate, threshold, reason)
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(tenant_id)
        .bind(sequence_name)
        .bind(error_rate)
        .bind(threshold)
        .bind(reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn query_error_rate(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        window_secs: i64,
    ) -> Result<Option<f64>, StorageError> {
        let start = chrono::Utc::now() - chrono::Duration::seconds(window_secs);
        let start_str = start.to_rfc3339();
        let row: Option<(i64, i64)> = sqlx::query_as(
            r"SELECT
               COUNT(*) FILTER (WHERE event_type = 'InstanceFailed') as failed,
               COUNT(*) as total
             FROM telemetry_events
             WHERE created_at >= ?1
               AND json_extract(payload, '$.sequence_name') = ?2
               AND json_extract(payload, '$.tenant_id') = ?3",
        )
        .bind(start_str)
        .bind(sequence_name)
        .bind(tenant_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.and_then(|(failed, total)| {
            if total > 0 {
                #[allow(clippy::cast_precision_loss)]
                Some(failed as f64 / total as f64)
            } else {
                None
            }
        }))
    }

    async fn list_rollback_history(
        &self,
        tenant_id: Option<&str>,
        sequence_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<orch8_types::rollback::RollbackHistory>, StorageError> {
        let mut query = String::from(
            "SELECT id, tenant_id, sequence_name, triggered_at, error_rate, threshold, previous_manifest_version, reason, alert_sent FROM rollback_history WHERE 1=1"
        );
        if tenant_id.is_some() {
            query.push_str(" AND tenant_id = ?");
        }
        if sequence_name.is_some() {
            query.push_str(" AND sequence_name = ?");
        }
        query.push_str(" ORDER BY triggered_at DESC LIMIT ?");

        let mut q = sqlx::query_as(&query);
        if let Some(t) = tenant_id {
            q = q.bind(t);
        }
        if let Some(s) = sequence_name {
            q = q.bind(s);
        }
        q = q.bind(limit);

        let rows: Vec<(
            i64,
            String,
            String,
            String,
            f64,
            f64,
            Option<String>,
            String,
            i32,
        )> = q.fetch_all(&self.pool).await?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    tenant_id,
                    sequence_name,
                    triggered_at,
                    error_rate,
                    threshold,
                    previous_manifest_version,
                    reason,
                    alert_sent,
                )| {
                    orch8_types::rollback::RollbackHistory {
                        id,
                        tenant_id,
                        sequence_name,
                        triggered_at: chrono::DateTime::parse_from_rfc3339(&triggered_at)
                            .map_or_else(
                                |_| chrono::Utc::now(),
                                |dt| dt.with_timezone(&chrono::Utc),
                            ),
                        error_rate,
                        threshold,
                        previous_manifest_version,
                        reason,
                        alert_sent: alert_sent != 0,
                    }
                },
            )
            .collect())
    }

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError> {
        misc::ping(self).await
    }
}

// ============================================================================
// Sub-trait 9: TelemetryStore
// ============================================================================

#[async_trait]
impl crate::TelemetryStore for SqliteStorage {
    async fn ingest_telemetry_event(
        &self,
        event_type: &str,
        payload: &str,
        _device_id: &str,
        _os_name: &str,
        _os_version: &str,
        _app_version: &str,
        _sdk_version: &str,
        tenant_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let enriched = match serde_json::from_str::<serde_json::Value>(payload) {
            Ok(mut v) => {
                if let Some(obj) = v.as_object_mut() {
                    obj.entry("tenant_id")
                        .or_insert_with(|| serde_json::Value::String(tenant_id.to_string()));
                }
                v.to_string()
            }
            Err(_) => payload.to_string(),
        };
        sqlx::query(
            "INSERT INTO telemetry_events (event_type, payload, created_at) VALUES (?1, ?2, ?3)",
        )
        .bind(event_type)
        .bind(&enriched)
        .bind(created_at.to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn record_usage_event(&self, event: &crate::UsageEvent) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO usage_events \
             (tenant_id, instance_id, block_id, kind, model, input_tokens, output_tokens, created_at) \
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        )
        .bind(&event.tenant_id)
        .bind(event.instance_id.map(|i| i.to_string()))
        .bind(&event.block_id)
        .bind(&event.kind)
        .bind(&event.model)
        .bind(event.input_tokens)
        .bind(event.output_tokens)
        .bind(event.created_at.to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn query_usage(
        &self,
        tenant_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<crate::UsageAggregate>, StorageError> {
        use sqlx::Row;
        let rows = sqlx::query(
            "SELECT kind, model, COUNT(*) AS events, \
                    COALESCE(SUM(input_tokens), 0) AS input_tokens, \
                    COALESCE(SUM(output_tokens), 0) AS output_tokens \
             FROM usage_events \
             WHERE tenant_id = ?1 AND created_at >= ?2 AND created_at < ?3 \
             GROUP BY kind, model ORDER BY kind, model",
        )
        .bind(tenant_id)
        .bind(start.to_rfc3339())
        .bind(end.to_rfc3339())
        .fetch_all(self.pool())
        .await?;
        Ok(rows
            .iter()
            .map(|r| crate::UsageAggregate {
                kind: r.get("kind"),
                model: r.get("model"),
                events: r.get("events"),
                input_tokens: r.get("input_tokens"),
                output_tokens: r.get("output_tokens"),
            })
            .collect())
    }

    async fn ingest_telemetry_events_batch(
        &self,
        events: &[crate::TelemetryEvent],
    ) -> Result<u64, StorageError> {
        if events.is_empty() {
            return Ok(0);
        }
        let mut total = 0u64;
        // SQLite has a variable limit of 999 by default; 3 params per row -> batch <= 333.
        for chunk in events.chunks(333) {
            let mut qb: sqlx::QueryBuilder<'_, sqlx::Sqlite> = sqlx::QueryBuilder::new(
                "INSERT INTO telemetry_events (event_type, payload, created_at) ",
            );
            qb.push_values(chunk, |mut b, event| {
                let enriched = match serde_json::from_str::<serde_json::Value>(&event.payload) {
                    Ok(mut v) => {
                        if let Some(obj) = v.as_object_mut() {
                            obj.entry("tenant_id").or_insert_with(|| {
                                serde_json::Value::String(event.tenant_id.clone())
                            });
                        }
                        v.to_string()
                    }
                    Err(_) => event.payload.clone(),
                };
                b.push_bind(&event.event_type);
                b.push_bind(enriched);
                b.push_bind(event.created_at.to_rfc3339());
            });
            let result = qb.build().execute(self.pool()).await?;
            total += result.rows_affected();
        }
        Ok(total)
    }

    async fn ingest_telemetry_error(
        &self,
        error_type: &str,
        message: &str,
        _stack_trace: Option<&str>,
        _device_id: &str,
        _os_name: &str,
        _os_version: &str,
        _app_version: &str,
        _sdk_version: &str,
        tenant_id: &str,
        instance_id: Option<&str>,
        sequence_name: Option<&str>,
    ) -> Result<(), StorageError> {
        let payload = serde_json::json!({
            "error_type": error_type,
            "message": message,
            "instance_id": instance_id,
            "sequence_name": sequence_name,
            "tenant_id": tenant_id,
        });
        sqlx::query(
            "INSERT INTO telemetry_events (event_type, payload, created_at) VALUES (?1, ?2, ?3)",
        )
        .bind("InstanceFailed")
        .bind(payload.to_string())
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(self.pool())
        .await?;
        Ok(())
    }

    async fn query_telemetry_dashboard(
        &self,
        _query_type: &str,
        _tenant_id: &str,
        _start: DateTime<Utc>,
        _end: DateTime<Utc>,
    ) -> Result<Vec<(String, i64)>, StorageError> {
        // Dashboard queries are server-side (Postgres) only.
        Ok(Vec::new())
    }

    async fn delete_old_telemetry_events(
        &self,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM telemetry_events WHERE id IN (
                SELECT id FROM telemetry_events WHERE created_at < ?1 LIMIT ?2
            )",
        )
        .bind(older_than.to_rfc3339())
        .bind(limit)
        .execute(self.pool())
        .await?;
        Ok(result.rows_affected())
    }
}

// ============================================================================
// Sub-trait 10: ResourceStore
// ============================================================================

#[async_trait]
impl crate::ResourceStore for SqliteStorage {
    // === Artifacts ===

    fn artifacts_enabled(&self) -> bool {
        self.artifact_store.is_some()
    }

    async fn put_artifact(
        &self,
        instance_id: InstanceId,
        content_type: &str,
        bytes: bytes::Bytes,
    ) -> Result<orch8_types::artifact::ArtifactRef, StorageError> {
        crate::artifacts::require_store(self.artifact_store.as_ref())?
            .put(&instance_id.to_string(), content_type, bytes)
            .await
    }

    async fn get_artifact(&self, key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        crate::artifacts::require_store(self.artifact_store.as_ref())?
            .get(key)
            .await
    }

    async fn delete_artifact(&self, key: &str) -> Result<(), StorageError> {
        crate::artifacts::require_store(self.artifact_store.as_ref())?
            .delete(key)
            .await
    }

    async fn list_artifacts(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::artifact::ArtifactMeta>, StorageError> {
        crate::artifacts::require_store(self.artifact_store.as_ref())?
            .list(&instance_id.to_string())
            .await
    }

    async fn list_artifact_gc_candidates(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<Vec<InstanceId>, StorageError> {
        instances::list_artifact_gc_candidates(self, cutoff, limit).await
    }

    async fn mark_artifacts_gced(&self, instance_id: InstanceId) -> Result<(), StorageError> {
        instances::mark_artifacts_gced(self, instance_id).await
    }

    // === Instance KV State ===

    async fn set_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.set_instance_kv_impl(instance_id, key, value).await
    }

    async fn get_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.get_instance_kv_impl(instance_id, key).await
    }

    async fn get_all_instance_kv(
        &self,
        instance_id: InstanceId,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        self.get_all_instance_kv_impl(instance_id).await
    }

    async fn delete_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<(), StorageError> {
        self.delete_instance_kv_impl(instance_id, key).await
    }

    // === Externalized State ===

    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError> {
        externalized::save(self, instance_id, ref_key, payload).await
    }

    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        externalized::batch_save(self, instance_id, entries).await
    }

    async fn get_externalized_state(
        &self,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        externalized::get(self, ref_key).await
    }

    async fn batch_get_externalized_state(
        &self,
        ref_keys: &[String],
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        externalized::batch_get(self, ref_keys).await
    }

    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError> {
        externalized::delete(self, ref_key).await
    }

    async fn delete_expired_externalized_state(&self, limit: u32) -> Result<u64, StorageError> {
        externalized::delete_expired(self, limit).await
    }

    // === Resource Pools ===

    async fn create_resource_pool(
        &self,
        pool: &orch8_types::pool::ResourcePool,
    ) -> Result<(), StorageError> {
        pools::create(self, pool).await
    }

    async fn get_resource_pool(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError> {
        pools::get(self, id).await
    }

    async fn list_resource_pools(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError> {
        pools::list(self, tenant_id).await
    }

    async fn update_pool_round_robin_index(
        &self,
        pool_id: uuid::Uuid,
        index: u32,
    ) -> Result<(), StorageError> {
        pools::update_round_robin(self, pool_id, index).await
    }

    async fn delete_resource_pool(&self, id: uuid::Uuid) -> Result<(), StorageError> {
        pools::delete(self, id).await
    }

    async fn add_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        pools::add_resource(self, resource).await
    }

    async fn list_pool_resources(
        &self,
        pool_id: uuid::Uuid,
    ) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError> {
        pools::list_resources(self, pool_id).await
    }

    async fn update_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError> {
        pools::update_resource(self, resource).await
    }

    async fn delete_pool_resource(&self, id: uuid::Uuid) -> Result<(), StorageError> {
        pools::delete_resource(self, id).await
    }

    async fn increment_resource_usage(
        &self,
        resource_id: uuid::Uuid,
        today: chrono::NaiveDate,
    ) -> Result<(), StorageError> {
        pools::increment_usage(self, resource_id, today).await
    }

    // === Checkpoints ===

    async fn save_checkpoint(
        &self,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<(), StorageError> {
        checkpoints::save(self, checkpoint).await
    }

    async fn get_latest_checkpoint(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError> {
        checkpoints::get_latest(self, instance_id).await
    }

    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError> {
        checkpoints::list(self, instance_id, limit).await
    }

    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError> {
        checkpoints::prune(self, instance_id, keep).await
    }
}

// ============================================================================
// Sub-trait 11: MobileSyncStore
// ============================================================================

#[async_trait]
impl crate::MobileSyncStore for SqliteStorage {
    async fn register_mobile_device(
        &self,
        device: &crate::MobileDevice,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_devices (device_id, tenant_id, push_token, platform, app_version, active, registered_at)
             VALUES (?, ?, ?, ?, ?, 1, datetime('now'))
             ON CONFLICT(device_id) DO UPDATE SET
               push_token = excluded.push_token,
               platform = excluded.platform,
               app_version = excluded.app_version,
               active = 1",
        )
        .bind(&device.device_id)
        .bind(&device.tenant_id)
        .bind(&device.push_token)
        .bind(&device.platform)
        .bind(&device.app_version)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn get_mobile_device(
        &self,
        device_id: &str,
    ) -> Result<Option<crate::MobileDevice>, StorageError> {
        let row: Option<(String, String, Option<String>, String, Option<String>, bool, Option<String>, String)> =
            sqlx::query_as(
                "SELECT device_id, tenant_id, push_token, platform, app_version, active, last_sync_at, registered_at
                 FROM mobile_devices WHERE device_id = ?",
            )
            .bind(device_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.map(
            |(
                device_id,
                tenant_id,
                push_token,
                platform,
                app_version,
                active,
                last_sync_at,
                registered_at,
            )| {
                crate::MobileDevice {
                    device_id,
                    tenant_id,
                    push_token,
                    platform,
                    app_version,
                    active,
                    last_sync_at,
                    registered_at,
                }
            },
        ))
    }

    async fn update_device_last_sync(&self, device_id: &str) -> Result<(), StorageError> {
        sqlx::query("UPDATE mobile_devices SET last_sync_at = datetime('now') WHERE device_id = ?")
            .bind(device_id)
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_mobile_devices(
        &self,
        tenant_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<crate::MobileDevice>, StorageError> {
        let mut sql = String::from(
            "SELECT device_id, tenant_id, push_token, platform, app_version, active, last_sync_at, registered_at
             FROM mobile_devices",
        );
        if tenant_id.is_some() {
            sql.push_str(" WHERE tenant_id = ?");
        }
        sql.push_str(" ORDER BY registered_at DESC LIMIT ?");

        let mut query = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                String,
                Option<String>,
                bool,
                Option<String>,
                String,
            ),
        >(&sql);
        if let Some(tid) = tenant_id {
            query = query.bind(tid);
        }
        query = query.bind(limit);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    device_id,
                    tenant_id,
                    push_token,
                    platform,
                    app_version,
                    active,
                    last_sync_at,
                    registered_at,
                )| {
                    crate::MobileDevice {
                        device_id,
                        tenant_id,
                        push_token,
                        platform,
                        app_version,
                        active,
                        last_sync_at,
                        registered_at,
                    }
                },
            )
            .collect())
    }

    async fn mark_stale_devices_inactive(
        &self,
        stale_threshold_secs: i64,
    ) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "UPDATE mobile_devices SET active = 0
             WHERE active = 1 AND last_sync_at < datetime('now', '-' || ? || ' seconds')",
        )
        .bind(stale_threshold_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn upsert_mobile_instance_status(
        &self,
        status: &crate::MobileInstanceStatus,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_instance_status (device_id, instance_id, sequence_name, state, current_step, handler, context_summary, steps, updated_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(device_id, instance_id) DO UPDATE SET
               sequence_name = excluded.sequence_name,
               state = excluded.state,
               current_step = excluded.current_step,
               handler = excluded.handler,
               context_summary = excluded.context_summary,
               steps = excluded.steps,
               updated_at = excluded.updated_at",
        )
        .bind(&status.device_id)
        .bind(&status.instance_id)
        .bind(&status.sequence_name)
        .bind(&status.state)
        .bind(&status.current_step)
        .bind(&status.handler)
        .bind(&status.context_summary)
        .bind(&status.steps)
        .bind(&status.updated_at)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn list_mobile_instance_status(
        &self,
        tenant_id: Option<&str>,
        device_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<crate::MobileInstanceStatus>, StorageError> {
        let mut sql = String::from(
            "SELECT s.device_id, s.instance_id, s.sequence_name, s.state, s.current_step, s.handler, s.context_summary, s.steps, s.updated_at
             FROM mobile_instance_status s",
        );
        let mut conditions = Vec::new();
        if tenant_id.is_some() {
            sql.push_str(" JOIN mobile_devices d ON d.device_id = s.device_id");
            conditions.push("d.tenant_id = ?");
        }
        if device_id.is_some() {
            conditions.push("s.device_id = ?");
        }
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push_str(" ORDER BY s.updated_at DESC LIMIT ?");

        let mut query = sqlx::query_as::<
            _,
            (
                String,
                String,
                Option<String>,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                String,
            ),
        >(&sql);
        if let Some(tid) = tenant_id {
            query = query.bind(tid);
        }
        if let Some(did) = device_id {
            query = query.bind(did);
        }
        query = query.bind(limit);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    device_id,
                    instance_id,
                    sequence_name,
                    state,
                    current_step,
                    handler,
                    context_summary,
                    steps,
                    updated_at,
                )| {
                    crate::MobileInstanceStatus {
                        device_id,
                        instance_id,
                        sequence_name,
                        state,
                        current_step,
                        handler,
                        context_summary,
                        steps,
                        updated_at,
                    }
                },
            )
            .collect())
    }

    async fn insert_mobile_approval(
        &self,
        approval: &crate::MobileApprovalRequest,
    ) -> Result<bool, StorageError> {
        let result = sqlx::query(
            "INSERT INTO mobile_approval_requests (id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', datetime('now'))
             ON CONFLICT(device_id, instance_id, block_id) DO NOTHING",
        )
        .bind(&approval.id)
        .bind(&approval.device_id)
        .bind(&approval.tenant_id)
        .bind(&approval.instance_id)
        .bind(&approval.block_id)
        .bind(&approval.sequence_name)
        .bind(&approval.prompt)
        .bind(&approval.choices)
        .bind(&approval.store_as)
        .bind(approval.timeout_secs)
        .bind(&approval.metadata)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected() > 0)
    }

    async fn get_mobile_approval(
        &self,
        id: &str,
    ) -> Result<Option<crate::MobileApprovalRequest>, StorageError> {
        let row: Option<(String, String, String, String, String, Option<String>, Option<String>, Option<String>, Option<String>, Option<i64>, Option<String>, String, Option<String>, String, Option<String>)> =
            sqlx::query_as(
                "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, resolution, created_at, resolved_at
                 FROM mobile_approval_requests WHERE id = ?",
            )
            .bind(id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(row.map(
            |(
                id,
                device_id,
                tenant_id,
                instance_id,
                block_id,
                sequence_name,
                prompt,
                choices,
                store_as,
                timeout_secs,
                metadata,
                state,
                resolution,
                created_at,
                resolved_at,
            )| {
                crate::MobileApprovalRequest {
                    id,
                    device_id,
                    tenant_id,
                    instance_id,
                    block_id,
                    sequence_name,
                    prompt,
                    choices,
                    store_as,
                    timeout_secs,
                    metadata,
                    state,
                    resolution,
                    created_at,
                    resolved_at,
                }
            },
        ))
    }

    async fn resolve_mobile_approval(
        &self,
        id: &str,
        resolution: &str,
    ) -> Result<Option<crate::MobileApprovalRequest>, StorageError> {
        let result = sqlx::query(
            "UPDATE mobile_approval_requests SET state = 'resolved', resolution = ?, resolved_at = datetime('now')
             WHERE id = ? AND state = 'pending'",
        )
        .bind(resolution)
        .bind(id)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

        if result.rows_affected() == 0 {
            return Ok(None);
        }
        self.get_mobile_approval(id).await
    }

    async fn list_mobile_approvals(
        &self,
        tenant_id: Option<&str>,
        state: Option<&str>,
        limit: u32,
    ) -> Result<Vec<crate::MobileApprovalRequest>, StorageError> {
        let mut sql = String::from(
            "SELECT id, device_id, tenant_id, instance_id, block_id, sequence_name, prompt, choices, store_as, timeout_secs, metadata, state, resolution, created_at, resolved_at
             FROM mobile_approval_requests WHERE 1=1",
        );
        if tenant_id.is_some() {
            sql.push_str(" AND tenant_id = ?");
        }
        if state.is_some() {
            sql.push_str(" AND state = ?");
        }
        sql.push_str(" ORDER BY created_at DESC LIMIT ?");

        let mut query = sqlx::query_as::<
            _,
            (
                String,
                String,
                String,
                String,
                String,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<String>,
                Option<i64>,
                Option<String>,
                String,
                Option<String>,
                String,
                Option<String>,
            ),
        >(&sql);
        if let Some(tid) = tenant_id {
            query = query.bind(tid);
        }
        if let Some(s) = state {
            query = query.bind(s);
        }
        query = query.bind(limit);

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(
                |(
                    id,
                    device_id,
                    tenant_id,
                    instance_id,
                    block_id,
                    sequence_name,
                    prompt,
                    choices,
                    store_as,
                    timeout_secs,
                    metadata,
                    state,
                    resolution,
                    created_at,
                    resolved_at,
                )| {
                    crate::MobileApprovalRequest {
                        id,
                        device_id,
                        tenant_id,
                        instance_id,
                        block_id,
                        sequence_name,
                        prompt,
                        choices,
                        store_as,
                        timeout_secs,
                        metadata,
                        state,
                        resolution,
                        created_at,
                        resolved_at,
                    }
                },
            )
            .collect())
    }

    async fn expire_mobile_approvals(&self) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "UPDATE mobile_approval_requests SET state = 'expired'
             WHERE state = 'pending' AND timeout_secs IS NOT NULL
               AND datetime(created_at, '+' || timeout_secs || ' seconds') < datetime('now')",
        )
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn create_mobile_command(
        &self,
        command: &crate::MobileCommand,
    ) -> Result<(), StorageError> {
        sqlx::query(
            "INSERT INTO mobile_commands (id, device_id, command_type, payload, created_at)
             VALUES (?, ?, ?, ?, datetime('now'))",
        )
        .bind(&command.id)
        .bind(&command.device_id)
        .bind(&command.command_type)
        .bind(&command.payload)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(())
    }

    async fn fetch_pending_commands(
        &self,
        device_id: &str,
        limit: u32,
    ) -> Result<Vec<crate::MobileCommand>, StorageError> {
        let rows: Vec<(String, String, String, String, String, Option<String>)> = sqlx::query_as(
            "SELECT id, device_id, command_type, payload, created_at, acked_at
             FROM mobile_commands
             WHERE device_id = ? AND acked_at IS NULL
             ORDER BY created_at ASC LIMIT ?",
        )
        .bind(device_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;

        Ok(rows
            .into_iter()
            .map(
                |(id, device_id, command_type, payload, created_at, acked_at)| {
                    crate::MobileCommand {
                        id,
                        device_id,
                        command_type,
                        payload,
                        created_at,
                        acked_at,
                    }
                },
            )
            .collect())
    }

    async fn ack_mobile_commands(
        &self,
        device_id: &str,
        command_ids: &[String],
    ) -> Result<u64, StorageError> {
        if command_ids.is_empty() {
            return Ok(0);
        }
        let placeholders: Vec<&str> = command_ids.iter().map(|_| "?").collect();
        let sql = format!(
            "UPDATE mobile_commands SET acked_at = datetime('now') WHERE device_id = ? AND id IN ({})",
            placeholders.join(",")
        );
        let mut query = sqlx::query(&sql).bind(device_id);
        for id in command_ids {
            query = query.bind(id);
        }
        let result = query
            .execute(&self.pool)
            .await
            .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn cleanup_acked_commands(&self, older_than_secs: i64) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM mobile_commands WHERE acked_at IS NOT NULL AND acked_at < datetime('now', '-' || ? || ' seconds')",
        )
        .bind(older_than_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }

    async fn cleanup_expired_commands(&self, ttl_secs: i64) -> Result<u64, StorageError> {
        let result = sqlx::query(
            "DELETE FROM mobile_commands WHERE acked_at IS NULL AND created_at < datetime('now', '-' || ? || ' seconds')",
        )
        .bind(ttl_secs)
        .execute(&self.pool)
        .await
        .map_err(|e| StorageError::Query(e.to_string()))?;
        Ok(result.rows_affected())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AdminStore, ExecutionTreeStore, InstanceStore, OutputStore, SequenceStore, SignalStore,
        TelemetryStore, WorkerStore,
    };
    use orch8_types::context::ExecutionContext;
    use orch8_types::instance::{InstanceState, Priority, TaskInstance};
    use orch8_types::sequence::{BlockDefinition, SequenceStatus, StepDef};

    #[tokio::test]
    async fn usage_events_record_and_aggregate() {
        use chrono::{Duration, Utc};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let mk = |tenant: &str, model: &str, inp: i64, out: i64| crate::UsageEvent {
            tenant_id: tenant.into(),
            instance_id: None,
            block_id: Some("b".into()),
            kind: "llm_tokens".into(),
            model: model.into(),
            input_tokens: inp,
            output_tokens: out,
            created_at: Utc::now(),
        };
        storage
            .record_usage_event(&mk("t1", "gpt-4o", 100, 50))
            .await
            .unwrap();
        storage
            .record_usage_event(&mk("t1", "gpt-4o", 30, 20))
            .await
            .unwrap();
        storage
            .record_usage_event(&mk("t1", "claude", 10, 5))
            .await
            .unwrap();
        // Different tenant — must be excluded from t1's aggregation.
        storage
            .record_usage_event(&mk("t2", "gpt-4o", 999, 999))
            .await
            .unwrap();

        let agg = storage
            .query_usage(
                "t1",
                Utc::now() - Duration::hours(1),
                Utc::now() + Duration::hours(1),
            )
            .await
            .unwrap();
        assert_eq!(agg.len(), 2, "grouped by (kind, model)");
        let gpt = agg.iter().find(|a| a.model == "gpt-4o").unwrap();
        assert_eq!(
            (gpt.events, gpt.input_tokens, gpt.output_tokens),
            (2, 130, 70)
        );
        let claude = agg.iter().find(|a| a.model == "claude").unwrap();
        assert_eq!(
            (claude.events, claude.input_tokens, claude.output_tokens),
            (1, 10, 5)
        );
        assert!(agg.iter().all(|a| a.input_tokens < 999), "t2 excluded");

        // Window outside the events → empty.
        let future = Utc::now() + Duration::days(2);
        assert!(storage
            .query_usage("t1", future, future + Duration::hours(1))
            .await
            .unwrap()
            .is_empty());
    }

    #[tokio::test]
    async fn sqlite_roundtrip_sequence() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let seq = orch8_types::sequence::SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            name: "test_seq".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("s1"),
                handler: "noop".into(),
                params: serde_json::Value::Null,
                delay: None,
                retry: None,
                timeout: None,
                rate_limit_key: None,
                send_window: None,
                context_access: None,
                cancellable: true,
                wait_for_input: None,
                queue_name: None,
                deadline: None,
                on_deadline_breach: None,
                fallback_handler: None,
                cache_key: None,
            }))],
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
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            state: InstanceState::Scheduled,
            next_fire_at: Some(now),
            priority: Priority::High,
            timezone: "UTC".into(),
            metadata: serde_json::json!({"key": "val"}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&inst).await.unwrap();
        let fetched = storage.get_instance(inst.id).await.unwrap().unwrap();
        assert_eq!(fetched.tenant_id.as_str(), "t1");
        assert_eq!(fetched.priority, Priority::High);
    }

    #[tokio::test]
    async fn sqlite_claim_due_instances() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();
        let inst = TaskInstance {
            id: InstanceId::new(),
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            state: InstanceState::Scheduled,
            next_fire_at: Some(now - chrono::Duration::seconds(10)),
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
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

    #[tokio::test]
    async fn record_or_get_emit_dedupe_first_call_inserts() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let parent = InstanceId::new();
        let candidate = InstanceId::new();

        let outcome = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k1", candidate)
            .await
            .unwrap();

        assert_eq!(outcome, EmitDedupeOutcome::Inserted);
    }

    #[tokio::test]
    async fn record_or_get_emit_dedupe_second_call_returns_existing() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let parent = InstanceId::new();
        let first = InstanceId::new();
        let second = InstanceId::new();

        let _ = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k", first)
            .await
            .unwrap();
        let outcome = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k", second)
            .await
            .unwrap();

        assert_eq!(outcome, EmitDedupeOutcome::AlreadyExists(first));
    }

    #[tokio::test]
    async fn record_or_get_emit_dedupe_per_parent_isolation() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let p1 = InstanceId::new();
        let p2 = InstanceId::new();
        let c1 = InstanceId::new();
        let c2 = InstanceId::new();

        let o1 = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(p1), "k", c1)
            .await
            .unwrap();
        let o2 = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(p2), "k", c2)
            .await
            .unwrap();

        assert_eq!(o1, EmitDedupeOutcome::Inserted);
        assert_eq!(o2, EmitDedupeOutcome::Inserted);
    }

    #[tokio::test]
    async fn record_or_get_emit_dedupe_concurrent_one_winner() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = std::sync::Arc::new(SqliteStorage::in_memory().await.unwrap());
        let parent = InstanceId::new();

        let candidates: Vec<_> = (0..10).map(|_| InstanceId::new()).collect();
        let mut handles = Vec::new();
        for cand in candidates.iter().copied() {
            let s = storage.clone();
            handles.push(tokio::spawn(async move {
                s.record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "race", cand)
                    .await
                    .unwrap()
            }));
        }

        let mut inserted = 0;
        let mut existing_ids = std::collections::HashSet::new();
        for h in handles {
            match h.await.unwrap() {
                EmitDedupeOutcome::Inserted => inserted += 1,
                EmitDedupeOutcome::AlreadyExists(id) => {
                    existing_ids.insert(id);
                }
            }
        }
        assert_eq!(inserted, 1, "exactly one task should win the race");
        assert_eq!(
            existing_ids.len(),
            1,
            "all losers should observe the same winner id"
        );
    }

    /// T15: TTL sweep deletes dedupe rows older than the cutoff and leaves
    /// recent rows in place. We backdate one row via a direct UPDATE since
    /// `created_at` is set by the default clause on insert.
    #[tokio::test]
    async fn delete_expired_emit_event_dedupe_removes_old_rows() {
        use crate::{DedupeScope, EmitDedupeOutcome};

        let storage = SqliteStorage::in_memory().await.unwrap();
        let parent = InstanceId::new();
        let old_child = InstanceId::new();
        let fresh_child = InstanceId::new();

        // Insert two dedupe rows with different keys under the same parent.
        storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "old", old_child)
            .await
            .unwrap();
        storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "fresh", fresh_child)
            .await
            .unwrap();

        // Backdate the "old" row to 40 days ago (past the 30d TTL).
        let backdated = (chrono::Utc::now() - chrono::Duration::days(40)).to_rfc3339();
        sqlx::query(
            "UPDATE emit_event_dedupe SET created_at = ?1
             WHERE scope_kind = 'parent' AND scope_value = ?2 AND dedupe_key = ?3",
        )
        .bind(&backdated)
        .bind(parent.to_string())
        .bind("old")
        .execute(&storage.pool)
        .await
        .unwrap();

        // Sweep with cutoff = now - 30d.
        let cutoff = chrono::Utc::now() - chrono::Duration::days(30);
        let deleted = storage
            .delete_expired_emit_event_dedupe(cutoff, 100)
            .await
            .unwrap();
        assert_eq!(deleted, 1, "only the backdated row should be swept");

        // Fresh row still resolves (second call -> AlreadyExists), old row was
        // removed (second call -> Inserted, since the row is gone).
        let fresh_outcome = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "fresh", InstanceId::new())
            .await
            .unwrap();
        assert_eq!(fresh_outcome, EmitDedupeOutcome::AlreadyExists(fresh_child));

        let old_outcome = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "old", InstanceId::new())
            .await
            .unwrap();
        assert_eq!(
            old_outcome,
            EmitDedupeOutcome::Inserted,
            "old row should be gone, letting a fresh insert succeed"
        );
    }

    /// Idempotent sweep -- a second pass with an unchanged cutoff finds
    /// nothing to delete. Guards against an accidental infinite-delete loop.
    #[tokio::test]
    async fn delete_expired_emit_event_dedupe_on_empty_store_is_zero() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let cutoff = chrono::Utc::now();
        let deleted = storage
            .delete_expired_emit_event_dedupe(cutoff, 100)
            .await
            .unwrap();
        assert_eq!(deleted, 0);
    }

    // === R4 / finding #2: atomic dedupe + instance creation =================

    fn mk_inst_for_dedupe(id: InstanceId) -> TaskInstance {
        let now = Utc::now();
        TaskInstance {
            id,
            sequence_id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            state: InstanceState::Scheduled,
            next_fire_at: Some(now),
            priority: Priority::Normal,
            timezone: String::new(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        }
    }

    #[tokio::test]
    async fn create_instance_with_dedupe_first_call_persists_instance() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let parent = InstanceId::new();
        let child_id = InstanceId::new();
        let inst = mk_inst_for_dedupe(child_id);

        let outcome = storage
            .create_instance_with_dedupe(&DedupeScope::Parent(parent), "k1", &inst)
            .await
            .unwrap();

        assert_eq!(outcome, EmitDedupeOutcome::Inserted);
        // Storage invariant: Inserted implies the child instance is present.
        let fetched = storage.get_instance(child_id).await.unwrap();
        assert!(
            fetched.is_some(),
            "Inserted outcome must leave the child instance persisted"
        );
    }

    #[tokio::test]
    async fn create_instance_with_dedupe_second_call_skips_instance_insert() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let parent = InstanceId::new();
        let first_id = InstanceId::new();
        let second_id = InstanceId::new();
        let inst1 = mk_inst_for_dedupe(first_id);
        let inst2 = mk_inst_for_dedupe(second_id);

        let o1 = storage
            .create_instance_with_dedupe(&DedupeScope::Parent(parent), "k", &inst1)
            .await
            .unwrap();
        let o2 = storage
            .create_instance_with_dedupe(&DedupeScope::Parent(parent), "k", &inst2)
            .await
            .unwrap();

        assert_eq!(o1, EmitDedupeOutcome::Inserted);
        assert_eq!(o2, EmitDedupeOutcome::AlreadyExists(first_id));

        // First instance persisted, second NOT inserted (dedupe rejected it).
        assert!(storage.get_instance(first_id).await.unwrap().is_some());
        assert!(
            storage.get_instance(second_id).await.unwrap().is_none(),
            "AlreadyExists must NOT create the candidate instance"
        );
    }

    #[tokio::test]
    async fn create_instance_with_dedupe_different_parents_both_insert() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let p1 = InstanceId::new();
        let p2 = InstanceId::new();
        let c1_id = InstanceId::new();
        let c2_id = InstanceId::new();

        let o1 = storage
            .create_instance_with_dedupe(&DedupeScope::Parent(p1), "k", &mk_inst_for_dedupe(c1_id))
            .await
            .unwrap();
        let o2 = storage
            .create_instance_with_dedupe(&DedupeScope::Parent(p2), "k", &mk_inst_for_dedupe(c2_id))
            .await
            .unwrap();

        assert_eq!(o1, EmitDedupeOutcome::Inserted);
        assert_eq!(o2, EmitDedupeOutcome::Inserted);
        assert!(storage.get_instance(c1_id).await.unwrap().is_some());
        assert!(storage.get_instance(c2_id).await.unwrap().is_some());
        assert_ne!(c1_id, c2_id);
    }

    // === R7: tenant-scope dedupe ===========================================

    /// Two different parents in the SAME tenant, same dedupe key under
    /// tenant scope -> second call is deduped to the first child. Proves
    /// tenant-wide at-most-once fan-out works.
    #[tokio::test]
    async fn create_instance_with_dedupe_tenant_scope_dedupes_across_parents() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let tenant = TenantId::unchecked("acme");
        let first_id = InstanceId::new();
        let second_id = InstanceId::new();
        let mut inst1 = mk_inst_for_dedupe(first_id);
        inst1.tenant_id = tenant.clone();
        let mut inst2 = mk_inst_for_dedupe(second_id);
        inst2.tenant_id = tenant.clone();

        let o1 = storage
            .create_instance_with_dedupe(&DedupeScope::Tenant(tenant.clone()), "welcome", &inst1)
            .await
            .unwrap();
        let o2 = storage
            .create_instance_with_dedupe(&DedupeScope::Tenant(tenant.clone()), "welcome", &inst2)
            .await
            .unwrap();

        assert_eq!(o1, EmitDedupeOutcome::Inserted);
        assert_eq!(
            o2,
            EmitDedupeOutcome::AlreadyExists(first_id),
            "second tenant-scope call must dedupe to the first child"
        );
        assert!(storage.get_instance(first_id).await.unwrap().is_some());
        assert!(
            storage.get_instance(second_id).await.unwrap().is_none(),
            "tenant-scope AlreadyExists must NOT persist the second candidate"
        );
    }

    /// Same dedupe key under tenant scope in TWO DIFFERENT tenants -> both
    /// succeed independently. Proves tenant isolation.
    #[tokio::test]
    async fn create_instance_with_dedupe_tenant_scope_isolates_tenants() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let t1 = TenantId::unchecked("acme");
        let t2 = TenantId::unchecked("globex");
        let c1 = InstanceId::new();
        let c2 = InstanceId::new();
        let mut i1 = mk_inst_for_dedupe(c1);
        i1.tenant_id = t1.clone();
        let mut i2 = mk_inst_for_dedupe(c2);
        i2.tenant_id = t2.clone();

        let o1 = storage
            .create_instance_with_dedupe(&DedupeScope::Tenant(t1), "k", &i1)
            .await
            .unwrap();
        let o2 = storage
            .create_instance_with_dedupe(&DedupeScope::Tenant(t2), "k", &i2)
            .await
            .unwrap();

        assert_eq!(o1, EmitDedupeOutcome::Inserted);
        assert_eq!(o2, EmitDedupeOutcome::Inserted);
        assert_ne!(c1, c2);
    }

    /// Parent scope and tenant scope share the same dedupe_key but land in
    /// independent namespaces (scope_kind is part of the PK), so both insert.
    #[tokio::test]
    async fn create_instance_with_dedupe_parent_and_tenant_scopes_are_independent() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let parent = InstanceId::new();
        let tenant = TenantId::unchecked("acme");
        let cp = InstanceId::new();
        let ct = InstanceId::new();
        let mut ip = mk_inst_for_dedupe(cp);
        ip.tenant_id = tenant.clone();
        let mut it = mk_inst_for_dedupe(ct);
        it.tenant_id = tenant.clone();

        let op = storage
            .create_instance_with_dedupe(&DedupeScope::Parent(parent), "k", &ip)
            .await
            .unwrap();
        let ot = storage
            .create_instance_with_dedupe(&DedupeScope::Tenant(tenant), "k", &it)
            .await
            .unwrap();

        assert_eq!(op, EmitDedupeOutcome::Inserted);
        assert_eq!(ot, EmitDedupeOutcome::Inserted);
        assert_ne!(cp, ct);
    }

    /// R4 atomicity guarantee: if the instance INSERT fails AFTER the dedupe
    /// row has been inserted inside the same transaction, the dedupe row must
    /// be rolled back too. Otherwise a crashed/failed create would leave a
    /// stale `(parent, key)` row pointing at a non-existent instance, and a
    /// subsequent retry would be wrongly deduped to a ghost id.
    ///
    /// We force the failure via a duplicate `task_instances.id` (PK conflict)
    /// -- independent of FK enforcement, which is off by default in SQLite.
    #[tokio::test]
    async fn create_instance_with_dedupe_rolls_back_dedupe_on_insert_failure() {
        use crate::{DedupeScope, EmitDedupeOutcome};
        let storage = SqliteStorage::in_memory().await.unwrap();
        let parent = InstanceId::new();
        let colliding_id = InstanceId::new();

        // Pre-seed a row with the same id so the second insert hits a PK
        // conflict, forcing the instance INSERT inside the dedupe tx to fail.
        let pre = mk_inst_for_dedupe(colliding_id);
        storage.create_instance(&pre).await.unwrap();

        // Build a distinct instance with the SAME id (collides on PK) but a
        // fresh sequence so there's no doubt about why the insert fails.
        let colliding = mk_inst_for_dedupe(colliding_id);

        let result = storage
            .create_instance_with_dedupe(&DedupeScope::Parent(parent), "k1", &colliding)
            .await;
        assert!(
            result.is_err(),
            "instance insert must fail on duplicate PK, but got {result:?}"
        );

        // Key claim: the dedupe row was rolled back with the failed insert.
        // A fresh candidate under the same (parent, key) must be accepted as
        // Inserted, not returned as AlreadyExists.
        let fresh_candidate = InstanceId::new();
        let retry = storage
            .record_or_get_emit_dedupe(&DedupeScope::Parent(parent), "k1", fresh_candidate)
            .await
            .unwrap();
        assert_eq!(
            retry,
            EmitDedupeOutcome::Inserted,
            "failed create_instance_with_dedupe must NOT leave a dedupe row \
             behind -- retry should see a fresh slot"
        );

        // And the instance that failed to insert must be absent. Only the
        // pre-seeded row with `colliding_id` exists, so the contract here is
        // simply: neither insert from the failing call survived as a new row.
        let still_pre = storage.get_instance(colliding_id).await.unwrap();
        assert!(
            still_pre.is_some(),
            "pre-seeded row must still be present (it was never part of the tx)"
        );
    }

    // === R5 / finding #4: atomic signal enqueue gated on non-terminal state =

    fn mk_signal_for(target: InstanceId) -> orch8_types::signal::Signal {
        orch8_types::signal::Signal {
            id: Uuid::now_v7(),
            instance_id: target,
            signal_type: orch8_types::signal::SignalType::Cancel,
            payload: serde_json::Value::Null,
            delivered: false,
            created_at: Utc::now(),
            delivered_at: None,
        }
    }

    #[tokio::test]
    async fn enqueue_signal_if_active_succeeds_when_target_running() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let target = mk_inst_for_dedupe(InstanceId::new());
        // mk_inst_for_dedupe seeds `Scheduled`; flip to Running so the
        // non-terminal branch is exercised explicitly.
        storage.create_instance(&target).await.unwrap();
        storage
            .update_instance_state(target.id, InstanceState::Running, None)
            .await
            .unwrap();

        let signal = mk_signal_for(target.id);
        storage.enqueue_signal_if_active(&signal).await.unwrap();

        let pending = storage.get_pending_signals(target.id).await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].id, signal.id);
    }

    /// Core R5 invariant: rejected calls must NOT leave a stale row in
    /// `signal_inbox`. This is the guarantee the atomic path was introduced to
    /// provide -- a permanent rejection must roll back cleanly.
    #[tokio::test]
    async fn enqueue_signal_if_active_rejects_terminal_and_leaves_no_row() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let target = mk_inst_for_dedupe(InstanceId::new());
        storage.create_instance(&target).await.unwrap();
        storage
            .update_instance_state(target.id, InstanceState::Running, None)
            .await
            .unwrap();
        storage
            .update_instance_state(target.id, InstanceState::Completed, None)
            .await
            .unwrap();

        let signal = mk_signal_for(target.id);
        let err = storage
            .enqueue_signal_if_active(&signal)
            .await
            .expect_err("expected TerminalTarget on terminal target");
        assert!(
            matches!(err, StorageError::TerminalTarget { .. }),
            "expected TerminalTarget, got: {err:?}"
        );

        let pending = storage.get_pending_signals(target.id).await.unwrap();
        assert!(
            pending.is_empty(),
            "rejected enqueue_signal_if_active must not persist a row, got: {pending:?}"
        );
    }

    #[tokio::test]
    async fn enqueue_signal_if_active_returns_not_found_for_missing_target() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let missing = InstanceId::new();
        let signal = mk_signal_for(missing);

        let err = storage
            .enqueue_signal_if_active(&signal)
            .await
            .expect_err("expected NotFound for missing target");
        assert!(
            matches!(
                err,
                StorageError::NotFound {
                    entity: "task_instance",
                    ..
                }
            ),
            "expected NotFound, got: {err:?}"
        );

        let pending = storage.get_pending_signals(missing).await.unwrap();
        assert!(pending.is_empty());
    }

    /// Fix-2 regression: a corrupted `state` column (unknown string) MUST NOT
    /// silently coerce to `Scheduled` and let the INSERT through. The strict
    /// parser used by `enqueue_if_active` surfaces this as `StorageError::Query`,
    /// matching Postgres's contract.
    #[tokio::test]
    async fn enqueue_signal_if_active_rejects_corrupted_state() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let target = mk_inst_for_dedupe(InstanceId::new());
        storage.create_instance(&target).await.unwrap();

        // Write a junk value directly into the state column. No public API
        // can produce this -- it models a legacy row, a migration bug, or a
        // corrupted file.
        sqlx::query("UPDATE task_instances SET state = ?1 WHERE id = ?2")
            .bind("totally_bogus_state")
            .bind(target.id.to_string())
            .execute(&storage.pool)
            .await
            .unwrap();

        let signal = mk_signal_for(target.id);
        let err = storage
            .enqueue_signal_if_active(&signal)
            .await
            .expect_err("expected Query error on corrupted state");
        match &err {
            StorageError::Query(msg) => assert!(
                msg.contains("unknown instance state"),
                "expected 'unknown instance state' in message, got: {msg}"
            ),
            other => panic!("expected StorageError::Query, got: {other:?}"),
        }

        // And crucially: no row leaked through.
        let pending = storage.get_pending_signals(target.id).await.unwrap();
        assert!(
            pending.is_empty(),
            "corrupted-state rejection must not persist a signal, got: {pending:?}"
        );
    }

    #[tokio::test]
    async fn fk_cascade_deletes_child_rows_when_instance_deleted() {
        use orch8_types::execution::{BlockType, ExecutionNode};
        use orch8_types::output::BlockOutput;
        use orch8_types::signal::{Signal, SignalType};
        use orch8_types::worker::{WorkerTask, WorkerTaskState};

        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = Utc::now();

        let seq = orch8_types::sequence::SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            name: "fk-test".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("s1"),
                handler: "h".into(),
                params: serde_json::Value::Null,
                delay: None,
                retry: None,
                timeout: None,
                rate_limit_key: None,
                send_window: None,
                context_access: None,
                cancellable: true,
                wait_for_input: None,
                queue_name: None,
                deadline: None,
                on_deadline_breach: None,
                fallback_handler: None,
                cache_key: None,
            }))],
            interceptors: None,
            created_at: now,
        };
        storage.create_sequence(&seq).await.unwrap();

        let inst = TaskInstance {
            id: InstanceId::new(),
            sequence_id: seq.id,
            tenant_id: TenantId::unchecked("t"),
            namespace: Namespace::new("ns"),
            state: InstanceState::Running,
            next_fire_at: None,
            priority: Priority::Normal,
            timezone: "UTC".into(),
            metadata: serde_json::json!({}),
            context: ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            created_at: now,
            updated_at: now,
        };
        storage.create_instance(&inst).await.unwrap();

        // Seed child rows
        storage
            .create_execution_node(&ExecutionNode {
                id: ExecutionNodeId::new(),
                instance_id: inst.id,
                block_id: BlockId::new("s1"),
                parent_id: None,
                block_type: BlockType::Step,
                branch_index: None,
                state: NodeState::Running,
                started_at: Some(now),
                completed_at: None,
            })
            .await
            .unwrap();

        storage
            .save_block_output(&BlockOutput {
                id: Uuid::now_v7(),
                instance_id: inst.id,
                block_id: BlockId::new("s1"),
                output: serde_json::json!({}),
                output_ref: None,
                output_size: 0,
                attempt: 1,
                created_at: now,
            })
            .await
            .unwrap();

        storage
            .enqueue_signal(&Signal {
                id: Uuid::now_v7(),
                instance_id: inst.id,
                signal_type: SignalType::Custom("sig".into()),
                payload: serde_json::json!({}),
                delivered: false,
                created_at: now,
                delivered_at: None,
            })
            .await
            .unwrap();

        let wt_id = Uuid::now_v7();
        storage
            .create_worker_task(&WorkerTask {
                id: wt_id,
                instance_id: inst.id,
                block_id: BlockId::new("s1"),
                handler_name: "h".into(),
                queue_name: None,
                params: serde_json::json!({}),
                context: serde_json::json!({}),
                attempt: 0,
                timeout_ms: None,
                state: WorkerTaskState::Pending,
                worker_id: None,
                claimed_at: None,
                heartbeat_at: None,
                completed_at: None,
                output: None,
                error_message: None,
                error_retryable: None,
                created_at: now,
            })
            .await
            .unwrap();

        // Verify child rows exist
        assert!(!storage
            .get_execution_tree(inst.id)
            .await
            .unwrap()
            .is_empty());
        assert!(storage.get_worker_task(wt_id).await.unwrap().is_some());

        // Delete instance via raw SQL (no trait method exists)
        sqlx::query("DELETE FROM task_instances WHERE id = ?1")
            .bind(inst.id.into_uuid().to_string())
            .execute(&storage.pool)
            .await
            .unwrap();

        // All child rows must be cascade-deleted
        assert!(storage
            .get_execution_tree(inst.id)
            .await
            .unwrap()
            .is_empty());
        assert!(storage.get_worker_task(wt_id).await.unwrap().is_none());
        assert!(storage
            .get_pending_signals(inst.id)
            .await
            .unwrap()
            .is_empty());
    }

    // -- Rollback policy tests --

    #[tokio::test]
    async fn sqlite_rollback_policy_crud() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let tenant = "t1";
        let seq = "seq-a";

        // Create
        storage
            .create_rollback_policy(tenant, seq, 0.1, 300, None, None, None)
            .await
            .unwrap();

        // Read
        let policy = storage
            .get_rollback_policy(tenant, seq)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(policy.tenant_id, tenant);
        assert_eq!(policy.sequence_name, seq);
        assert!((policy.error_rate_threshold - 0.1).abs() < f64::EPSILON);
        assert_eq!(policy.time_window_secs, 300);
        assert!(policy.enabled);
        assert!(policy.webhook_url.is_none());

        // Update via create (upsert)
        storage
            .create_rollback_policy(
                tenant,
                seq,
                0.2,
                600,
                None,
                None,
                Some("https://hooks.example.com/rollback"),
            )
            .await
            .unwrap();
        let policy = storage
            .get_rollback_policy(tenant, seq)
            .await
            .unwrap()
            .unwrap();
        assert!((policy.error_rate_threshold - 0.2).abs() < f64::EPSILON);
        assert_eq!(policy.time_window_secs, 600);
        assert_eq!(
            policy.webhook_url.as_deref(),
            Some("https://hooks.example.com/rollback")
        );

        // List
        let policies = storage
            .list_rollback_policies(Some(tenant), 100)
            .await
            .unwrap();
        assert_eq!(policies.len(), 1);

        // List all tenants
        storage
            .create_rollback_policy("t2", seq, 0.05, 100, None, None, None)
            .await
            .unwrap();
        let all = storage.list_rollback_policies(None, 100).await.unwrap();
        assert_eq!(all.len(), 2);

        // Delete
        storage.delete_rollback_policy(tenant, seq).await.unwrap();
        assert!(storage
            .get_rollback_policy(tenant, seq)
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn sqlite_rollback_history_recorded() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        storage
            .record_rollback("t1", "seq-x", 0.15, 0.1, "threshold_breach")
            .await
            .unwrap();

        let rows: Vec<(i64, String, String, f64, f64, String)> =
            sqlx::query_as("SELECT id, tenant_id, sequence_name, error_rate, threshold, reason FROM rollback_history")
                .fetch_all(&storage.pool)
                .await
                .unwrap();
        assert_eq!(rows.len(), 1);
        let (_, tenant, seq, rate, threshold, reason) = &rows[0];
        assert_eq!(tenant, "t1");
        assert_eq!(seq, "seq-x");
        assert!((rate - 0.15).abs() < f64::EPSILON);
        assert!((threshold - 0.1).abs() < f64::EPSILON);
        assert_eq!(reason, "threshold_breach");
    }

    #[tokio::test]
    async fn sqlite_query_error_rate_computes_from_telemetry() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = "seq-y";

        // Insert 2 failed and 1 success event
        for _ in 0..2 {
            storage
                .ingest_telemetry_error(
                    "RuntimeError",
                    "boom",
                    None,
                    "d1",
                    "iOS",
                    "17",
                    "1.0",
                    "0.1",
                    "t1",
                    Some("i1"),
                    Some(seq),
                )
                .await
                .unwrap();
        }
        storage
            .ingest_telemetry_event(
                "InstanceCompleted",
                &serde_json::json!({"sequence_name": seq}).to_string(),
                "d1",
                "iOS",
                "17",
                "1.0",
                "0.1",
                "t1",
                chrono::Utc::now(),
            )
            .await
            .unwrap();

        let rate = storage.query_error_rate("t1", seq, 3600).await.unwrap();
        assert!(rate.is_some());
        // 2 failed out of 3 total where sequence_name matches
        // Wait - the success event also has sequence_name in payload,
        // so total = 3, failed = 2
        assert!((rate.unwrap() - 0.666_666).abs() < 0.001);
    }

    #[tokio::test]
    async fn sqlite_list_rollback_history_filtered() {
        let storage = SqliteStorage::in_memory().await.unwrap();

        storage
            .record_rollback("t1", "seq-a", 0.1, 0.05, "r1")
            .await
            .unwrap();
        storage
            .record_rollback("t1", "seq-b", 0.2, 0.1, "r2")
            .await
            .unwrap();
        storage
            .record_rollback("t2", "seq-a", 0.3, 0.15, "r3")
            .await
            .unwrap();

        // All
        let all = storage
            .list_rollback_history(None, None, 100)
            .await
            .unwrap();
        assert_eq!(all.len(), 3);

        // Filter by tenant
        let t1 = storage
            .list_rollback_history(Some("t1"), None, 100)
            .await
            .unwrap();
        assert_eq!(t1.len(), 2);

        // Filter by sequence
        let seq_a = storage
            .list_rollback_history(None, Some("seq-a"), 100)
            .await
            .unwrap();
        assert_eq!(seq_a.len(), 2);

        // Filter by both
        let both = storage
            .list_rollback_history(Some("t1"), Some("seq-a"), 100)
            .await
            .unwrap();
        assert_eq!(both.len(), 1);
        assert!((both[0].error_rate - 0.1).abs() < f64::EPSILON);

        // Limit
        let limited = storage.list_rollback_history(None, None, 2).await.unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn sqlite_query_error_rate_returns_none_when_no_events() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let rate = storage.query_error_rate("t1", "seq-z", 3600).await.unwrap();
        assert!(rate.is_none());
    }

    #[tokio::test]
    async fn sqlite_ingest_telemetry_event_enriches_payload_with_tenant_id() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = chrono::Utc::now();
        storage
            .ingest_telemetry_event(
                "InstanceCompleted",
                r#"{"sequence_name":"seq1"}"#,
                "dev1",
                "iOS",
                "17",
                "1.0",
                "0.4.0",
                "my-tenant",
                now,
            )
            .await
            .unwrap();

        let row: (String,) =
            sqlx::query_as("SELECT payload FROM telemetry_events ORDER BY id DESC LIMIT 1")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.0).unwrap();
        assert_eq!(payload["tenant_id"], "my-tenant");
        assert_eq!(payload["sequence_name"], "seq1");
    }

    #[tokio::test]
    async fn sqlite_ingest_telemetry_event_preserves_existing_tenant_id() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = chrono::Utc::now();
        storage
            .ingest_telemetry_event(
                "InstanceCompleted",
                r#"{"sequence_name":"seq1","tenant_id":"original"}"#,
                "dev1",
                "iOS",
                "17",
                "1.0",
                "0.4.0",
                "overrider",
                now,
            )
            .await
            .unwrap();

        let row: (String,) =
            sqlx::query_as("SELECT payload FROM telemetry_events ORDER BY id DESC LIMIT 1")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.0).unwrap();
        assert_eq!(payload["tenant_id"], "original");
    }

    #[tokio::test]
    async fn sqlite_ingest_error_includes_tenant_id() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        storage
            .ingest_telemetry_error(
                "RuntimeError",
                "step failed",
                None,
                "dev1",
                "iOS",
                "17",
                "1.0",
                "0.4.0",
                "t1",
                None,
                Some("seq-x"),
            )
            .await
            .unwrap();

        let row: (String,) =
            sqlx::query_as("SELECT payload FROM telemetry_events ORDER BY id DESC LIMIT 1")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.0).unwrap();
        assert_eq!(payload["tenant_id"], "t1");
        assert_eq!(payload["sequence_name"], "seq-x");
    }

    #[tokio::test]
    async fn sqlite_query_error_rate_filters_by_tenant() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = chrono::Utc::now();

        // Tenant A: 1 success event
        storage
            .ingest_telemetry_event(
                "InstanceCompleted",
                r#"{"sequence_name":"seq1"}"#,
                "d1",
                "iOS",
                "17",
                "1.0",
                "0.4.0",
                "tenant-a",
                now,
            )
            .await
            .unwrap();

        // Tenant B: 1 failed event
        storage
            .ingest_telemetry_error(
                "RuntimeError",
                "fail",
                None,
                "d1",
                "iOS",
                "17",
                "1.0",
                "0.4.0",
                "tenant-b",
                None,
                Some("seq1"),
            )
            .await
            .unwrap();

        // Tenant A should have 0% error rate (1 success, 0 failures)
        let rate_a = storage
            .query_error_rate("tenant-a", "seq1", 3600)
            .await
            .unwrap();
        assert_eq!(rate_a, Some(0.0));

        // Tenant B should have 100% error rate (0 success, 1 failure)
        let rate_b = storage
            .query_error_rate("tenant-b", "seq1", 3600)
            .await
            .unwrap();
        assert_eq!(rate_b, Some(1.0));
    }

    #[tokio::test]
    async fn sqlite_ingest_telemetry_events_batch_inserts_multiple() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = chrono::Utc::now();
        let events = vec![
            crate::TelemetryEvent {
                event_type: "InstanceCompleted".to_string(),
                payload: r#"{"sequence_name":"seq1"}"#.to_string(),
                device_id: "d1".to_string(),
                os_name: "iOS".to_string(),
                os_version: "17".to_string(),
                app_version: "1.0".to_string(),
                sdk_version: "0.4.0".to_string(),
                tenant_id: "t1".to_string(),
                created_at: now,
            },
            crate::TelemetryEvent {
                event_type: "InstanceFailed".to_string(),
                payload: r#"{"sequence_name":"seq2"}"#.to_string(),
                device_id: "d2".to_string(),
                os_name: "Android".to_string(),
                os_version: "14".to_string(),
                app_version: "2.0".to_string(),
                sdk_version: "0.4.0".to_string(),
                tenant_id: "t1".to_string(),
                created_at: now,
            },
        ];

        let count = storage
            .ingest_telemetry_events_batch(&events)
            .await
            .unwrap();
        assert_eq!(count, 2);

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM telemetry_events")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(row.0, 2);
    }

    #[tokio::test]
    async fn sqlite_list_rollback_policies_respects_limit() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        for i in 0..5 {
            storage
                .create_rollback_policy("t1", &format!("seq-{i}"), 0.1, 300, None, None, None)
                .await
                .unwrap();
        }

        let all = storage
            .list_rollback_policies(Some("t1"), 100)
            .await
            .unwrap();
        assert_eq!(all.len(), 5);

        let limited = storage.list_rollback_policies(Some("t1"), 2).await.unwrap();
        assert_eq!(limited.len(), 2);
    }

    #[tokio::test]
    async fn sqlite_delete_old_telemetry_events_removes_old_rows() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let old = chrono::Utc::now() - chrono::Duration::days(60);
        let recent = chrono::Utc::now();

        storage
            .ingest_telemetry_event(
                "OldEvent", "{}", "d1", "iOS", "17", "1.0", "0.4.0", "t1", old,
            )
            .await
            .unwrap();
        storage
            .ingest_telemetry_event(
                "RecentEvent",
                "{}",
                "d1",
                "iOS",
                "17",
                "1.0",
                "0.4.0",
                "t1",
                recent,
            )
            .await
            .unwrap();

        let cutoff = chrono::Utc::now() - chrono::Duration::days(30);
        let deleted = storage
            .delete_old_telemetry_events(cutoff, 100)
            .await
            .unwrap();
        assert_eq!(deleted, 1);

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM telemetry_events")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(row.0, 1);
    }

    #[tokio::test]
    async fn sqlite_batch_telemetry_enriches_tenant_id() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = chrono::Utc::now();
        let events = vec![crate::TelemetryEvent {
            event_type: "InstanceCompleted".to_string(),
            payload: r#"{"sequence_name":"seq1"}"#.to_string(),
            device_id: "d1".to_string(),
            os_name: "iOS".to_string(),
            os_version: "17".to_string(),
            app_version: "1.0".to_string(),
            sdk_version: "0.4.0".to_string(),
            tenant_id: "batch-tenant".to_string(),
            created_at: now,
        }];

        storage
            .ingest_telemetry_events_batch(&events)
            .await
            .unwrap();

        let row: (String,) =
            sqlx::query_as("SELECT payload FROM telemetry_events ORDER BY id DESC LIMIT 1")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.0).unwrap();
        assert_eq!(payload["tenant_id"], "batch-tenant");
        assert_eq!(payload["sequence_name"], "seq1");
    }

    #[tokio::test]
    async fn sqlite_batch_telemetry_preserves_existing_tenant_id() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let now = chrono::Utc::now();
        let events = vec![crate::TelemetryEvent {
            event_type: "InstanceCompleted".to_string(),
            payload: r#"{"sequence_name":"s","tenant_id":"original"}"#.to_string(),
            device_id: "d1".to_string(),
            os_name: "iOS".to_string(),
            os_version: "17".to_string(),
            app_version: "1.0".to_string(),
            sdk_version: "0.4.0".to_string(),
            tenant_id: "overrider".to_string(),
            created_at: now,
        }];

        storage
            .ingest_telemetry_events_batch(&events)
            .await
            .unwrap();

        let row: (String,) =
            sqlx::query_as("SELECT payload FROM telemetry_events ORDER BY id DESC LIMIT 1")
                .fetch_one(storage.pool())
                .await
                .unwrap();
        let payload: serde_json::Value = serde_json::from_str(&row.0).unwrap();
        assert_eq!(payload["tenant_id"], "original");
    }

    #[tokio::test]
    async fn sqlite_batch_telemetry_empty_is_noop() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let count = storage.ingest_telemetry_events_batch(&[]).await.unwrap();
        assert_eq!(count, 0);
    }

    #[tokio::test]
    async fn sqlite_delete_old_telemetry_respects_limit() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let old = chrono::Utc::now() - chrono::Duration::days(60);

        for _ in 0..5 {
            storage
                .ingest_telemetry_event(
                    "OldEvent", "{}", "d1", "iOS", "17", "1.0", "0.4.0", "t1", old,
                )
                .await
                .unwrap();
        }

        let cutoff = chrono::Utc::now() - chrono::Duration::days(30);
        let deleted = storage
            .delete_old_telemetry_events(cutoff, 2)
            .await
            .unwrap();
        assert_eq!(deleted, 2);

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM telemetry_events")
            .fetch_one(storage.pool())
            .await
            .unwrap();
        assert_eq!(row.0, 3);
    }

    #[tokio::test]
    async fn update_sequence_status_roundtrip() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        let seq = orch8_types::sequence::SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId::unchecked("t1"),
            namespace: Namespace::new("default"),
            name: "status_test".into(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::default(),
            blocks: vec![],
            interceptors: None,
            created_at: Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();

        let loaded = storage.get_sequence(seq.id).await.unwrap().unwrap();
        assert_eq!(
            loaded.status,
            orch8_types::sequence::SequenceStatus::Production
        );

        storage
            .update_sequence_status(seq.id, "staging")
            .await
            .unwrap();
        let loaded = storage.get_sequence(seq.id).await.unwrap().unwrap();
        assert_eq!(
            loaded.status,
            orch8_types::sequence::SequenceStatus::Staging
        );

        storage
            .update_sequence_status(seq.id, "unpublished")
            .await
            .unwrap();
        let loaded = storage.get_sequence(seq.id).await.unwrap().unwrap();
        assert_eq!(
            loaded.status,
            orch8_types::sequence::SequenceStatus::Unpublished
        );
    }

    #[tokio::test]
    async fn advisory_lock_default_noop() {
        let storage = SqliteStorage::in_memory().await.unwrap();
        storage.acquire_manifest_lock("tenant-1").await.unwrap();
        storage.release_manifest_lock("tenant-1").await.unwrap();
    }
}
