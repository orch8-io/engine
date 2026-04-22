pub mod compression;
pub mod encrypting;
pub mod externalizing;
pub mod postgres;
pub mod sqlite;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

use orch8_types::audit::AuditLogEntry;
use orch8_types::circuit_breaker::CircuitBreakerState;
use orch8_types::cron::CronSchedule;
pub use orch8_types::dedupe::DedupeScope;
use orch8_types::error::StorageError;
use orch8_types::execution::{ExecutionNode, NodeState};
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{
    BlockId, ExecutionNodeId, InstanceId, Namespace, ResourceKey, SequenceId, TenantId,
};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::output::BlockOutput;
use orch8_types::plugin::PluginDef;
use orch8_types::rate_limit::{RateLimit, RateLimitCheck};
use orch8_types::sequence::SequenceDefinition;
use orch8_types::session::Session;
use orch8_types::signal::Signal;
use orch8_types::trigger::TriggerDef;
use orch8_types::worker::WorkerTask;

/// Outcome of a dedupe insert attempt for `emit_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitDedupeOutcome {
    /// First call for (parent, key) — caller proceeds with `candidate_child`.
    Inserted,
    /// Key already used; caller should reuse the existing child instance ID.
    AlreadyExists(orch8_types::ids::InstanceId),
}

/// The core storage abstraction.
///
/// Object-safe for `dyn StorageBackend` dispatch.
/// Every method returns `Result<T, StorageError>`.
/// Batch methods exist for hot paths.
#[async_trait]
pub trait StorageBackend: Send + Sync + 'static {
    // === Sequences ===

    async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError>;

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError>;

    async fn get_sequence_by_name(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
        version: Option<i32>,
    ) -> Result<Option<SequenceDefinition>, StorageError>;

    /// List all versions of a sequence by name.
    async fn list_sequence_versions(
        &self,
        tenant_id: &TenantId,
        namespace: &Namespace,
        name: &str,
    ) -> Result<Vec<SequenceDefinition>, StorageError>;

    /// List all sequences across all names/versions, optionally filtered.
    ///
    /// Returns rows ordered by (`tenant_id`, namespace, name, version DESC) so
    /// the same name's versions cluster together with the newest first.
    async fn list_sequences(
        &self,
        tenant_id: Option<&TenantId>,
        namespace: Option<&Namespace>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<SequenceDefinition>, StorageError>;

    /// Mark a sequence version as deprecated.
    async fn deprecate_sequence(&self, id: SequenceId) -> Result<(), StorageError>;

    /// Delete a sequence by ID.
    async fn delete_sequence(&self, id: SequenceId) -> Result<(), StorageError>;

    // === Task Instances ===

    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError>;

    async fn create_instances_batch(&self, instances: &[TaskInstance])
        -> Result<u64, StorageError>;

    /// Persist a new instance while externalizing large `context.data` fields.
    ///
    /// Contract mirrors [`Self::update_instance_context_externalized`]:
    /// externalized payloads and the marker-swapped instance row land in the
    /// same transaction, so partial failure can't leave dangling markers in
    /// `task_instances.context`.
    ///
    /// When `threshold_bytes == 0` this degenerates to [`Self::create_instance`].
    ///
    /// The default impl is non-atomic (save refs, then insert instance) so
    /// in-memory/test backends keep compiling. Production backends override
    /// with a single transaction.
    async fn create_instance_externalized(
        &self,
        instance: &TaskInstance,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        let mut inst_clone = instance.clone();
        let refs = crate::externalizing::externalize_fields(
            &mut inst_clone.context.data,
            &instance.id.0.to_string(),
            threshold_bytes,
        );
        if !refs.is_empty() {
            self.batch_save_externalized_state(instance.id, &refs)
                .await?;
        }
        self.create_instance(&inst_clone).await
    }

    /// Batched counterpart of [`Self::create_instance_externalized`].
    ///
    /// Each instance's context is independently externalized (its own
    /// `ref_key` prefix uses its own `instance_id`), then all externalized
    /// payloads and all instance rows commit in a single transaction on
    /// production backends.
    async fn create_instances_batch_externalized(
        &self,
        instances: &[TaskInstance],
        threshold_bytes: u32,
    ) -> Result<u64, StorageError> {
        let mut clones: Vec<TaskInstance> = Vec::with_capacity(instances.len());
        for inst in instances {
            let mut c = inst.clone();
            let refs = crate::externalizing::externalize_fields(
                &mut c.context.data,
                &inst.id.0.to_string(),
                threshold_bytes,
            );
            if !refs.is_empty() {
                self.batch_save_externalized_state(inst.id, &refs).await?;
            }
            clones.push(c);
        }
        self.create_instances_batch(&clones).await
    }

    async fn get_instance(&self, id: InstanceId) -> Result<Option<TaskInstance>, StorageError>;

    /// Hot path. Uses `FOR UPDATE SKIP LOCKED` on Postgres.
    /// Returns instances with `next_fire_at <= now`, ordered by priority DESC, `next_fire_at` ASC.
    /// When `max_per_tenant > 0`, applies per-tenant fairness cap (noisy-neighbor protection).
    async fn claim_due_instances(
        &self,
        now: DateTime<Utc>,
        limit: u32,
        max_per_tenant: u32,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    async fn update_instance_state(
        &self,
        id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    /// Atomically update instance state only if the current state matches
    /// `expected_state`. Returns `true` if the row was updated, `false` if
    /// the state had already moved (concurrent writer won the race).
    ///
    /// Default implementation falls through to `update_instance_state`
    /// without the guard — production backends override with
    /// `WHERE id = $1 AND state = $expected`.
    async fn conditional_update_instance_state(
        &self,
        id: InstanceId,
        _expected_state: InstanceState,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<bool, StorageError> {
        self.update_instance_state(id, new_state, next_fire_at)
            .await?;
        Ok(true)
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError>;

    /// Update only the `runtime.started_at` field for an instance.
    /// Avoids the full context clone + deserialization that
    /// `update_instance_context` incurs when all we need is stamp the start
    /// time on the first run.
    async fn update_instance_started_at(
        &self,
        id: InstanceId,
        started_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        // Default impl for test/memory backends: fall back to the full-path.
        let mut inst = self
            .get_instance(id)
            .await?
            .ok_or_else(|| StorageError::Query(format!("instance not found: {id}")))?;
        inst.context.runtime.started_at = Some(started_at);
        self.update_instance_context(id, &inst.context).await
    }

    /// Persist `context` with top-level `data` fields >= `threshold_bytes`
    /// swapped for externalization markers. The payloads are written to
    /// `externalized_state` and the mutated context lands in
    /// `task_instances.context` **in the same transaction** so partial
    /// failure can't leave dangling markers.
    ///
    /// When `threshold_bytes == 0` this is equivalent to
    /// [`Self::update_instance_context`] — no field is ever large enough to
    /// externalize. The scheduler uses this hook to enforce
    /// [`crate::externalizing`] semantics under the configured
    /// `ExternalizationMode`.
    ///
    /// The default impl is non-atomic (save refs, then update context) to
    /// keep test/memory backends compiling. Production backends
    /// (Postgres/SQLite) override this with a single transaction.
    async fn update_instance_context_externalized(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        let mut ctx_clone = context.clone();
        let refs = crate::externalizing::externalize_fields(
            &mut ctx_clone.data,
            &id.0.to_string(),
            threshold_bytes,
        );
        if !refs.is_empty() {
            self.batch_save_externalized_state(id, &refs).await?;
        }
        self.update_instance_context(id, &ctx_clone).await
    }

    /// Hot migration: rebind an instance to a different sequence version.
    async fn update_instance_sequence(
        &self,
        id: InstanceId,
        new_sequence_id: SequenceId,
    ) -> Result<(), StorageError>;

    /// Merge a key-value pair into context->'data' (JSONB merge).
    async fn merge_context_data(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn list_instances(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    /// List instances currently in the Waiting state together with their execution trees.
    /// Only instances matching `filter.tenant_id` / `filter.namespace` are returned.
    /// Ignores `filter.states` — this method always filters to Waiting.
    async fn list_waiting_with_trees(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<(TaskInstance, Vec<ExecutionNode>)>, StorageError>;

    async fn count_instances(&self, filter: &InstanceFilter) -> Result<u64, StorageError>;

    async fn bulk_update_state(
        &self,
        filter: &InstanceFilter,
        new_state: InstanceState,
    ) -> Result<u64, StorageError>;

    /// Shift `next_fire_at` by `offset_secs` for scheduled instances matching the filter.
    async fn bulk_reschedule(
        &self,
        filter: &InstanceFilter,
        offset_secs: i64,
    ) -> Result<u64, StorageError>;

    // === Execution Tree ===

    async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError>;

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[ExecutionNode],
    ) -> Result<(), StorageError>;

    async fn get_execution_tree(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<ExecutionNode>, StorageError>;

    async fn update_node_state(
        &self,
        node_id: ExecutionNodeId,
        state: NodeState,
    ) -> Result<(), StorageError>;

    /// Batch-activate multiple nodes from Pending to Running in a single
    /// round-trip. Only nodes that are currently Pending are updated.
    async fn batch_activate_nodes(&self, node_ids: &[ExecutionNodeId]) -> Result<(), StorageError>;

    /// Batch transition a set of nodes to the same target state in a single
    /// round-trip. Timestamps (`started_at` / `completed_at`) are updated
    /// using the same rules as [`Self::update_node_state`].
    ///
    /// Callers: iteration-boundary reset loops in `Loop` / `ForEach` that
    /// previously issued one UPDATE per descendant. For a moderately nested
    /// subtree this cuts N round-trips to 1.
    async fn update_nodes_state(
        &self,
        node_ids: &[ExecutionNodeId],
        state: NodeState,
    ) -> Result<(), StorageError>;

    async fn get_children(
        &self,
        parent_id: ExecutionNodeId,
    ) -> Result<Vec<ExecutionNode>, StorageError>;

    /// Delete all execution tree nodes for an instance (used by retry to
    /// force the evaluator to rebuild the tree from scratch).
    async fn delete_execution_tree(&self, instance_id: InstanceId) -> Result<(), StorageError>;

    // === Block Outputs ===

    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError>;

    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<Option<BlockOutput>, StorageError>;

    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError>;

    /// Return block outputs created after the given timestamp.
    /// Used by SSE streaming to avoid fetching the entire history on every poll.
    /// When `after` is `None`, behaves like `get_all_outputs`.
    async fn get_outputs_after_created_at(
        &self,
        instance_id: InstanceId,
        after: Option<DateTime<Utc>>,
    ) -> Result<Vec<BlockOutput>, StorageError>;

    /// Return just the block IDs that have outputs for this instance.
    /// Lighter than `get_all_outputs` — avoids deserializing full output JSON.
    async fn get_completed_block_ids(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockId>, StorageError>;

    /// Batch variant: fetch completed block IDs for multiple instances in one query.
    async fn get_completed_block_ids_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<BlockId>>, StorageError>;

    /// Atomic: save block output + update instance state in a single transaction.
    /// Eliminates one DB round-trip on the hot path.
    async fn save_output_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    /// Atomic: save block output + overwrite instance context + update state
    /// in a single transaction.
    ///
    /// Closes the crash-safety gap in external-worker completion where the
    /// previous sequence (`update_instance_context` → `save_output_and_transition`)
    /// could leave an instance with merged context but no state transition
    /// if the server crashed between the two calls, or — in the reversed
    /// ordering — could let the scheduler advance on stale context.
    ///
    /// Every backend MUST implement this — no default impl so a missing
    /// implementation fails at compile time rather than silently falling
    /// back to the non-atomic path (same convention as
    /// [`Self::enqueue_signal_if_active`]).
    async fn save_output_merge_context_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    /// Delete every `block_outputs` row for `(instance_id, block_id)`.
    ///
    /// Returns the number of rows actually removed.
    ///
    /// Purpose: under the write-append model (migration 027) composite blocks
    /// (`Loop`, `ForEach`) persist their iteration counter as a `BlockOutput`
    /// marker keyed by their own `block_id`. When an outer iteration boundary
    /// resets a descendant subtree back to `Pending`, the descendant's
    /// previous-iteration marker must be purged too — otherwise the
    /// next-tick `get_block_output` would return the stale counter and the
    /// top-of-handler cap guard would immediately complete the descendant
    /// without ever running its body.
    ///
    /// Step outputs are intentionally keyed by the step's own `block_id`,
    /// so they are NOT affected when a sibling composite's marker is
    /// purged — callers should only invoke this method against composite
    /// markers whose semantics are "internal iteration state".
    ///
    /// Every backend MUST implement this — there is deliberately no default
    /// impl so a missing implementation fails at compile time rather than
    /// silently no-oping (same convention as
    /// [`Self::enqueue_signal_if_active`] and
    /// [`Self::record_or_get_emit_dedupe`]).
    async fn delete_block_outputs(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<u64, StorageError>;

    /// Batch variant of [`Self::delete_block_outputs`]. Issues a single
    /// `DELETE ... IN (...)` round-trip instead of N.
    ///
    /// Used by iteration-boundary reset in composites (`Loop` / `ForEach`) to
    /// purge every descendant composite marker in one shot.
    async fn delete_block_outputs_batch(
        &self,
        instance_id: InstanceId,
        block_ids: &[BlockId],
    ) -> Result<u64, StorageError>;

    /// Delete ALL `block_outputs` for an instance.
    async fn delete_all_block_outputs(&self, instance_id: InstanceId) -> Result<u64, StorageError>;

    /// Delete only sentinel `block_output` rows (`output_ref = '__in_progress__'`)
    /// for an instance. Used by DLQ retry to clear in-progress markers from
    /// permanently-failed steps while preserving real outputs from successfully
    /// completed steps (so they are skipped on retry, preventing double
    /// execution of side-effectful handlers like email sends).
    async fn delete_sentinel_block_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<u64, StorageError>;

    /// Delete a single `block_output` row by its primary key.
    ///
    /// Used to clean up sentinel rows after a real output has been saved:
    /// the sentinel prevents double-execution on crash recovery, but once
    /// the real output is persisted the sentinel must be removed so output
    /// counts stay correct.
    async fn delete_block_output_by_id(&self, id: Uuid) -> Result<(), StorageError>;

    // === Rate Limits ===

    /// Atomic check-and-increment. Single DB round-trip.
    async fn check_rate_limit(
        &self,
        tenant_id: &TenantId,
        resource_key: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<RateLimitCheck, StorageError>;

    async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError>;

    // === Signals ===

    async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError>;

    /// Atomically enqueue a signal, but only if the target instance exists and
    /// is NOT in a terminal state (Completed / Failed / Cancelled).
    ///
    /// Closes the TOCTOU window between a read-side terminal-state check and
    /// the INSERT into `signal_inbox`. Reads the target's state and inserts
    /// the signal row inside a single transaction, so no concurrent worker
    /// can transition the target in between.
    ///
    /// Errors:
    /// - [`StorageError::NotFound`] if the target instance does not exist.
    /// - [`StorageError::TerminalTarget`] if the target is in a terminal state
    ///   (Completed / Failed / Cancelled). This is a dedicated variant —
    ///   distinct from [`StorageError::Conflict`], which is reserved for
    ///   idempotency-key duplicates and constraint violations — so the handler
    ///   layer can map it to a `Permanent` "cannot send signal to terminal
    ///   instance" without overloading the generic conflict path.
    /// - [`StorageError::Query`] if the target's persisted state column holds
    ///   an unknown value. The backend MUST NOT silently coerce unknown states
    ///   to `Scheduled` (or any non-terminal) — a corrupted row should surface
    ///   as a hard error so operators notice.
    /// - Standard sqlx mappings for connection / serialization issues.
    ///
    /// Every backend MUST implement this — no default impl so a missing
    /// implementation fails at compile time instead of silently falling back
    /// to the non-atomic [`Self::enqueue_signal`] path (same rule as the R4
    /// dedupe methods).
    async fn enqueue_signal_if_active(&self, signal: &Signal) -> Result<(), StorageError>;

    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<Signal>, StorageError>;

    /// Batch variant: fetch pending signals for multiple instances in one query.
    async fn get_pending_signals_batch(
        &self,
        instance_ids: &[InstanceId],
    ) -> Result<std::collections::HashMap<InstanceId, Vec<Signal>>, StorageError>;

    async fn mark_signal_delivered(&self, signal_id: Uuid) -> Result<(), StorageError>;

    /// Batch variant: mark multiple signals delivered in one query.
    async fn mark_signals_delivered(&self, signal_ids: &[Uuid]) -> Result<(), StorageError>;

    /// Return `(instance_id, current_state)` pairs for instances in a
    /// non-scheduled state (`paused`, `waiting`) that have undelivered signals.
    ///
    /// The scheduler calls this on each tick so that resume/cancel signals
    /// queued against paused or waiting instances are processed promptly
    /// instead of waiting for the instance to be claimed via `claim_due_instances`.
    async fn get_signalled_instance_ids(
        &self,
        limit: u32,
    ) -> Result<Vec<(InstanceId, InstanceState)>, StorageError>;

    // === Idempotency ===

    /// Find an instance by tenant + idempotency key.
    async fn find_by_idempotency_key(
        &self,
        tenant_id: &TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError>;

    // === Concurrency ===

    /// Count running instances with the given concurrency key.
    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError>;

    /// Returns the 1-based position of an instance among running instances
    /// with the same concurrency key, ordered by ID.
    /// Used to deterministically pick which instances proceed vs. defer.
    async fn concurrency_position(
        &self,
        instance_id: InstanceId,
        concurrency_key: &str,
    ) -> Result<i64, StorageError>;

    // === Recovery ===

    /// Find all instances that were `Running` when the engine crashed
    /// and reset them to `Scheduled` for re-execution.
    async fn recover_stale_instances(&self, stale_threshold: Duration)
        -> Result<u64, StorageError>;

    // === Cron Schedules ===

    async fn create_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError>;

    async fn list_cron_schedules(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<CronSchedule>, StorageError>;

    async fn update_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError>;

    /// Fetch all enabled cron schedules whose `next_fire_at <= now`.
    async fn claim_due_cron_schedules(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<CronSchedule>, StorageError>;

    /// After triggering, update `last_triggered_at` and `next_fire_at`.
    async fn update_cron_fire_times(
        &self,
        id: Uuid,
        last_triggered_at: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    // === Worker Tasks ===

    async fn create_worker_task(&self, task: &WorkerTask) -> Result<(), StorageError>;

    async fn get_worker_task(&self, task_id: Uuid) -> Result<Option<WorkerTask>, StorageError>;

    /// Atomically claim up to `limit` pending tasks for a handler.
    /// Uses `FOR UPDATE SKIP LOCKED` for safe concurrent polling.
    async fn claim_worker_tasks(
        &self,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    /// Atomically claim up to `limit` pending tasks for a handler, scoped
    /// to a specific tenant.
    ///
    /// The tenant predicate must live INSIDE the claim's lock window.
    /// Filtering after a tenant-agnostic claim (what the HTTP handler used
    /// to do post-hoc) would mark a foreign tenant's row as claimed with
    /// this `worker_id` and then drop it from the response — the row then
    /// stays `claimed` with no active worker, heartbeat-reapable but
    /// invisible to everyone until reap. This entry point binds the two
    /// guarantees together so a tenant-scoped poll can never touch another
    /// tenant's row.
    async fn claim_worker_tasks_for_tenant(
        &self,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &TenantId,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    /// Mark a claimed task as completed with output. Verifies `worker_id` ownership.
    async fn complete_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        output: &serde_json::Value,
    ) -> Result<bool, StorageError>;

    /// Mark a claimed task as failed. Verifies `worker_id` ownership.
    async fn fail_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        message: &str,
        retryable: bool,
    ) -> Result<bool, StorageError>;

    /// Update heartbeat timestamp for a claimed task. Verifies `worker_id` ownership.
    async fn heartbeat_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
    ) -> Result<bool, StorageError>;

    /// Delete a worker task (used when retryable failure needs re-dispatch).
    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError>;

    /// Reset stale claimed tasks (no heartbeat within threshold) back to pending.
    async fn reap_stale_worker_tasks(&self, stale_threshold: Duration)
        -> Result<u64, StorageError>;

    /// Fail worker tasks whose `timeout_ms` has elapsed since `created_at`.
    /// Returns the number of tasks expired.
    async fn expire_timed_out_worker_tasks(&self) -> Result<u64, StorageError>;

    /// Delete pending/claimed worker tasks for an instance + block (used when race cancels a branch).
    async fn cancel_worker_tasks_for_block(
        &self,
        instance_id: Uuid,
        block_id: &str,
    ) -> Result<u64, StorageError>;

    /// Batch variant of [`Self::cancel_worker_tasks_for_block`]. Single
    /// round-trip for an arbitrary set of block IDs under one instance.
    ///
    /// Used at loop iteration boundaries to purge stale `worker_tasks` rows
    /// across every descendant block — without this the
    /// `UNIQUE(instance_id, block_id)` constraint would swallow the next
    /// iteration's dispatch via `ON CONFLICT DO NOTHING`.
    async fn cancel_worker_tasks_for_blocks(
        &self,
        instance_id: Uuid,
        block_ids: &[String],
    ) -> Result<u64, StorageError>;

    /// List worker tasks with optional filtering and pagination.
    async fn list_worker_tasks(
        &self,
        filter: &orch8_types::worker_filter::WorkerTaskFilter,
        pagination: &orch8_types::filter::Pagination,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    /// Aggregate worker task statistics: counts by state, by handler, and active workers.
    /// When `tenant_id` is provided, stats are scoped to tasks belonging to that tenant's instances.
    async fn worker_task_stats(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError>;

    // === Resource Pools ===

    async fn create_resource_pool(
        &self,
        pool: &orch8_types::pool::ResourcePool,
    ) -> Result<(), StorageError>;

    async fn get_resource_pool(
        &self,
        id: uuid::Uuid,
    ) -> Result<Option<orch8_types::pool::ResourcePool>, StorageError>;

    async fn list_resource_pools(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::pool::ResourcePool>, StorageError>;

    async fn update_pool_round_robin_index(
        &self,
        pool_id: uuid::Uuid,
        index: u32,
    ) -> Result<(), StorageError>;

    async fn delete_resource_pool(&self, id: uuid::Uuid) -> Result<(), StorageError>;

    async fn add_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError>;

    async fn list_pool_resources(
        &self,
        pool_id: uuid::Uuid,
    ) -> Result<Vec<orch8_types::pool::PoolResource>, StorageError>;

    async fn update_pool_resource(
        &self,
        resource: &orch8_types::pool::PoolResource,
    ) -> Result<(), StorageError>;

    async fn delete_pool_resource(&self, id: uuid::Uuid) -> Result<(), StorageError>;

    /// Atomically increment the daily usage counter for a resource.
    /// Resets the counter if the date has changed.
    async fn increment_resource_usage(
        &self,
        resource_id: uuid::Uuid,
        today: chrono::NaiveDate,
    ) -> Result<(), StorageError>;

    // === Checkpoints ===

    /// Save a checkpoint for an instance.
    async fn save_checkpoint(
        &self,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<(), StorageError>;

    /// Get the latest checkpoint for an instance.
    async fn get_latest_checkpoint(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<orch8_types::checkpoint::Checkpoint>, StorageError>;

    /// List all checkpoints for an instance.
    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError>;

    /// Delete old checkpoints, keeping only the latest N.
    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError>;

    // === Externalized State ===

    /// Store a large payload externally, returning the `ref_key`.
    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Persist multiple externalized payloads atomically in one call.
    ///
    /// The write-path counterpart of [`Self::batch_get_externalized_state`].
    /// Used when externalizing multiple context fields in the same scheduler
    /// tick so that a partial failure can't leave the `task_instances.context`
    /// markers pointing at ref-keys that don't exist.
    ///
    /// The default impl is a **non-atomic** sequential loop so test/memory
    /// backends compile — production callers must use a backend that
    /// overrides this with a real transaction (Postgres/SQLite below).
    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        for (ref_key, payload) in entries {
            self.save_externalized_state(instance_id, ref_key, payload)
                .await?;
        }
        Ok(())
    }

    /// Retrieve an externalized payload by `ref_key`.
    async fn get_externalized_state(
        &self,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError>;

    /// Retrieve multiple externalized payloads in one round-trip.
    ///
    /// Returns a map keyed by `ref_key`; absent entries mean the key did not
    /// exist in `externalized_state` (missing keys are **not** errors — the
    /// scheduler's preload path treats them as "nothing to hydrate").
    ///
    /// The default impl just loops over [`Self::get_externalized_state`] so
    /// less-hot backends (memory/test) compile without extra work. Production
    /// backends should override with a single batched query (e.g. `ANY($1)` on
    /// Postgres, `IN (?,?,...)` on `SQLite`) to amortize round-trip cost.
    async fn batch_get_externalized_state(
        &self,
        ref_keys: &[String],
    ) -> Result<HashMap<String, serde_json::Value>, StorageError> {
        let mut out = HashMap::with_capacity(ref_keys.len());
        for key in ref_keys {
            if let Some(v) = self.get_externalized_state(key).await? {
                out.insert(key.clone(), v);
            }
        }
        Ok(out)
    }

    /// Delete externalized state by `ref_key`.
    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError>;

    /// Delete up to `limit` `externalized_state` rows whose `expires_at` has
    /// elapsed. Returns the number of rows actually deleted.
    ///
    /// This is the GC sweeper's storage primitive (M4). Limiting per-sweep
    /// deletion prevents a single long transaction from blocking writers on a
    /// backlog of stale rows. The sweeper calls this on a fixed interval and
    /// logs the deletion count.
    ///
    /// Rows with `expires_at IS NULL` never expire and are never touched.
    /// The default impl returns `Ok(0)` so test/memory backends remain
    /// compilable without an implementation.
    async fn delete_expired_externalized_state(&self, _limit: u32) -> Result<u64, StorageError> {
        Ok(0)
    }

    // === Audit Log ===

    /// Append an audit log entry.
    async fn append_audit_log(&self, entry: &AuditLogEntry) -> Result<(), StorageError>;

    /// List audit log entries for an instance.
    async fn list_audit_log(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<AuditLogEntry>, StorageError>;

    /// List audit log entries for a tenant.
    async fn list_audit_log_by_tenant(
        &self,
        tenant_id: &TenantId,
        limit: u32,
    ) -> Result<Vec<AuditLogEntry>, StorageError>;

    // === Sessions ===

    async fn create_session(&self, session: &Session) -> Result<(), StorageError>;

    async fn get_session(&self, id: Uuid) -> Result<Option<Session>, StorageError>;

    async fn get_session_by_key(
        &self,
        tenant_id: &TenantId,
        session_key: &str,
    ) -> Result<Option<Session>, StorageError>;

    async fn update_session_data(
        &self,
        id: Uuid,
        data: &serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn update_session_state(
        &self,
        id: Uuid,
        state: orch8_types::session::SessionState,
    ) -> Result<(), StorageError>;

    /// Get all instances belonging to a session.
    async fn list_session_instances(
        &self,
        session_id: Uuid,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    // === Sub-Sequences ===

    /// Get child instances of a parent.
    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    // === Task Queue Routing ===

    /// Claim worker tasks from a specific named queue.
    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    /// Tenant-scoped variant of `claim_worker_tasks_from_queue`. See
    /// `claim_worker_tasks_for_tenant` for why this lives at the storage
    /// layer instead of being filtered post-claim in the HTTP handler.
    async fn claim_worker_tasks_from_queue_for_tenant(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &TenantId,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError>;

    // === Dynamic Step Injection ===

    /// Append blocks to a running instance's sequence (stored as instance-level overrides).
    async fn inject_blocks(
        &self,
        instance_id: InstanceId,
        blocks_json: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Atomically merge new blocks into an instance's existing injected-blocks
    /// array at the given position, inside a single transaction.
    ///
    /// If `position` is `None`, `new_blocks_json` replaces any prior value
    /// (equivalent to `inject_blocks`). If `position` is `Some(pos)`, the
    /// current injected blocks are read, `new_blocks_json`'s entries are
    /// inserted at `pos` (clamped to the current length), and the resulting
    /// array is written back — all within one transaction so two concurrent
    /// calls cannot lose each other's writes.
    ///
    /// Returns the final injected-blocks array actually persisted.
    async fn inject_blocks_at_position(
        &self,
        instance_id: InstanceId,
        new_blocks_json: &serde_json::Value,
        position: Option<usize>,
    ) -> Result<serde_json::Value, StorageError>;

    /// Get injected blocks for an instance.
    async fn get_injected_blocks(
        &self,
        instance_id: InstanceId,
    ) -> Result<Option<serde_json::Value>, StorageError>;

    // === Cluster ===

    /// Register a new cluster node.
    async fn register_node(
        &self,
        node: &orch8_types::cluster::ClusterNode,
    ) -> Result<(), StorageError>;

    /// Update heartbeat timestamp for a node.
    async fn heartbeat_node(&self, node_id: Uuid) -> Result<(), StorageError>;

    /// Set the drain flag on a node, triggering coordinated shutdown.
    async fn drain_node(&self, node_id: Uuid) -> Result<(), StorageError>;

    /// Mark a node as stopped.
    async fn deregister_node(&self, node_id: Uuid) -> Result<(), StorageError>;

    /// List all nodes (for admin dashboard / health check).
    async fn list_nodes(&self) -> Result<Vec<orch8_types::cluster::ClusterNode>, StorageError>;

    /// Check if this node should drain (returns true if `drain = true` in DB).
    async fn should_drain(&self, node_id: Uuid) -> Result<bool, StorageError>;

    /// Remove stale nodes that haven't heartbeated within the threshold.
    async fn reap_stale_nodes(
        &self,
        stale_threshold: std::time::Duration,
    ) -> Result<u64, StorageError>;

    // === Plugins ===

    async fn create_plugin(&self, plugin: &PluginDef) -> Result<(), StorageError>;

    async fn get_plugin(&self, name: &str) -> Result<Option<PluginDef>, StorageError>;

    async fn list_plugins(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<PluginDef>, StorageError>;

    async fn update_plugin(&self, plugin: &PluginDef) -> Result<(), StorageError>;

    async fn delete_plugin(&self, name: &str) -> Result<(), StorageError>;

    // === Triggers ===

    async fn create_trigger(&self, trigger: &TriggerDef) -> Result<(), StorageError>;

    async fn get_trigger(&self, slug: &str) -> Result<Option<TriggerDef>, StorageError>;

    async fn list_triggers(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<TriggerDef>, StorageError>;

    async fn update_trigger(&self, trigger: &TriggerDef) -> Result<(), StorageError>;

    async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError>;

    // === Credentials ===

    async fn create_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError>;

    async fn get_credential(
        &self,
        id: &str,
    ) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError>;

    async fn list_credentials(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError>;

    async fn update_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError>;

    async fn delete_credential(&self, id: &str) -> Result<(), StorageError>;

    /// List `OAuth2` credentials whose `expires_at` is within `threshold` of now
    /// (and that have a `refresh_url` + `refresh_token` configured). Used by
    /// the refresh loop — returns an empty vec if no credentials are due.
    async fn list_credentials_due_for_refresh(
        &self,
        threshold: std::time::Duration,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError>;

    // === Emit Event Dedupe ===

    /// Record a dedupe key for `emit_event`. If `(scope, key)` already exists,
    /// returns the previously-recorded `child_instance_id` without modifying state.
    ///
    /// `scope` selects the dedupe namespace (see [`DedupeScope`]):
    /// per-parent (retry idempotency) or per-tenant (tenant-wide at-most-once).
    ///
    /// Atomic per row. Every backend MUST implement this — there is deliberately
    /// no default impl so that a new backend cannot silently fall back to a
    /// broken stub at runtime (see architectural finding #8).
    ///
    /// Prefer [`StorageBackend::create_instance_with_dedupe`] in production code
    /// paths: this method only records the dedupe row, so a crash between the
    /// dedupe insert and the child `create_instance` would leave an orphan row.
    /// This primitive is retained for GC tests and tools that intentionally
    /// manipulate dedupe state without creating a child.
    async fn record_or_get_emit_dedupe(
        &self,
        scope: &DedupeScope,
        key: &str,
        candidate_child: orch8_types::ids::InstanceId,
    ) -> Result<EmitDedupeOutcome, StorageError>;

    /// Atomically record a dedupe row AND create the child `TaskInstance` in a
    /// single transaction. Closes the orphan window described in architectural
    /// finding #2: before this method existed, a crash between
    /// [`StorageBackend::record_or_get_emit_dedupe`] and
    /// [`StorageBackend::create_instance`] could leave a dedupe row pointing at
    /// a non-existent child.
    ///
    /// Semantics:
    /// - If `(scope, key)` is free: inserts the dedupe row AND the instance.
    ///   Returns `Inserted`; `instance.id` is now present in `task_instances`.
    /// - If `(scope, key)` already exists: returns `AlreadyExists(existing_id)`
    ///   without creating the instance. The caller must NOT persist `instance`.
    ///
    /// Every backend MUST implement this — no default impl (finding #8).
    async fn create_instance_with_dedupe(
        &self,
        scope: &DedupeScope,
        key: &str,
        instance: &TaskInstance,
    ) -> Result<EmitDedupeOutcome, StorageError>;

    /// Delete up to `limit` `emit_event_dedupe` rows whose `created_at` is older
    /// than `older_than`. Returns the number of rows actually deleted.
    ///
    /// This is the GC sweeper's storage primitive for dedupe TTL (default 30d).
    /// Dedupe rows are idempotency records — once the configured TTL has
    /// elapsed a retry of the same `(parent, key)` can safely create a fresh
    /// child, because callers should not depend on dedupe beyond the window.
    ///
    /// Limit is bounded per call so a large backlog doesn't starve writers in
    /// a single long transaction — same convention as
    /// [`StorageBackend::delete_expired_externalized_state`].
    ///
    /// Every backend MUST implement this — there is deliberately no default
    /// impl so a missing implementation fails at compile time rather than
    /// silently returning `Ok(0)` (see architectural finding #8).
    async fn delete_expired_emit_event_dedupe(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<u64, StorageError>;

    // === Circuit Breakers ===
    //
    // Only `Open` rows are persisted — this is a correctness backstop so that
    // a crash mid-cooldown does not reset every tripped breaker. Default
    // impls are no-ops so decorator backends (encrypting, externalizing) and
    // future backends can opt in without forcing every crate to edit.

    /// Upsert an `Open` circuit breaker row. Keyed by `(tenant_id, handler)`.
    async fn upsert_circuit_breaker(
        &self,
        _state: &CircuitBreakerState,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    /// Return every persisted `Open` row across all tenants. Used at boot to
    /// rehydrate the in-memory registry.
    async fn list_open_circuit_breakers(&self) -> Result<Vec<CircuitBreakerState>, StorageError> {
        Ok(Vec::new())
    }

    /// Delete any persisted circuit breaker row for `(tenant_id, handler)`.
    /// No-op if no row exists.
    async fn delete_circuit_breaker(
        &self,
        _tenant_id: &TenantId,
        _handler: &str,
    ) -> Result<(), StorageError> {
        Ok(())
    }

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError>;
}
