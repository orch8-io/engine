pub mod api_key_cache;
pub mod artifacts;
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
use orch8_types::continuity::{
    CapsuleId, CapsuleManifest, ContinuationGrant, ContinuationGrantId, ContinuationGrantState,
    ContinuityExecution, ContinuityId, ContinuityStream, EffectId, EffectReceipt, EffectState,
    ExecutionEpoch, ExecutionHandoff, HandoffId, HandoffState, PlacementDecision,
    PlacementDecisionId, ProvenanceEntry, RuntimeCapabilities, RuntimeId, StreamFrame, StreamId,
};
use orch8_types::continuity_advanced::{
    AttentionTask, AttentionTaskId, BudgetReservation, EvaluationScore, FederationEnvelope,
    InvariantResult, WorkflowInvariant,
};
use orch8_types::cron::CronSchedule;
pub use orch8_types::dedupe::DedupeScope;
use orch8_types::dlq::DlqIncidentReproduction;
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
use orch8_types::trigger::{TriggerDef, TriggerPollState};
use orch8_types::worker::WorkerTask;

/// Represents a single telemetry event for batch ingestion.
#[derive(Debug, Clone)]
pub struct TelemetryEvent {
    pub event_type: String,
    pub payload: String,
    pub device_id: String,
    pub os_name: String,
    pub os_version: String,
    pub app_version: String,
    pub sdk_version: String,
    pub tenant_id: String,
    pub created_at: DateTime<Utc>,
}

/// A single usage/billing event — e.g. LLM token consumption emitted by
/// `llm_call`/`agent`. Captured in a structured form so a control plane can
/// aggregate cost without scanning every block output.
#[derive(Debug, Clone)]
pub struct UsageEvent {
    pub tenant_id: String,
    pub instance_id: Option<InstanceId>,
    pub block_id: Option<String>,
    /// Usage category, e.g. `"llm_tokens"`.
    pub kind: String,
    /// Model identifier the usage is attributed to (empty if unknown).
    pub model: String,
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub created_at: DateTime<Utc>,
}

/// Usage totals for a tenant over a window, grouped by `(kind, model)`.
#[derive(Debug, Clone, serde::Serialize)]
pub struct UsageAggregate {
    pub kind: String,
    pub model: String,
    pub events: i64,
    pub input_tokens: i64,
    pub output_tokens: i64,
}

/// Outcome of a dedupe insert attempt for `emit_event`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EmitDedupeOutcome {
    /// First call for (parent, key) -- caller proceeds with `candidate_child`.
    Inserted,
    /// Key already used; caller should reuse the existing child instance ID.
    AlreadyExists(orch8_types::ids::InstanceId),
}

// ============================================================================
// Sub-trait 1: SequenceStore
// ============================================================================

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait SequenceStore: Send + Sync + 'static {
    async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError>;

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError>;

    /// Fetch several sequences in one storage round-trip. Missing IDs are
    /// omitted from the result so callers can apply their own error policy.
    async fn get_sequences(
        &self,
        ids: &[SequenceId],
    ) -> Result<Vec<SequenceDefinition>, StorageError>;

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

    /// Update the lifecycle status of a sequence.
    async fn update_sequence_status(
        &self,
        id: SequenceId,
        status: &str,
    ) -> Result<(), StorageError> {
        let _ = (id, status);
        Ok(())
    }

    /// Delete a sequence by ID.
    async fn delete_sequence(&self, id: SequenceId) -> Result<(), StorageError>;

    /// Atomically replace a sequence: delete `old_id` and insert `new` as a
    /// single unit. Used when upserting a same-name sequence with a new
    /// definition (e.g. mobile sync) so a mid-operation failure can't leave
    /// the old row deleted with the new one never created.
    ///
    /// The default impl is a **non-atomic** delete-then-create for backends
    /// that don't override it -- overriding backends must wrap both
    /// operations in a single transaction (see `sqlite::sequences::replace`).
    async fn replace_sequence(
        &self,
        old_id: SequenceId,
        new: &SequenceDefinition,
    ) -> Result<(), StorageError> {
        self.delete_sequence(old_id).await?;
        self.create_sequence(new).await
    }

    // === Workflow releases (release control plane) ===

    /// Persist a new release (state `draft`).
    async fn create_release(
        &self,
        release: &orch8_types::release::WorkflowRelease,
    ) -> Result<(), StorageError>;

    async fn get_release(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError>;

    /// List releases, optionally filtered by tenant, newest first.
    async fn list_releases(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::release::WorkflowRelease>, StorageError>;

    /// Compare-and-swap state transition. Returns `false` when the
    /// release was not in `expected` (a concurrent writer won) — the
    /// caller must NOT treat that as success. Optionally updates
    /// `canary_percent` and `canary_started_at` in the same write.
    async fn cas_release_state(
        &self,
        id: Uuid,
        expected: orch8_types::release::ReleaseState,
        next: orch8_types::release::ReleaseState,
        canary_percent: Option<u8>,
        canary_started_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<bool, StorageError>;

    /// Store the historical-validation summary on the release.
    async fn set_release_validation_summary(
        &self,
        id: Uuid,
        summary: &serde_json::Value,
    ) -> Result<(), StorageError>;

    /// Append one immutable audit decision.
    async fn record_release_decision(
        &self,
        decision: &orch8_types::release::ReleaseDecision,
    ) -> Result<(), StorageError>;

    /// All decisions for a release, oldest first.
    async fn list_release_decisions(
        &self,
        release_id: Uuid,
    ) -> Result<Vec<orch8_types::release::ReleaseDecision>, StorageError>;

    /// The release currently routing traffic for a baseline sequence
    /// (state `canary` or `promoted`), if any.
    async fn find_routing_release_for_sequence(
        &self,
        baseline_sequence_id: SequenceId,
    ) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError>;

    // === Manifest advisory lock (Postgres only) ===

    /// Acquire a per-tenant advisory lock for the duration of a manifest
    /// publish, returning an RAII guard that releases it on drop.
    ///
    /// Release is intentionally not a separate caller-invoked method: the
    /// caller previously had to remember to call a matching
    /// `release_manifest_lock`, and any early return (including a panic)
    /// between acquire and that call stranded the lock/connection. The
    /// guard makes release unconditional -- it runs whether the critical
    /// section returns `Ok`, `Err`, or unwinds.
    ///
    /// Backends with nothing to release (in-memory, the default impl here)
    /// return a no-op guard. `SQLite` has a real lock to release (a row in
    /// `manifest_locks`) and returns a guard that deletes it on drop.
    async fn acquire_manifest_lock(
        &self,
        _tenant_id: &str,
    ) -> Result<ManifestLockGuard, StorageError> {
        Ok(ManifestLockGuard::noop())
    }
}

/// RAII guard returned by [`SequenceStore::acquire_manifest_lock`]. Dropping
/// it runs the backend's release logic (if any) exactly once.
///
/// Holds an optional `FnOnce` rather than a backend-specific type so every
/// implementor of the object-safe [`StorageBackend`] trait can return the
/// same concrete type regardless of what (if anything) it needs to clean up.
pub struct ManifestLockGuard {
    on_drop: Option<Box<dyn FnOnce() + Send>>,
}

impl ManifestLockGuard {
    /// A guard with nothing to release on drop.
    #[must_use]
    pub const fn noop() -> Self {
        Self { on_drop: None }
    }

    /// A guard that runs `f` exactly once, on drop.
    #[must_use]
    pub fn new(f: impl FnOnce() + Send + 'static) -> Self {
        Self {
            on_drop: Some(Box::new(f)),
        }
    }
}

impl Drop for ManifestLockGuard {
    fn drop(&mut self) {
        if let Some(f) = self.on_drop.take() {
            f();
        }
    }
}

// ============================================================================
// Sub-trait 2: InstanceStore
// ============================================================================

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait InstanceStore: Send + Sync + 'static {
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
            &instance.id.into_uuid().to_string(),
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
                &inst.id.into_uuid().to_string(),
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

    /// True when runnable scheduled work exists above `priority`. Used for
    /// cooperative preemption at durable step boundaries.
    async fn has_due_higher_priority(
        &self,
        now: DateTime<Utc>,
        priority: orch8_types::instance::Priority,
    ) -> Result<bool, StorageError>;

    async fn update_instance_state(
        &self,
        id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    async fn batch_reschedule_instances(
        &self,
        ids: &[InstanceId],
        fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        for &id in ids {
            self.update_instance_state(id, InstanceState::Scheduled, Some(fire_at))
                .await?;
        }
        Ok(())
    }

    /// Atomically update instance state only if the current state matches
    /// `expected_state`. Returns `true` if the row was updated, `false` if
    /// the state had already moved (concurrent writer won the race).
    ///
    /// Default implementation falls through to `update_instance_state`
    /// without the guard -- production backends override with
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

    /// CAS the instance state AND enqueue webhook outbox rows in one
    /// transaction, so a terminal transition can never commit without its
    /// webhook events being durably queued (a crash between the two writes
    /// would otherwise lose the event). Returns whether the CAS won.
    ///
    /// Required method (it spans `task_instances` and `webhook_outbox`, so no
    /// single-trait default exists); the encrypting wrapper passes through to
    /// its inner backend.
    async fn conditional_update_instance_state_with_outbox(
        &self,
        id: InstanceId,
        expected_state: InstanceState,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
        entries: &[orch8_types::webhook_outbox::WebhookOutboxEntry],
    ) -> Result<bool, StorageError>;

    /// Reset the per-run bookkeeping in `context.runtime` for a NEW run:
    /// regenerate `run_id`, zero `total_steps_executed`, and clear
    /// `current_step` / `current_step_started_at` / `started_at` (the start
    /// timestamp is re-stamped on the next claim). Called by retry /
    /// resume-from-block / DLQ-retry so a resurrected instance does not
    /// phantom-re-fail against `max_steps_per_instance` and its webhooks
    /// carry a fresh run identity.
    ///
    /// The default is a read-modify-write fallback for in-memory / test
    /// backends; SQL backends override with a targeted `json_set` /
    /// `jsonb_set` UPDATE (same style as [`Self::increment_total_steps`]).
    async fn reset_instance_run(&self, id: InstanceId, run_id: &str) -> Result<(), StorageError> {
        let Some(mut inst) = self.get_instance(id).await? else {
            return Ok(());
        };
        inst.context.runtime.run_id = Some(run_id.to_string());
        inst.context.runtime.total_steps_executed = 0;
        inst.context.runtime.current_step = None;
        inst.context.runtime.current_step_started_at = None;
        inst.context.runtime.started_at = None;
        self.update_instance_context(id, &inst.context).await
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError>;

    /// CAS variant: update context only if `updated_at` still matches the
    /// expected timestamp. Returns `true` if the write landed, `false` if
    /// contention was detected (caller should re-read and retry).
    async fn update_instance_context_cas(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        _expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        self.update_instance_context(id, context).await?;
        Ok(true)
    }

    /// Ensure `runtime.run_id` and `runtime.started_at` are set, keeping
    /// whichever value is already stored for each (first writer wins per
    /// field). Stamps a fresh run identity on the first execution of a run
    /// without clobbering the `run_id` a retry/resume already regenerated,
    /// and avoids the full context clone + deserialization that
    /// `update_instance_context` incurs.
    async fn ensure_instance_run_started(
        &self,
        id: InstanceId,
        run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        // Default impl for test/memory backends: fall back to the full-path.
        let mut inst = self
            .get_instance(id)
            .await?
            .ok_or_else(|| StorageError::Query(format!("instance not found: {id}")))?;
        inst.context
            .runtime
            .run_id
            .get_or_insert_with(|| run_id.to_string());
        inst.context.runtime.started_at.get_or_insert(started_at);
        self.update_instance_context(id, &inst.context).await
    }

    /// Update only the `runtime.current_step_started_at` field for an instance.
    /// Used to record when the current step began so per-step deadlines and
    /// `wait_for_input` timeouts are measured from step start rather than
    /// workflow start.
    async fn update_instance_current_step_started_at(
        &self,
        id: InstanceId,
        ts: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        let mut inst = self
            .get_instance(id)
            .await?
            .ok_or_else(|| StorageError::Query(format!("instance not found: {id}")))?;
        inst.context.runtime.current_step_started_at = Some(ts);
        self.update_instance_context(id, &inst.context).await
    }

    /// Atomically increment `context.runtime.total_steps_executed` and return
    /// the new value.
    ///
    /// Avoids the read + full-context rewrite the scheduler otherwise performs
    /// per completed step, and — critically — touches only the counter path, so
    /// concurrent `context.data` mutations made *during* step execution (e.g.
    /// `merge_context_data`) are not clobbered. Returns `0` when the instance no
    /// longer exists. The default is a read-modify-write fallback for in-memory
    /// / test backends; SQL backends override it with a single targeted
    /// `json_set` / `jsonb_set` UPDATE.
    async fn increment_total_steps(&self, id: InstanceId) -> Result<u32, StorageError> {
        let Some(mut inst) = self.get_instance(id).await? else {
            return Ok(0);
        };
        inst.context.runtime.total_steps_executed =
            inst.context.runtime.total_steps_executed.saturating_add(1);
        let new_total = inst.context.runtime.total_steps_executed;
        self.update_instance_context(id, &inst.context).await?;
        Ok(new_total)
    }

    /// Persist `context` with top-level `data` fields >= `threshold_bytes`
    /// swapped for externalization markers. The payloads are written to
    /// `externalized_state` and the mutated context lands in
    /// `task_instances.context` **in the same transaction** so partial
    /// failure can't leave dangling markers.
    ///
    /// When `threshold_bytes == 0` this is equivalent to
    /// [`Self::update_instance_context`] -- no field is ever large enough to
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
            &id.to_string(),
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

    /// Shallow-merge `patch` into the instance's `metadata` JSON object.
    ///
    /// Existing keys not present in `patch` are preserved; keys present in
    /// `patch` overwrite. Used by the scheduler's budget enforcement to record
    /// `paused_reason` / `budget_breach` without clobbering caller-supplied
    /// metadata.
    ///
    /// Every backend MUST implement this -- there is deliberately no default
    /// impl so a missing implementation fails at compile time rather than
    /// silently dropping the patch (same convention as
    /// [`Self::record_or_get_emit_dedupe`]).
    async fn merge_instance_metadata(
        &self,
        id: InstanceId,
        patch: &serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn list_instances(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError>;

    /// List instances currently in the Waiting state together with their execution trees.
    /// Only instances matching `filter.tenant_id` / `filter.namespace` are returned.
    /// Ignores `filter.states` -- this method always filters to Waiting.
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

    // === Idempotency ===

    /// Find an instance by tenant + idempotency key.
    async fn find_by_idempotency_key(
        &self,
        tenant_id: &TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError>;

    // === Concurrency ===

    /// Count running instances with the given concurrency key.
    ///
    /// Default delegates to [] with a single key.
    async fn count_running_by_concurrency_key(
        &self,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        let mut map = self
            .count_running_by_concurrency_keys(&[concurrency_key])
            .await?;
        Ok(map.remove(concurrency_key).unwrap_or(0))
    }

    /// Batch count running instances for multiple concurrency keys.
    /// Returns a map from key to count.
    async fn count_running_by_concurrency_keys(
        &self,
        concurrency_keys: &[&str],
    ) -> Result<HashMap<String, i64>, StorageError>;

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
    ///
    /// `Waiting` instances are deliberately out of scope: they never
    /// heartbeat, so a lease-age sweep recycles every parked gate on each
    /// pass. Their liveness is owned by the deadline/timeout sweep.
    async fn recover_stale_instances(&self, stale_threshold: Duration)
    -> Result<u64, StorageError>;

    /// Renew a `Running`/`Waiting` instance's lease by touching `updated_at`.
    ///
    /// Called periodically by the scheduler while a step is genuinely
    /// in-flight (C-1): without it, [`InstanceStore::recover_stale_instances`]
    /// cannot distinguish a step that is still legitimately executing from
    /// one whose worker died, and would reclaim (and re-dispatch) a slow but
    /// healthy step out from under itself once it outlives the stale
    /// threshold. A no-op if the instance has already left `Running`/
    /// `Waiting` (e.g. it just completed) — this is a best-effort lease
    /// renewal, not a state transition.
    async fn heartbeat_instance(&self, instance_id: InstanceId) -> Result<(), StorageError>;

    // === Sub-Sequences ===

    /// Get child instances of a parent.
    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError>;

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
    /// array is written back -- all within one transaction so two concurrent
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

    // === Emit Event Dedupe ===

    /// Record a dedupe key for `emit_event`. If `(scope, key)` already exists,
    /// returns the previously-recorded `child_instance_id` without modifying state.
    ///
    /// `scope` selects the dedupe namespace (see [`DedupeScope`]):
    /// per-parent (retry idempotency) or per-tenant (tenant-wide at-most-once).
    ///
    /// Atomic per row. Every backend MUST implement this -- there is deliberately
    /// no default impl so that a new backend cannot silently fall back to a
    /// broken stub at runtime (see architectural finding #8).
    ///
    /// Prefer [`InstanceStore::create_instance_with_dedupe`] in production code
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
    /// [`InstanceStore::record_or_get_emit_dedupe`] and
    /// [`InstanceStore::create_instance`] could leave a dedupe row pointing at
    /// a non-existent child.
    ///
    /// Semantics:
    /// - If `(scope, key)` is free: inserts the dedupe row AND the instance.
    ///   Returns `Inserted`; `instance.id` is now present in `task_instances`.
    /// - If `(scope, key)` already exists: returns `AlreadyExists(existing_id)`
    ///   without creating the instance. The caller must NOT persist `instance`.
    ///
    /// Every backend MUST implement this -- no default impl (finding #8).
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
    /// Dedupe rows are idempotency records -- once the configured TTL has
    /// elapsed a retry of the same `(parent, key)` can safely create a fresh
    /// child, because callers should not depend on dedupe beyond the window.
    ///
    /// Limit is bounded per call so a large backlog doesn't starve writers in
    /// a single long transaction -- same convention as
    /// [`ResourceStore::delete_expired_externalized_state`].
    ///
    /// Every backend MUST implement this -- there is deliberately no default
    /// impl so a missing implementation fails at compile time rather than
    /// silently returning `Ok(0)` (see architectural finding #8).
    async fn delete_expired_emit_event_dedupe(
        &self,
        older_than: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<u64, StorageError>;

    // The following methods are needed by the default impls above.
    // They are defined in ResourceStore but must be available here for the
    // default externalization logic. We use a helper method that sub-trait
    // impls provide.

    /// Save multiple externalized state entries. Required for default
    /// `create_instance_externalized` / `update_instance_context_externalized`.
    /// Implementors that also implement `ResourceStore` should delegate to
    /// the same underlying implementation.
    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError>;
}

// ============================================================================
// Sub-trait 3: ExecutionTreeStore
// ============================================================================

#[async_trait]
pub trait ExecutionTreeStore: Send + Sync + 'static {
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
}

// ============================================================================
// Sub-trait 4: OutputStore
// ============================================================================

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait OutputStore: Send + Sync + 'static {
    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError>;

    async fn get_block_output(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
    ) -> Result<Option<BlockOutput>, StorageError>;

    /// Batch-fetch the most recent `BlockOutput` for multiple
    /// `(instance_id, block_id)` pairs.
    ///
    /// Returns a map from `(InstanceId, BlockId)` to the latest output
    /// (ordered by `created_at DESC`). Missing pairs are omitted.
    ///
    /// Performance Note: `keys` accepts `&BlockId` by reference inside the tuple
    /// to avoid expensive allocations/clones in hot paths like the scheduler loop.
    async fn get_block_outputs_batch(
        &self,
        keys: &[(InstanceId, &BlockId)],
    ) -> Result<HashMap<(InstanceId, BlockId), BlockOutput>, StorageError>;

    async fn get_all_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError>;

    /// Return block outputs created at or after the given timestamp
    /// (inclusive). Used by SSE streaming to avoid fetching the entire
    /// history on every poll. The bound is inclusive so outputs sharing the
    /// cursor's exact timestamp (same-millisecond batches) are never skipped;
    /// callers deduplicate boundary rows by output id.
    /// When `after` is `None`, behaves like `get_all_outputs`.
    async fn get_outputs_after_created_at(
        &self,
        instance_id: InstanceId,
        after: Option<DateTime<Utc>>,
    ) -> Result<Vec<BlockOutput>, StorageError>;

    /// Return just the block IDs that have outputs for this instance.
    /// Lighter than `get_all_outputs` -- avoids deserializing full output JSON.
    ///
    /// Internal bookkeeping rows are NOT completion evidence and are excluded:
    /// `__retry__` markers (attempt-counter bookkeeping after a retryable
    /// failure) and `__in_progress__` sentinels (crash mid-handler — the
    /// effect is unknown, so the block must stay eligible for re-execution).
    /// "Completed" must imply the handler ran to completion.
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
    /// previous sequence (`update_instance_context` -> `save_output_and_transition`)
    /// could leave an instance with merged context but no state transition
    /// if the server crashed between the two calls, or -- in the reversed
    /// ordering -- could let the scheduler advance on stale context.
    ///
    /// Every backend MUST implement this -- no default impl so a missing
    /// implementation fails at compile time rather than silently falling
    /// back to the non-atomic path (same convention as
    /// [`SignalStore::enqueue_signal_if_active`]).
    async fn save_output_merge_context_and_transition(
        &self,
        output: &BlockOutput,
        instance_id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    /// Atomic: save block output + mark execution node Completed + update
    /// instance state in a single transaction.
    ///
    /// Closes the race where the scheduler claims an instance between
    /// `save_output_and_transition` and `update_node_state`, observes the
    /// node still Waiting, and parks the instance back to Waiting.
    ///
    /// The instance UPDATE is guarded by a CAS: it only succeeds if the
    /// current state is NOT terminal or paused. If the instance became
    /// terminal/paused between the caller's read and this write, a
    /// [`StorageError::TerminalTarget`] is returned.
    async fn save_output_complete_node_and_transition(
        &self,
        output: &BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
        instance_id: InstanceId,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    /// Atomic: save block output + mark execution node Completed + merge
    /// context + update instance state in a single transaction.
    ///
    /// Combines the crash-safety of `save_output_merge_context_and_transition`
    /// with the node-state atomicity of `save_output_complete_node_and_transition`.
    ///
    /// The instance UPDATE is guarded by a CAS (see
    /// [`Self::save_output_complete_node_and_transition`]).
    async fn save_output_complete_node_merge_context_and_transition(
        &self,
        output: &BlockOutput,
        node_id: orch8_types::ids::ExecutionNodeId,
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
    /// previous-iteration marker must be purged too -- otherwise the
    /// next-tick `get_block_output` would return the stale counter and the
    /// top-of-handler cap guard would immediately complete the descendant
    /// without ever running its body.
    ///
    /// Step outputs are intentionally keyed by the step's own `block_id`,
    /// so they are NOT affected when a sibling composite's marker is
    /// purged -- callers should only invoke this method against composite
    /// markers whose semantics are "internal iteration state".
    ///
    /// Every backend MUST implement this -- there is deliberately no default
    /// impl so a missing implementation fails at compile time rather than
    /// silently no-oping (same convention as
    /// [`SignalStore::enqueue_signal_if_active`] and
    /// [`InstanceStore::record_or_get_emit_dedupe`]).
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

    /// Delete only sentinel `block_output` rows (`output_ref` of
    /// `'__in_progress__'` — a crash mid-step — or `'__error__'` — a
    /// permanent failure) for an instance. Used by DLQ retry to clear
    /// never-finished markers from failed steps so they re-execute, while
    /// preserving real outputs from successfully completed steps (so they
    /// are skipped on retry, preventing double execution of side-effectful
    /// handlers like email sends).
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

    /// Return one page of `block_outputs` rows for an instance in execution
    /// order (`created_at ASC`, `id ASC` tiebreak).
    ///
    /// Backs `GET /instances/{id}/timeline`: unlike [`Self::get_all_outputs`]
    /// the result is bounded, so a long-running instance with thousands of
    /// loop-iteration rows cannot blow up a single response.
    async fn get_outputs_page(
        &self,
        instance_id: InstanceId,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<BlockOutput>, StorageError>;

    /// Copy `block_outputs` rows for the given block IDs from `src` to `dst`,
    /// inserting new rows (fresh primary keys, `instance_id = dst`) that
    /// preserve `block_id`, `output`, `output_size`, `attempt` and
    /// `created_at`. Returns the number of rows copied.
    ///
    /// Only **inline** rows (`output_ref IS NULL`) are copied. Rows with a
    /// non-null `output_ref` — externalized payload references (which are
    /// keyed by the *source* instance ID and ownership-checked on read) and
    /// internal sentinels (`__in_progress__` / `__retry__` / `__error__`) —
    /// are deliberately skipped: a copied reference would dangle or leak
    /// across instances. Callers (fork-from) must put blocks whose outputs
    /// were not copied into the re-run set instead.
    ///
    /// Every backend MUST implement this — no default impl so a missing
    /// implementation fails at compile time rather than silently no-oping
    /// (same convention as [`Self::delete_block_outputs`]).
    async fn copy_block_outputs(
        &self,
        src: InstanceId,
        dst: InstanceId,
        block_ids: &[BlockId],
    ) -> Result<u64, StorageError>;
}

// ============================================================================
// Sub-trait 5: SignalStore
// ============================================================================

#[async_trait]
pub trait SignalStore: Send + Sync + 'static {
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
    ///   (Completed / Failed / Cancelled). This is a dedicated variant --
    ///   distinct from [`StorageError::Conflict`], which is reserved for
    ///   idempotency-key duplicates and constraint violations -- so the handler
    ///   layer can map it to a `Permanent` "cannot send signal to terminal
    ///   instance" without overloading the generic conflict path.
    /// - [`StorageError::Query`] if the target's persisted state column holds
    ///   an unknown value. The backend MUST NOT silently coerce unknown states
    ///   to `Scheduled` (or any non-terminal) -- a corrupted row should surface
    ///   as a hard error so operators notice.
    /// - Standard sqlx mappings for connection / serialization issues.
    ///
    /// Every backend MUST implement this -- no default impl so a missing
    /// implementation fails at compile time instead of silently falling back
    /// to the non-atomic [`Self::enqueue_signal`] path (same rule as the R4
    /// dedupe methods).
    async fn enqueue_signal_if_active(&self, signal: &Signal) -> Result<(), StorageError>;

    async fn get_pending_signals(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<Signal>, StorageError>;

    // === Durable event correlation ===

    /// Insert an event. Returns `false` (and changes nothing) when the
    /// `(tenant, event_name, producer_event_id)` identity already exists.
    async fn ingest_event(
        &self,
        envelope: &orch8_types::event_correlation::EventEnvelope,
    ) -> Result<bool, StorageError>;

    async fn get_event(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::event_correlation::EventEnvelope>, StorageError>;

    /// List events for a tenant, newest first, optionally by status.
    async fn list_events(
        &self,
        tenant_id: &str,
        status: Option<orch8_types::event_correlation::EventStatus>,
        limit: u32,
    ) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError>;

    /// Pending events matching `(tenant, name ∈ names, correlation_key)`,
    /// oldest first.
    async fn find_pending_events(
        &self,
        tenant_id: &str,
        event_names: &[String],
        correlation_key: &str,
    ) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError>;

    /// Consume events for `instance_id`: at-most-once via a conditional
    /// update from `pending`. Returns how many were actually consumed —
    /// callers must treat fewer-than-requested as a lost race.
    async fn consume_events(
        &self,
        event_ids: &[Uuid],
        instance_id: InstanceId,
    ) -> Result<u64, StorageError>;

    /// Create-or-replace the wait registered for `(instance, block)`.
    async fn upsert_event_wait(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
    ) -> Result<(), StorageError>;

    async fn get_event_wait(
        &self,
        instance_id: InstanceId,
        block_id: &str,
    ) -> Result<Option<orch8_types::event_correlation::EventWait>, StorageError>;

    /// Waiting registrations that listen for `event_name` under
    /// `(tenant, correlation_key)`, oldest first.
    async fn find_waiting_event_waits(
        &self,
        tenant_id: &str,
        event_name: &str,
        correlation_key: &str,
    ) -> Result<Vec<orch8_types::event_correlation::EventWait>, StorageError>;

    /// Persist an updated wait iff its stored status is still
    /// `expected_status` (CAS). Returns `false` when a concurrent writer
    /// won.
    async fn update_event_wait(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
        expected_status: orch8_types::event_correlation::WaitStatus,
    ) -> Result<bool, StorageError>;

    /// Retention: mark pending events received before `cutoff` as
    /// expired. Returns rows affected.
    async fn expire_events_before(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, StorageError>;

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
}

// ============================================================================
// Sub-trait 6: WorkerStore
// ============================================================================

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait WorkerStore: Send + Sync + 'static {
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
    /// this `worker_id` and then drop it from the response -- the row then
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

    /// Atomically persist resumable activity progress and heartbeat the lease.
    /// Returns the next checkpoint sequence, or `None` when ownership/state or
    /// the caller's expected sequence no longer matches.
    async fn checkpoint_worker_task(
        &self,
        task_id: Uuid,
        worker_id: &str,
        expected_seq: u64,
        checkpoint: &serde_json::Value,
    ) -> Result<Option<u64>, StorageError>;

    /// Delete a worker task (used when retryable failure needs re-dispatch).
    async fn delete_worker_task(&self, task_id: Uuid) -> Result<(), StorageError>;

    /// Atomically replace a failed worker task with a retry: delete the old
    /// task, insert the new one, reset the execution node to Pending, and
    /// reschedule the instance -- all in a single transaction.
    ///
    /// Default impl is non-atomic (sequential calls); production backends
    /// should override with a real transaction.
    async fn retry_worker_task(
        &self,
        old_task_id: Uuid,
        new_task: &WorkerTask,
        node_id: Option<orch8_types::ids::ExecutionNodeId>,
        instance_id: InstanceId,
        fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

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
    /// across every descendant block -- without this the
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

    // === Worker Registry ===

    /// Upsert a worker registration (one row per `worker_id` +
    /// `handler_name`), refreshing `last_seen_at`. Called on every poll, so
    /// implementations must keep this a single cheap statement.
    async fn upsert_worker_registration(
        &self,
        registration: &orch8_types::worker::WorkerRegistration,
    ) -> Result<(), StorageError>;

    /// List worker registrations seen within the last `seen_within_secs`
    /// seconds (all rows when `None`), most recently seen first.
    async fn list_worker_registrations(
        &self,
        seen_within_secs: Option<i64>,
    ) -> Result<Vec<orch8_types::worker::WorkerRegistration>, StorageError>;

    /// Count currently-claimed worker tasks grouped by claiming worker id.
    async fn claimed_task_counts_by_worker(&self) -> Result<Vec<(String, i64)>, StorageError>;

    // === Webhook Outbox ===

    /// Insert a webhook outbox row: `pending` rows queue an event for the
    /// drain loop / in-memory fast path; `parked` rows preserve a delivery
    /// that exhausted its retries for later inspection / redelivery.
    /// Best-effort at call sites: they log on error rather than failing the
    /// originating operation.
    async fn park_webhook(
        &self,
        entry: &orch8_types::webhook_outbox::WebhookOutboxEntry,
    ) -> Result<(), StorageError>;

    /// List parked webhook deliveries (exhausted retries; the operator
    /// surface), most recently parked first. In-flight `pending` work is
    /// deliberately not listed.
    async fn list_webhook_outbox(
        &self,
        limit: u32,
    ) -> Result<Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;

    /// Fetch one parked delivery by id.
    async fn get_webhook_outbox(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;

    /// Remove a parked delivery (after a successful redelivery or a discard).
    async fn delete_webhook_outbox(&self, id: Uuid) -> Result<(), StorageError>;

    /// Claim up to `limit` due `pending` outbox rows for dispatch, flipping
    /// them to `in_flight` with `now` as the claim timestamp. `Postgres` uses
    /// `FOR UPDATE SKIP LOCKED`; `SQLite` serializes the claim in a
    /// `BEGIN IMMEDIATE` transaction (the same analogue as
    /// [`SchedulingStore::claim_due_instances`]).
    async fn claim_due_webhook_outbox(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError>;

    /// Claim one specific `pending` row for the in-memory fast path. Returns
    /// `false` when the row is gone or already claimed — another dispatcher
    /// owns it and the caller must NOT dispatch a duplicate.
    async fn claim_webhook_outbox_row(
        &self,
        id: Uuid,
        claimed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    /// Record a failed dispatch attempt against a claimed row: bump
    /// `attempts`, store `last_error`, and clear the claim. When
    /// `next_attempt_at` is `Some` the row goes back to `pending` for that
    /// time (backoff reschedule); when `None` the delivery is exhausted and
    /// the row is `parked` (manual redelivery only — the pre-existing
    /// exhausted-delivery behavior).
    async fn fail_webhook_outbox_attempt(
        &self,
        id: Uuid,
        last_error: &str,
        next_attempt_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError>;

    /// Reset `in_flight` rows whose claim predates `stale_before` back to
    /// `pending` — crash recovery for a dispatcher that died mid-dispatch.
    /// Returns the number of rows recovered.
    async fn recover_stale_webhook_claims(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<u64, StorageError>;

    // === Webhook Delivery Attempts (delivery inspector) ===

    /// Record one delivery attempt (bounded, redacted metadata only).
    async fn record_webhook_attempt(
        &self,
        attempt: &orch8_types::webhook_delivery::WebhookDeliveryAttempt,
    ) -> Result<(), StorageError>;

    /// List delivery summaries (grouped by `delivery_id`), newest first.
    async fn list_webhook_deliveries(
        &self,
        filter: &orch8_types::webhook_delivery::DeliveryFilter,
        limit: u32,
    ) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliverySummary>, StorageError>;

    /// All attempts of one delivery, in attempt order.
    async fn get_webhook_delivery_attempts(
        &self,
        delivery_id: Uuid,
    ) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliveryAttempt>, StorageError>;

    /// Retention: delete attempts older than `cutoff`; returns rows removed.
    async fn delete_webhook_attempts_before(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, StorageError>;

    // === Queue Routing Rules ===

    /// Create a queue routing rule.
    async fn create_queue_routing_rule(
        &self,
        rule: &orch8_types::queue_routing::QueueRoutingRule,
    ) -> Result<(), StorageError>;

    /// List routing rules, optionally filtered by tenant and/or handler,
    /// ordered by `priority` DESC then `created_at` ASC. The enqueue path
    /// passes both filters; the API passes whatever the caller specifies.
    async fn list_queue_routing_rules(
        &self,
        tenant_id: Option<&TenantId>,
        handler_name: Option<&str>,
    ) -> Result<Vec<orch8_types::queue_routing::QueueRoutingRule>, StorageError>;

    /// Fetch one routing rule by id.
    async fn get_queue_routing_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::queue_routing::QueueRoutingRule>, StorageError>;

    /// Delete a routing rule.
    async fn delete_queue_routing_rule(&self, id: Uuid) -> Result<(), StorageError>;

    // === Worker Control Channel ===

    /// Queue a control command for a worker.
    async fn enqueue_worker_command(
        &self,
        command: &orch8_types::worker::WorkerCommand,
    ) -> Result<(), StorageError>;

    /// List pending commands for a worker, oldest first.
    async fn list_worker_commands(
        &self,
        worker_id: &str,
    ) -> Result<Vec<orch8_types::worker::WorkerCommand>, StorageError>;

    /// Acknowledge (delete) a delivered command.
    async fn delete_worker_command(&self, id: Uuid) -> Result<(), StorageError>;

    // === Worker Version Pins ===

    /// Create-or-update a `(tenant, handler)` minimum-version pin.
    async fn upsert_worker_version_pin(
        &self,
        pin: &orch8_types::worker::WorkerVersionPin,
    ) -> Result<(), StorageError>;

    /// Fetch the pin for a `(tenant, handler)` pair, if any.
    async fn get_worker_version_pin(
        &self,
        tenant_id: &str,
        handler_name: &str,
    ) -> Result<Option<orch8_types::worker::WorkerVersionPin>, StorageError>;

    /// List pins, optionally filtered by tenant.
    async fn list_worker_version_pins(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<orch8_types::worker::WorkerVersionPin>, StorageError>;

    /// Delete a `(tenant, handler)` pin.
    async fn delete_worker_version_pin(
        &self,
        tenant_id: &str,
        handler_name: &str,
    ) -> Result<(), StorageError>;

    // === Queue Dispatch Config ===

    /// Create-or-update a `(tenant, queue)` dispatch config.
    async fn upsert_queue_dispatch(
        &self,
        config: &orch8_types::queue_dispatch::QueueDispatchConfig,
    ) -> Result<(), StorageError>;

    /// Fetch the dispatch config for a `(tenant, queue)` pair, if any.
    async fn get_queue_dispatch(
        &self,
        tenant_id: &str,
        queue_name: &str,
    ) -> Result<Option<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError>;

    /// List dispatch configs, optionally filtered by tenant. Secrets are not
    /// returned (the `secret` field is always `None` in listed rows).
    async fn list_queue_dispatch(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError>;

    /// Delete a `(tenant, queue)` dispatch config.
    async fn delete_queue_dispatch(
        &self,
        tenant_id: &str,
        queue_name: &str,
    ) -> Result<(), StorageError>;

    // === Step Logs ===

    /// Append log lines for a step (best-effort — callers log on error rather
    /// than failing the step). Called by the in-process capture layer and by
    /// worker complete/fail with reported logs.
    async fn append_step_logs(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
        entries: &[orch8_types::step_log::StepLogEntry],
    ) -> Result<(), StorageError>;

    /// List all step logs for an instance, oldest first.
    async fn list_step_logs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::step_log::StepLog>, StorageError>;
}

// ============================================================================
// Sub-trait 7: SchedulingStore
// ============================================================================

#[async_trait]
pub trait SchedulingStore: Send + Sync + 'static {
    // === Cron Schedules ===

    async fn create_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError>;

    async fn list_cron_schedules(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<CronSchedule>, StorageError>;

    async fn update_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError>;

    async fn delete_cron_schedule(&self, id: Uuid) -> Result<(), StorageError>;

    /// Atomically claim enabled cron schedules whose `next_fire_at <= now`.
    ///
    /// **Missed-fire policy: skip.** If the scheduler was down and multiple
    /// fire windows were missed, only a single trigger fires -- the most
    /// recent due window. The `next_fire_at` is then advanced past `now` so
    /// no backfill of missed windows occurs. This prevents burst-spawning
    /// hundreds of instances after a prolonged outage.
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

    /// Record a skipped occurrence (the `skip` overlap policy): increment
    /// `skipped_fires`, stamp `last_skipped_at`, and advance the fire times
    /// in one statement so the schedule does not stay due.
    async fn record_cron_skip(
        &self,
        id: Uuid,
        now: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    /// Non-terminal instance IDs created by this schedule (attributed via
    /// the `metadata.cron_schedule_id` stamp the cron loop writes on every
    /// fire). Used by the overlap policies to detect still-active runs.
    async fn active_instance_ids_for_cron(
        &self,
        cron_id: Uuid,
        limit: u32,
    ) -> Result<Vec<InstanceId>, StorageError>;

    // === Rate Limits ===

    /// Atomic check-and-increment. Single DB round-trip.
    async fn check_rate_limit(
        &self,
        tenant_id: &TenantId,
        resource_key: &ResourceKey,
        now: DateTime<Utc>,
    ) -> Result<RateLimitCheck, StorageError>;

    async fn upsert_rate_limit(&self, limit: &RateLimit) -> Result<(), StorageError>;
}

// ============================================================================
// Sub-trait 8: AdminStore
// ============================================================================

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait AdminStore: Send + Sync + 'static {
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

    // === Plugins ===

    async fn create_plugin(&self, plugin: &PluginDef) -> Result<(), StorageError>;

    /// Fetch a plugin by name, scoped to `tenant_id`. `name` is a globally
    /// unique primary key, but callers must not be able to fetch another
    /// tenant's plugin by guessing/knowing its name -- passing `Some` here
    /// means a forgotten ownership check at the API layer fails closed
    /// (`None`) instead of leaking cross-tenant. `tenant_id: None` is an
    /// intentionally unscoped lookup for system contexts not acting on
    /// behalf of any one tenant -- new callers should reach for `Some`.
    async fn get_plugin(
        &self,
        tenant_id: Option<&TenantId>,
        name: &str,
    ) -> Result<Option<PluginDef>, StorageError>;

    async fn list_plugins(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<PluginDef>, StorageError>;

    async fn update_plugin(&self, plugin: &PluginDef) -> Result<(), StorageError>;

    async fn delete_plugin(&self, name: &str) -> Result<(), StorageError>;

    // === Triggers ===

    async fn create_trigger(&self, trigger: &TriggerDef) -> Result<(), StorageError>;

    /// Fetch a trigger by slug, scoped to `tenant_id` when `Some` (see
    /// [`Self::get_plugin`] for the rationale and the meaning of `None`).
    /// The public webhook endpoint legitimately needs `None`: resolving
    /// *which* tenant owns `slug` is the lookup's job there, not something
    /// the caller already knows.
    async fn get_trigger(
        &self,
        tenant_id: Option<&TenantId>,
        slug: &str,
    ) -> Result<Option<TriggerDef>, StorageError>;

    async fn list_triggers(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<TriggerDef>, StorageError>;

    async fn update_trigger(&self, trigger: &TriggerDef) -> Result<(), StorageError>;

    /// Delete a trigger and its associated poll state (if any).
    async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError>;

    /// Atomically claim a webhook nonce until `expires_at`.
    ///
    /// Returns `true` only for the first caller across all processes using
    /// this backend. Expired claims are removed opportunistically.
    async fn claim_webhook_nonce(
        &self,
        slug: &str,
        nonce: &str,
        expires_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    // === Trigger poll state (activepieces_poll) ===

    /// Fetch the persisted poll cursor/state for a polling trigger.
    /// Returns `None` if the trigger has never polled.
    async fn get_trigger_poll_state(
        &self,
        slug: &str,
    ) -> Result<Option<TriggerPollState>, StorageError>;

    /// Insert or replace the poll cursor/state for a polling trigger.
    async fn upsert_trigger_poll_state(&self, state: &TriggerPollState)
    -> Result<(), StorageError>;

    // === Credentials ===

    async fn create_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError>;

    /// Fetch a credential by ID, scoped to `tenant_id` when `Some` (see
    /// [`Self::get_plugin`] for the rationale and the meaning of `None`) --
    /// a forgotten check at a new API route must not leak another tenant's
    /// OAuth token.
    async fn get_credential(
        &self,
        tenant_id: Option<&TenantId>,
        id: &str,
    ) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError>;

    async fn list_credentials(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError>;

    async fn update_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError>;

    async fn delete_credential(&self, id: &str) -> Result<(), StorageError>;

    /// List `OAuth2` credentials whose `expires_at` is within `threshold` of now
    /// (and that have a `refresh_url` + `refresh_token` configured). Used by
    /// the refresh loop -- returns an empty vec if no credentials are due.
    async fn list_credentials_due_for_refresh(
        &self,
        threshold: std::time::Duration,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError>;

    // === API keys (per-tenant authentication) ===

    /// Persist a freshly minted API key. Only the SHA-256 hash is stored.
    async fn create_api_key(
        &self,
        key: &orch8_types::api_key::ApiKeyRecord,
    ) -> Result<(), StorageError>;

    /// Look up an API key by the SHA-256 hash of the presented secret. Returns
    /// the record (including revoked/expired ones — the caller decides) or
    /// `None` when no key matches.
    async fn lookup_api_key_by_hash(
        &self,
        key_hash: &str,
    ) -> Result<Option<orch8_types::api_key::ApiKeyRecord>, StorageError>;

    /// List API keys for a tenant (metadata only — never the secret).
    async fn list_api_keys(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::api_key::ApiKeyRecord>, StorageError>;

    /// Revoke a key by id. Returns `true` if a key was revoked, `false` if no
    /// key with that id exists.
    async fn revoke_api_key(&self, id: &str) -> Result<bool, StorageError>;

    /// Update `last_used_at` for a key. Fire-and-forget audit hygiene.
    async fn touch_api_key(
        &self,
        id: &str,
        at: chrono::DateTime<chrono::Utc>,
    ) -> Result<(), StorageError>;

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

    // === Circuit Breakers ===
    //
    // Only `Open` rows are persisted -- this is a correctness backstop so that
    // a crash mid-cooldown does not reset every tripped breaker. Implementors
    // must provide this capability; silently dropping state is not safe.

    /// Upsert an `Open` circuit breaker row. Keyed by `(tenant_id, handler)`.
    async fn upsert_circuit_breaker(&self, state: &CircuitBreakerState)
    -> Result<(), StorageError>;

    /// Return every persisted `Open` row across all tenants. Used at boot to
    /// rehydrate the in-memory registry.
    async fn list_open_circuit_breakers(&self) -> Result<Vec<CircuitBreakerState>, StorageError>;

    /// Delete any persisted circuit breaker row for `(tenant_id, handler)`.
    /// No-op if no row exists.
    async fn delete_circuit_breaker(
        &self,
        tenant_id: &TenantId,
        handler: &str,
    ) -> Result<(), StorageError>;

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
    ) -> Result<(), StorageError>;

    async fn get_rollback_policy(
        &self,
        tenant_id: &str,
        sequence_name: &str,
    ) -> Result<Option<orch8_types::rollback::RollbackPolicy>, StorageError>;

    async fn list_rollback_policies(
        &self,
        tenant_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<orch8_types::rollback::RollbackPolicy>, StorageError>;

    async fn delete_rollback_policy(
        &self,
        tenant_id: &str,
        sequence_name: &str,
    ) -> Result<(), StorageError>;

    async fn record_rollback(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        error_rate: f64,
        threshold: f64,
        reason: &str,
    ) -> Result<(), StorageError>;

    async fn query_error_rate(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        window_secs: i64,
    ) -> Result<Option<f64>, StorageError>;

    async fn list_rollback_history(
        &self,
        tenant_id: Option<&str>,
        sequence_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<orch8_types::rollback::RollbackHistory>, StorageError>;

    // === Health ===

    async fn ping(&self) -> Result<(), StorageError>;
}

// ============================================================================
// Sub-trait 9: TelemetryStore
// ============================================================================

#[allow(clippy::too_many_arguments)]
#[async_trait]
pub trait TelemetryStore: Send + Sync + 'static {
    async fn ingest_telemetry_event(
        &self,
        event_type: &str,
        payload: &str,
        device_id: &str,
        os_name: &str,
        os_version: &str,
        app_version: &str,
        sdk_version: &str,
        tenant_id: &str,
        created_at: DateTime<Utc>,
    ) -> Result<(), StorageError>;

    /// Batch-insert telemetry events in a single round-trip.
    ///
    /// Default: falls back to one-by-one insertion.
    async fn ingest_telemetry_events_batch(
        &self,
        events: &[TelemetryEvent],
    ) -> Result<u64, StorageError> {
        let mut count = 0u64;
        for e in events {
            self.ingest_telemetry_event(
                &e.event_type,
                &e.payload,
                &e.device_id,
                &e.os_name,
                &e.os_version,
                &e.app_version,
                &e.sdk_version,
                &e.tenant_id,
                e.created_at,
            )
            .await?;
            count += 1;
        }
        Ok(count)
    }

    async fn ingest_telemetry_error(
        &self,
        error_type: &str,
        message: &str,
        stack_trace: Option<&str>,
        device_id: &str,
        os_name: &str,
        os_version: &str,
        app_version: &str,
        sdk_version: &str,
        tenant_id: &str,
        instance_id: Option<&str>,
        sequence_name: Option<&str>,
    ) -> Result<(), StorageError>;

    async fn query_telemetry_dashboard(
        &self,
        query_type: &str,
        tenant_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<(String, i64)>, StorageError>;

    async fn delete_old_telemetry_events(
        &self,
        older_than: DateTime<Utc>,
        limit: u32,
    ) -> Result<u64, StorageError>;

    /// Record a usage event (e.g. LLM token consumption). Best-effort, called
    /// from the hot path — callers log and continue on error.
    async fn record_usage_event(&self, event: &UsageEvent) -> Result<(), StorageError>;

    /// Aggregate a tenant's usage over `[start, end)`, grouped by `(kind, model)`.
    /// Default: empty.
    async fn query_usage(
        &self,
        tenant_id: &str,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
    ) -> Result<Vec<UsageAggregate>, StorageError>;

    /// Sum recorded usage for a single instance across all `usage_events`
    /// rows. Returns `(input_tokens, output_tokens)`.
    ///
    /// Used by the scheduler's budget enforcement — only called for instances
    /// that actually carry a token budget.
    async fn query_instance_usage_totals(
        &self,
        instance_id: InstanceId,
    ) -> Result<(i64, i64), StorageError>;
}

// ============================================================================
// Sub-trait 10: ResourceStore
// ============================================================================

#[async_trait]
pub trait ResourceStore: Send + Sync + 'static {
    // === Instance KV State ===
    //
    // Per-instance key-value store for workflow state that persists across
    // ticks. Used by `set_state` / `get_state` / `delete_state` built-in
    // handlers and the `state.*` template root variable.

    async fn set_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn get_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError>;

    async fn get_all_instance_kv(
        &self,
        instance_id: InstanceId,
    ) -> Result<HashMap<String, serde_json::Value>, StorageError>;

    async fn delete_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<(), StorageError>;

    // === Shared agent knowledge ===
    //
    // Tenant-isolated, namespace-scoped records shared by workflows and agent
    // instances. The value is opaque to storage; the agent-memory handlers
    // persist text, embeddings, metadata, and provenance in it.

    async fn set_shared_knowledge(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError>;

    async fn list_shared_knowledge(
        &self,
        tenant_id: &str,
        namespace: &str,
        limit: u32,
    ) -> Result<HashMap<String, serde_json::Value>, StorageError>;

    async fn delete_shared_knowledge(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
    ) -> Result<(), StorageError>;

    // === Artifacts (durable binary blobs) ===
    //
    // Backed by an object-store backend (local FS / S3-compatible) wired into
    // the concrete storage impl. Defaults return `Unsupported` so a backend
    // with no artifact store configured fails loudly rather than silently
    // dropping bytes — losing artifacts would break the durability contract.

    /// True when a durable artifact backend is wired into this storage. Lets
    /// callers (e.g. the retention sweeper) skip artifact work entirely when
    /// artifacts are disabled, rather than no-op'ing per instance. Default
    /// `false` — backends opt in by overriding.
    fn artifacts_enabled(&self) -> bool {
        false
    }

    async fn put_artifact(
        &self,
        _instance_id: InstanceId,
        _content_type: &str,
        _bytes: bytes::Bytes,
    ) -> Result<orch8_types::artifact::ArtifactRef, StorageError> {
        Err(StorageError::Unsupported(
            "artifact storage is not configured".into(),
        ))
    }

    async fn get_artifact(&self, _key: &str) -> Result<Option<Vec<u8>>, StorageError> {
        Err(StorageError::Unsupported(
            "artifact storage is not configured".into(),
        ))
    }

    async fn delete_artifact(&self, _key: &str) -> Result<(), StorageError> {
        Err(StorageError::Unsupported(
            "artifact storage is not configured".into(),
        ))
    }

    async fn list_artifacts(
        &self,
        _instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::artifact::ArtifactMeta>, StorageError> {
        Err(StorageError::Unsupported(
            "artifact storage is not configured".into(),
        ))
    }

    /// Delete every artifact belonging to an instance and return the count
    /// removed. Best-effort lifecycle cleanup for retention / instance deletion.
    ///
    /// Returns `Ok(0)` when no artifact backend is configured (so callers can
    /// invoke it unconditionally). For S3/R2, also configure a bucket lifecycle
    /// (TTL) policy as defense-in-depth against orphaned blobs.
    ///
    /// Provided method: lists then deletes; concrete backends inherit it.
    async fn delete_instance_artifacts(
        &self,
        instance_id: InstanceId,
    ) -> Result<u64, StorageError> {
        let metas = match self.list_artifacts(instance_id).await {
            Ok(m) => m,
            // No backend configured → nothing to clean.
            Err(StorageError::Unsupported(_)) => return Ok(0),
            Err(e) => return Err(e),
        };
        let mut removed = 0u64;
        for m in metas {
            self.delete_artifact(&m.key).await?;
            removed += 1;
        }
        Ok(removed)
    }

    /// Return up to `limit` instances in a **terminal** state whose `updated_at`
    /// is older than `cutoff` and that have not yet had their artifacts swept
    /// (no `_artifacts_gced` marker). Drives the background artifact-retention
    /// sweeper.
    async fn list_artifact_gc_candidates(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<InstanceId>, StorageError>;

    /// Mark an instance's artifacts as swept (idempotent) so the retention
    /// sweeper does not re-scan it.
    async fn mark_artifacts_gced(&self, instance_id: InstanceId) -> Result<(), StorageError>;

    /// Delete up to `limit` **terminal** instances (state Completed / Failed /
    /// Cancelled -- same set as [`Self::list_artifact_gc_candidates`]) whose
    /// `updated_at` is older than `cutoff`, along with their execution
    /// history: `task_instances` row deletion cascades to execution tree,
    /// block outputs, signals, worker tasks, checkpoints and externalized
    /// state (all declare `ON DELETE CASCADE`); `step_logs`, `audit_log` and
    /// `usage_events` have no FK to `task_instances` and are deleted
    /// explicitly for the same instance ids in the same transaction.
    ///
    /// Drives the opt-in instance-retention GC sweeper
    /// (`instance_retention_secs` in `SchedulerConfig`) -- unlike artifact
    /// retention this removes queryable instance history, so it is off by
    /// default. Returns the number of instances deleted.
    async fn delete_terminal_instances(
        &self,
        cutoff: DateTime<Utc>,
        limit: u32,
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
    /// backends compile -- production callers must use a backend that
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
    /// exist in `externalized_state` (missing keys are **not** errors -- the
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
    async fn delete_expired_externalized_state(&self, limit: u32) -> Result<u64, StorageError>;

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

    /// List checkpoints for an instance (most recent first).
    async fn list_checkpoints(
        &self,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<orch8_types::checkpoint::Checkpoint>, StorageError>;

    /// Delete old checkpoints, keeping only the latest N.
    async fn prune_checkpoints(
        &self,
        instance_id: InstanceId,
        keep: u32,
    ) -> Result<u64, StorageError>;
}

// ============================================================================
// Sub-trait 11: MobileSyncStore
// ============================================================================

/// Device registration record.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MobileDevice {
    pub device_id: String,
    pub tenant_id: String,
    pub push_token: Option<String>,
    pub platform: String,
    pub app_version: Option<String>,
    pub active: bool,
    pub last_sync_at: Option<String>,
    pub registered_at: String,
}

/// Status update from a mobile device for a single instance.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MobileInstanceStatus {
    pub device_id: String,
    pub instance_id: String,
    pub sequence_name: Option<String>,
    pub state: String,
    pub current_step: Option<String>,
    pub handler: Option<String>,
    pub context_summary: Option<String>,
    /// JSON array of step entries from the execution tree:
    /// `[{block_id, block_type, state, handler, started_at, completed_at}]`
    pub steps: Option<String>,
    pub updated_at: String,
}

/// Approval request sent from mobile when a `wait_for_input` step is hit.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MobileApprovalRequest {
    pub id: String,
    pub device_id: String,
    pub tenant_id: String,
    pub instance_id: String,
    pub block_id: String,
    pub sequence_name: Option<String>,
    pub prompt: Option<String>,
    pub choices: Option<String>,
    pub store_as: Option<String>,
    pub timeout_secs: Option<i64>,
    pub metadata: Option<String>,
    pub state: String,
    pub resolution: Option<String>,
    pub created_at: String,
    pub resolved_at: Option<String>,
}

/// Command queued on the server for a mobile device to execute.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MobileCommand {
    pub id: String,
    pub device_id: String,
    pub command_type: String,
    pub payload: String,
    pub created_at: String,
    pub acked_at: Option<String>,
}

#[async_trait]
pub trait MobileSyncStore: Send + Sync + 'static {
    // --- Devices ---

    async fn register_mobile_device(&self, device: &MobileDevice) -> Result<(), StorageError>;

    async fn get_mobile_device(
        &self,
        device_id: &str,
    ) -> Result<Option<MobileDevice>, StorageError>;

    async fn update_device_last_sync(&self, device_id: &str) -> Result<(), StorageError>;

    async fn list_mobile_devices(
        &self,
        tenant_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<MobileDevice>, StorageError>;

    async fn mark_stale_devices_inactive(
        &self,
        stale_threshold_secs: i64,
    ) -> Result<u64, StorageError>;

    // --- Instance Status ---

    async fn upsert_mobile_instance_status(
        &self,
        status: &MobileInstanceStatus,
    ) -> Result<(), StorageError>;

    async fn upsert_mobile_instance_status_batch(
        &self,
        statuses: &[MobileInstanceStatus],
    ) -> Result<(), StorageError> {
        for s in statuses {
            self.upsert_mobile_instance_status(s).await?;
        }
        Ok(())
    }

    async fn list_mobile_instance_status(
        &self,
        tenant_id: Option<&str>,
        device_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<MobileInstanceStatus>, StorageError>;

    // --- Approval Requests ---

    async fn insert_mobile_approval(
        &self,
        approval: &MobileApprovalRequest,
    ) -> Result<bool, StorageError>;

    async fn get_mobile_approval(
        &self,
        id: &str,
    ) -> Result<Option<MobileApprovalRequest>, StorageError>;

    async fn resolve_mobile_approval(
        &self,
        id: &str,
        resolution: &str,
    ) -> Result<Option<MobileApprovalRequest>, StorageError>;

    async fn list_mobile_approvals(
        &self,
        tenant_id: Option<&str>,
        state: Option<&str>,
        limit: u32,
    ) -> Result<Vec<MobileApprovalRequest>, StorageError>;

    async fn expire_mobile_approvals(&self) -> Result<u64, StorageError>;

    // --- Commands ---

    async fn create_mobile_command(&self, command: &MobileCommand) -> Result<(), StorageError>;

    async fn fetch_pending_commands(
        &self,
        device_id: &str,
        limit: u32,
    ) -> Result<Vec<MobileCommand>, StorageError>;

    async fn ack_mobile_commands(
        &self,
        device_id: &str,
        command_ids: &[String],
    ) -> Result<u64, StorageError>;

    async fn cleanup_acked_commands(&self, older_than_secs: i64) -> Result<u64, StorageError>;

    async fn cleanup_expired_commands(&self, ttl_secs: i64) -> Result<u64, StorageError>;
}

// ============================================================================
// Sub-trait 12: ContinuityStore
// ============================================================================

/// Durable portable-execution state. Compare-and-swap methods are the only
/// supported way to advance ownership, handoff, and effect state.
#[async_trait]
pub trait ContinuityStore: Send + Sync + 'static {
    async fn create_continuity_execution(
        &self,
        execution: &ContinuityExecution,
    ) -> Result<(), StorageError>;

    /// Return the durable execution scope for an instance, creating the
    /// supplied candidate when absent.
    ///
    /// `create_continuity_execution` is protected by unique constraints on
    /// both continuity ID and current instance. If concurrent dispatchers race
    /// to create the same scope, the loser re-reads the winner. A create error
    /// is only suppressed when that durable winner is observable.
    async fn ensure_continuity_execution(
        &self,
        candidate: &ContinuityExecution,
    ) -> Result<ContinuityExecution, StorageError> {
        if let Some(existing) = self
            .get_continuity_execution_by_instance(
                &candidate.tenant_id,
                candidate.current_instance_id,
            )
            .await?
        {
            return Ok(existing);
        }

        match self.create_continuity_execution(candidate).await {
            Ok(()) => Ok(candidate.clone()),
            Err(create_error) => self
                .get_continuity_execution_by_instance(
                    &candidate.tenant_id,
                    candidate.current_instance_id,
                )
                .await?
                .ok_or(create_error),
        }
    }

    async fn get_continuity_execution(
        &self,
        tenant_id: &TenantId,
        id: ContinuityId,
    ) -> Result<Option<ContinuityExecution>, StorageError>;

    async fn get_continuity_execution_by_instance(
        &self,
        tenant_id: &TenantId,
        instance_id: InstanceId,
    ) -> Result<Option<ContinuityExecution>, StorageError>;

    async fn list_continuity_locations(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<orch8_types::continuity::ContinuityLocation>, StorageError>;

    async fn cas_continuity_owner(
        &self,
        tenant_id: &TenantId,
        id: ContinuityId,
        expected_epoch: ExecutionEpoch,
        expected_owner: RuntimeId,
        next: &ContinuityExecution,
    ) -> Result<bool, StorageError>;

    async fn create_handoff(&self, handoff: &ExecutionHandoff) -> Result<(), StorageError>;

    async fn get_handoff(
        &self,
        tenant_id: &TenantId,
        id: HandoffId,
    ) -> Result<Option<ExecutionHandoff>, StorageError>;

    async fn cas_handoff(
        &self,
        tenant_id: &TenantId,
        id: HandoffId,
        expected_state: HandoffState,
        expected_version: u64,
        next: &ExecutionHandoff,
    ) -> Result<bool, StorageError>;

    /// Atomically accept a handoff and transfer execution ownership. Returns
    /// false when either the handoff CAS token or ownership epoch is stale.
    async fn accept_handoff(
        &self,
        tenant_id: &TenantId,
        expected_handoff: &ExecutionHandoff,
        accepted_handoff: &ExecutionHandoff,
        expected_execution: &ContinuityExecution,
        accepted_execution: &ContinuityExecution,
    ) -> Result<bool, StorageError>;

    /// Atomically attach an exported capsule and move ownership into the
    /// transferring state without advancing the epoch.
    async fn commit_handoff_export(
        &self,
        tenant_id: &TenantId,
        expected_handoff: &ExecutionHandoff,
        exported_handoff: &ExecutionHandoff,
        expected_execution: &ContinuityExecution,
        transferring_execution: &ContinuityExecution,
    ) -> Result<bool, StorageError>;

    /// Atomically schedule the paused destination and record the handoff as
    /// resumed. Returns false when either compare-and-swap token is stale.
    async fn resume_handoff(
        &self,
        tenant_id: &TenantId,
        expected_handoff: &ExecutionHandoff,
        resumed_handoff: &ExecutionHandoff,
        destination_instance_id: InstanceId,
    ) -> Result<bool, StorageError>;

    async fn save_capsule_manifest(&self, manifest: &CapsuleManifest) -> Result<(), StorageError>;

    async fn get_capsule_manifest(
        &self,
        tenant_id: &TenantId,
        id: CapsuleId,
    ) -> Result<Option<CapsuleManifest>, StorageError>;

    /// Idempotently import a capsule by atomically claiming the destination,
    /// creating its paused local instance, and storing its checkpoint.
    async fn import_capsule_instance(
        &self,
        capsule_id: CapsuleId,
        destination_runtime_id: RuntimeId,
        instance: &TaskInstance,
        checkpoint: &orch8_types::checkpoint::Checkpoint,
    ) -> Result<InstanceId, StorageError>;

    /// Verify that an imported instance was created from the given capsule for
    /// the destination runtime. Import records are immutable.
    async fn is_capsule_import_instance(
        &self,
        tenant_id: &TenantId,
        capsule_id: CapsuleId,
        destination_runtime_id: RuntimeId,
        instance_id: InstanceId,
    ) -> Result<bool, StorageError>;

    async fn upsert_runtime_capabilities(
        &self,
        tenant_id: &TenantId,
        capabilities: &RuntimeCapabilities,
    ) -> Result<(), StorageError>;

    async fn list_runtime_capabilities(
        &self,
        tenant_id: &TenantId,
        observed_after: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<RuntimeCapabilities>, StorageError>;

    async fn create_effect_receipt(&self, receipt: &EffectReceipt) -> Result<(), StorageError>;

    /// Insert a deterministic receipt if absent and return the durable value.
    /// This is the crash/retry-safe entry point used by dispatch paths.
    async fn ensure_effect_receipt(
        &self,
        receipt: &EffectReceipt,
    ) -> Result<EffectReceipt, StorageError>;

    async fn find_unresolved_effect_receipt(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        instance_id: InstanceId,
        block_id: &orch8_types::ids::BlockId,
    ) -> Result<Option<EffectReceipt>, StorageError>;

    async fn get_effect_receipt(
        &self,
        tenant_id: &TenantId,
        id: EffectId,
    ) -> Result<Option<EffectReceipt>, StorageError>;

    async fn cas_effect_receipt(
        &self,
        tenant_id: &TenantId,
        id: EffectId,
        expected_state: EffectState,
        next: &EffectReceipt,
    ) -> Result<bool, StorageError>;

    /// Atomically transition a prepared receipt to dispatched only when it is
    /// the first active effect with the same kind, destination, and request
    /// fingerprint in this continuity execution.
    async fn dispatch_effect_receipt_at_most_once(
        &self,
        tenant_id: &TenantId,
        next: &EffectReceipt,
    ) -> Result<orch8_types::continuity::EffectDispatchOutcome, StorageError>;

    async fn list_effect_receipts(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<EffectReceipt>, StorageError>;

    /// List the universal effect ledger for one runtime-local instance.
    async fn list_instance_effect_receipts(
        &self,
        tenant_id: &TenantId,
        instance_id: InstanceId,
        limit: u32,
    ) -> Result<Vec<EffectReceipt>, StorageError> {
        let Some(execution) = self
            .get_continuity_execution_by_instance(tenant_id, instance_id)
            .await?
        else {
            return Ok(Vec::new());
        };
        self.list_effect_receipts(tenant_id, execution.continuity_id, limit)
            .await
    }

    async fn append_provenance(&self, entry: &ProvenanceEntry) -> Result<(), StorageError>;

    /// Return the current chain head without loading the retained history.
    async fn get_provenance_head(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
    ) -> Result<Option<ProvenanceEntry>, StorageError>;

    async fn list_provenance(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<ProvenanceEntry>, StorageError>;

    /// Persist a verified federation message exactly once.
    ///
    /// Returns `false` when the peer/message tuple was already retained. The
    /// uniqueness constraint is the replay boundary; callers must verify the
    /// signature and epoch before invoking this method.
    async fn accept_federation_message(
        &self,
        envelope: &FederationEnvelope,
        envelope_sha256: &str,
        accepted_at: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn save_incident_reproduction(
        &self,
        reproduction: &DlqIncidentReproduction,
    ) -> Result<(), StorageError>;

    async fn list_incident_reproductions(
        &self,
        tenant_id: &TenantId,
        fingerprint: &str,
        limit: u32,
    ) -> Result<Vec<DlqIncidentReproduction>, StorageError>;

    async fn create_continuation_grant(
        &self,
        grant: &ContinuationGrant,
    ) -> Result<(), StorageError>;

    async fn get_continuation_grant(
        &self,
        tenant_id: &TenantId,
        id: ContinuationGrantId,
    ) -> Result<Option<ContinuationGrant>, StorageError>;

    /// Atomically consume an active, unexpired grant. A nonce can win once.
    async fn consume_continuation_grant(
        &self,
        tenant_id: &TenantId,
        id: ContinuationGrantId,
        nonce_sha256: &str,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn cas_continuation_grant_state(
        &self,
        tenant_id: &TenantId,
        id: ContinuationGrantId,
        expected: ContinuationGrantState,
        next: &ContinuationGrant,
    ) -> Result<bool, StorageError>;

    async fn save_placement_decision(
        &self,
        decision: &PlacementDecision,
    ) -> Result<(), StorageError>;

    async fn get_placement_decision(
        &self,
        tenant_id: &TenantId,
        id: PlacementDecisionId,
    ) -> Result<Option<PlacementDecision>, StorageError>;

    async fn create_continuity_stream(&self, stream: &ContinuityStream)
    -> Result<(), StorageError>;

    async fn get_continuity_stream(
        &self,
        tenant_id: &TenantId,
        stream_id: StreamId,
    ) -> Result<Option<ContinuityStream>, StorageError>;

    /// Append exactly the next frame in a single transaction. Returns false
    /// for a stale epoch, expired stream, or non-contiguous sequence number.
    async fn append_stream_frame(&self, frame: &StreamFrame) -> Result<bool, StorageError>;

    async fn list_stream_frames(
        &self,
        tenant_id: &TenantId,
        stream_id: StreamId,
        after_sequence: Option<u64>,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<StreamFrame>, StorageError>;

    async fn retract_stream_frames(
        &self,
        tenant_id: &TenantId,
        stream_id: StreamId,
        epoch: ExecutionEpoch,
        after_sequence: u64,
    ) -> Result<u64, StorageError>;

    async fn save_live_migration(
        &self,
        record: &orch8_types::continuity_advanced::LiveMigrationRecord,
    ) -> Result<(), StorageError>;

    async fn get_live_migration(
        &self,
        tenant_id: &TenantId,
        id: orch8_types::continuity_advanced::MigrationPlanId,
    ) -> Result<Option<orch8_types::continuity_advanced::LiveMigrationRecord>, StorageError>;

    async fn cas_live_migration(
        &self,
        tenant_id: &TenantId,
        expected: &orch8_types::continuity_advanced::LiveMigrationRecord,
        next: &orch8_types::continuity_advanced::LiveMigrationRecord,
    ) -> Result<bool, StorageError>;

    /// Atomically advance a migration record, continuity epoch, instance
    /// sequence/context/state, execution tree, and checkpoint. When
    /// `forbid_effects_epoch` is set, the transaction fails closed if that
    /// epoch has any dispatched/committed/unknown/verified effect receipt.
    async fn commit_live_migration_transition(
        &self,
        transition: LiveMigrationTransition<'_>,
    ) -> Result<bool, StorageError>;

    async fn create_compensation_run(
        &self,
        run: &orch8_types::continuity_advanced::CompensationRunRecord,
    ) -> Result<bool, StorageError>;

    async fn get_compensation_run(
        &self,
        tenant_id: &TenantId,
        id: orch8_types::continuity_advanced::CompensationRunId,
    ) -> Result<Option<orch8_types::continuity_advanced::CompensationRunRecord>, StorageError>;

    async fn cas_compensation_run(
        &self,
        tenant_id: &TenantId,
        expected_version: u64,
        next: &orch8_types::continuity_advanced::CompensationRunRecord,
    ) -> Result<bool, StorageError>;
}

/// Inputs to one atomic live-migration state transition. Grouping the fields
/// prevents apply and rollback call sites from accidentally swapping adjacent
/// state, sequence, or epoch arguments.
pub struct LiveMigrationTransition<'a> {
    pub tenant_id: &'a TenantId,
    pub expected_record: &'a orch8_types::continuity_advanced::LiveMigrationRecord,
    pub next_record: &'a orch8_types::continuity_advanced::LiveMigrationRecord,
    pub expected_execution: &'a ContinuityExecution,
    pub next_execution: &'a ContinuityExecution,
    pub expected_instance_state: orch8_types::instance::InstanceState,
    pub next_instance_state: orch8_types::instance::InstanceState,
    pub next_sequence_id: SequenceId,
    pub next_context: &'a orch8_types::context::ExecutionContext,
    pub checkpoint: &'a orch8_types::checkpoint::Checkpoint,
    pub forbid_effects_epoch: Option<ExecutionEpoch>,
}

// ============================================================================
// Sub-traits 13-15: Continuity evidence and policy stores
// ============================================================================

#[async_trait]
pub trait InvariantStore: Send + Sync + 'static {
    async fn create_workflow_invariant(
        &self,
        invariant: &WorkflowInvariant,
    ) -> Result<(), StorageError>;

    async fn list_workflow_invariants(
        &self,
        tenant_id: &TenantId,
        sequence_id: SequenceId,
        sequence_version: i32,
        limit: u32,
    ) -> Result<Vec<WorkflowInvariant>, StorageError>;

    /// Append a deduplicated result. Returns false when the same evidence was
    /// already evaluated for this tenant.
    async fn append_invariant_result(
        &self,
        tenant_id: &TenantId,
        result: &InvariantResult,
    ) -> Result<bool, StorageError>;

    async fn list_invariant_results(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<InvariantResult>, StorageError>;

    async fn save_what_if_run(
        &self,
        run: &orch8_types::continuity_advanced::WhatIfRunRecord,
    ) -> Result<(), StorageError>;

    async fn list_what_if_runs(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<orch8_types::continuity_advanced::WhatIfRunRecord>, StorageError>;
}

#[async_trait]
pub trait EvaluationStore: Send + Sync + 'static {
    /// Append a direct or deferred score exactly once by tenant/dedupe key.
    async fn append_evaluation_score(&self, score: &EvaluationScore) -> Result<bool, StorageError>;

    async fn list_evaluation_scores(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<EvaluationScore>, StorageError>;
}

#[async_trait]
pub trait AttentionStore: Send + Sync + 'static {
    async fn create_attention_task(&self, task: &AttentionTask) -> Result<(), StorageError>;

    async fn get_attention_task(
        &self,
        tenant_id: &TenantId,
        id: AttentionTaskId,
    ) -> Result<Option<AttentionTask>, StorageError>;

    async fn claim_attention_task(
        &self,
        tenant_id: &TenantId,
        expected: &AttentionTask,
        assigned: &AttentionTask,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn reassign_expired_attention_task(
        &self,
        expected: &AttentionTask,
        assigned: &AttentionTask,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    async fn decide_attention_task(
        &self,
        expected: &AttentionTask,
        decided: &AttentionTask,
        now: DateTime<Utc>,
    ) -> Result<bool, StorageError>;

    /// Atomically reserve all dimensions against the execution's currently
    /// active reservations. Returns false for stale epochs or hard-limit
    /// exhaustion.
    async fn reserve_budget(
        &self,
        reservation: &BudgetReservation,
        budget: &orch8_types::instance::Budget,
    ) -> Result<bool, StorageError>;

    async fn get_budget_reservation(
        &self,
        tenant_id: &TenantId,
        id: orch8_types::continuity_advanced::BudgetReservationId,
    ) -> Result<Option<BudgetReservation>, StorageError>;

    async fn list_budget_reservations(
        &self,
        tenant_id: &TenantId,
        continuity_id: ContinuityId,
        limit: u32,
    ) -> Result<Vec<BudgetReservation>, StorageError>;

    /// Atomically transitions an active reservation to reconciled or released.
    /// Returns false when another caller already settled it or its epoch changed.
    async fn settle_budget_reservation(
        &self,
        reservation: &BudgetReservation,
    ) -> Result<bool, StorageError>;
}

// ============================================================================
// StorageBackend supertrait
// ============================================================================

/// The core storage abstraction.
///
/// Object-safe for `dyn StorageBackend` dispatch.
/// Every method returns `Result<T, StorageError>`.
/// Batch methods exist for hot paths.
///
/// `StorageBackend` is a supertrait of all domain-scoped sub-traits.
/// Any type implementing all sub-traits automatically implements
/// `StorageBackend` via the blanket impl below.
pub trait StorageBackend:
    SequenceStore
    + InstanceStore
    + ExecutionTreeStore
    + OutputStore
    + SignalStore
    + WorkerStore
    + SchedulingStore
    + AdminStore
    + TelemetryStore
    + ResourceStore
    + MobileSyncStore
    + ContinuityStore
    + InvariantStore
    + EvaluationStore
    + AttentionStore
    + Send
    + Sync
    + 'static
{
}

/// Blanket impl: any type implementing all sub-traits automatically
/// implements `StorageBackend`.
impl<T> StorageBackend for T where
    T: SequenceStore
        + InstanceStore
        + ExecutionTreeStore
        + OutputStore
        + SignalStore
        + WorkerStore
        + SchedulingStore
        + AdminStore
        + TelemetryStore
        + ResourceStore
        + MobileSyncStore
        + ContinuityStore
        + InvariantStore
        + EvaluationStore
        + AttentionStore
        + Send
        + Sync
        + 'static
{
}
