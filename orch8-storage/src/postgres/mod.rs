// Postgres uses i32 for integer columns; our domain types use u32.
// The values are always small enough that wrapping/sign-loss cannot occur.
#![allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]

mod ai;
mod alert_rules;
mod api_keys;
mod approval_tokens;
mod audit;
mod capsule_activation;
mod capsule_export;
mod capsule_source_snapshot;
mod checkpoints;
mod circuit_breakers;
mod claimed_writes;
mod cluster;
mod compensation_start;
mod continuity;
mod credentials;
mod cron;
mod effect_attempt;
mod effect_dispatch;
mod effect_receipts;
mod events;
mod evidence;
mod execution_tree;
mod externalized;
mod externalized_snapshot;
mod federation;
mod instances;
mod json_encoding;
mod kv_state;
mod local_migration;
mod misc;
mod mobile_access;
mod mobile_sync;
mod outputs;
mod placement;
mod plugins;
mod pools;
mod portable_effects;
mod progress_shares;
mod push_outbox;
mod queue_dispatch;
mod queue_routing;
mod rate_limits;
mod releases;
mod restore;
mod rollback;
mod rows;
mod sequences;
mod sessions;
mod signals;
mod step_logs;
mod telemetry;
mod tenancy;
mod triggers;
mod webhook_deliveries;
mod webhook_outbox;
mod worker_admission;
mod worker_claim;
mod worker_commands;
mod worker_dispatch;
mod worker_invocations;
mod worker_outcome;
mod worker_resolution;
mod worker_resolution_application;
mod worker_version_pins;
mod workers;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::PgPool;
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
use orch8_types::worker::{WorkerClaim, WorkerTask, WorkerTaskAttemptEvent};

/// Rows per statement for maintenance DELETE/UPDATEs that loop until done
/// (L4-12 / L4-15): small enough to stay far below `statement_timeout` and
/// keep row locks short, large enough to drain a backlog quickly.
pub(crate) const BULK_STATEMENT_BATCH: i64 = 5_000;

/// Upper bound on batches one bounded-retention call issues (D-02). A long
/// outage can leave millions of expired rows; a single call deletes at most
/// `BULK_MAX_BATCHES * BULK_STATEMENT_BATCH` of them (or stops at
/// [`BULK_TIME_BUDGET`]) and the next periodic tick continues.
pub(crate) const BULK_MAX_BATCHES: u32 = 40;

/// Wall-clock budget for one bounded-retention call (D-02).
pub(crate) const BULK_TIME_BUDGET: std::time::Duration = std::time::Duration::from_secs(20);

/// Delete rows older than `cutoff` in `BULK_STATEMENT_BATCH`-row statements
/// until a short batch, [`BULK_MAX_BATCHES`] batches, or [`BULK_TIME_BUDGET`].
/// `sql` takes the cutoff as `$1` and the batch size as `$2`. Returns the
/// rows deleted by this call.
pub(crate) async fn bulk_delete_before(
    pool: &sqlx::PgPool,
    sql: &'static str,
    cutoff: DateTime<Utc>,
) -> Result<u64, StorageError> {
    let started = std::time::Instant::now();
    let mut total = 0;
    for _ in 0..BULK_MAX_BATCHES {
        let deleted = sqlx::query(sql)
            .bind(cutoff)
            .bind(BULK_STATEMENT_BATCH)
            .execute(pool)
            .await?
            .rows_affected();
        total += deleted;
        if deleted < BULK_STATEMENT_BATCH.unsigned_abs() || started.elapsed() >= BULK_TIME_BUDGET {
            break;
        }
    }
    Ok(total)
}

#[derive(Clone)]
enum ConnectionMode {
    Runtime,
    ReadSnapshot(std::sync::Arc<std::sync::atomic::AtomicBool>),
}

pub struct PostgresStorage {
    pub(crate) pool: PgPool,
    /// Optional durable artifact backend (local FS / S3). `None` → artifact
    /// methods return `Unsupported`.
    artifact_store: Option<std::sync::Arc<crate::artifacts::ObjectArtifactStore>>,
}

impl PostgresStorage {
    /// Attach a durable artifact backend (builder-style).
    #[must_use]
    pub fn with_artifact_store(
        mut self,
        store: std::sync::Arc<crate::artifacts::ObjectArtifactStore>,
    ) -> Self {
        self.artifact_store = Some(store);
        self
    }

    pub async fn new(
        database_url: &str,
        max_connections: u32,
        search_path: Option<&str>,
    ) -> Result<Self, StorageError> {
        Self::with_min_connections(database_url, max_connections, 2, search_path).await
    }

    #[allow(unknown_lints, clippy::duration_suboptimal_units)]
    pub async fn with_min_connections(
        database_url: &str,
        max_connections: u32,
        min_connections: u32,
        search_path: Option<&str>,
    ) -> Result<Self, StorageError> {
        Self::connect(
            database_url,
            max_connections,
            min_connections,
            search_path,
            ConnectionMode::Runtime,
        )
        .await
    }

    /// Read a logical backup through one read-only, repeatable-read transaction.
    ///
    /// All reads share the initial view across pool acquisitions. Losing that
    /// connection fails subsequent reads; replacement connections are rejected
    /// so a backup cannot silently switch snapshots. Drop/close the storage to
    /// release its transaction. Normal statement and idle-transaction guards
    /// remain active. This storage is intended exclusively for backup reads.
    pub async fn read_snapshot(
        database_url: &str,
        search_path: Option<&str>,
    ) -> Result<Self, StorageError> {
        Self::connect(
            database_url,
            1,
            1,
            search_path,
            ConnectionMode::ReadSnapshot(std::sync::Arc::new(std::sync::atomic::AtomicBool::new(
                false,
            ))),
        )
        .await
    }

    #[allow(unknown_lints, clippy::duration_suboptimal_units)]
    async fn connect(
        database_url: &str,
        max_connections: u32,
        min_connections: u32,
        search_path: Option<&str>,
        mode: ConnectionMode,
    ) -> Result<Self, StorageError> {
        // Validate the schema name up-front (before moving it into the
        // after_connect closure) so an injection attempt fails fast.
        let search_path_value = match search_path {
            Some(schema) => {
                if !schema
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_')
                {
                    return Err(StorageError::Connection(format!(
                        "Invalid search_path schema name: {schema}"
                    )));
                }
                Some(format!("\"{schema}\", public"))
            }
            None => None,
        };

        let read_snapshot = matches!(mode, ConnectionMode::ReadSnapshot(_));
        let pool_opts = sqlx::postgres::PgPoolOptions::new()
            .max_connections(max_connections)
            .min_connections(min_connections)
            .acquire_timeout(std::time::Duration::from_secs(10))
            .idle_timeout(std::time::Duration::from_secs(300))
            // Recycle connections periodically so a connection that survived a
            // failover (or drifted server-side) can't be pinned forever.
            .max_lifetime(std::time::Duration::from_secs(1800))
            // Server-side guards run on EVERY connection, whether or not a
            // custom search_path is configured: a single hung statement or an
            // abandoned open transaction must not pin a pool slot indefinitely
            // and starve the scheduler tick loop.
            .after_connect(move |conn, _meta| {
                let search_path_value = search_path_value.clone();
                let mode = mode.clone();
                Box::pin(async move {
                    if let ConnectionMode::ReadSnapshot(used) = &mode
                        && used.swap(true, std::sync::atomic::Ordering::AcqRel)
                    {
                        return Err(sqlx::Error::Protocol(
                            "backup snapshot connection was lost; refusing a replacement view"
                                .into(),
                        ));
                    }
                    sqlx::query("SET statement_timeout = '30s'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET idle_in_transaction_session_timeout = '60s'")
                        .execute(&mut *conn)
                        .await?;
                    sqlx::query("SET orch8.native_json_protocol = 'json_text_v1'")
                        .execute(&mut *conn)
                        .await?;
                    if let Some(sp) = search_path_value {
                        sqlx::query("SELECT set_config('search_path', $1, false)")
                            .bind(&sp)
                            .execute(&mut *conn)
                            .await?;
                    }
                    if matches!(mode, ConnectionMode::ReadSnapshot(_)) {
                        use sqlx::SqlSafeStr as _;
                        use sqlx_core::transaction::TransactionManager as _;
                        // Track the transaction in SQLx without an RAII handle
                        // that would roll it back as this callback returns.
                        // The dedicated pool owns it until the socket closes.
                        sqlx::postgres::PgTransactionManager::begin(
                            conn,
                            Some("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY".into_sql_str()),
                        )
                        .await?;
                    }
                    Ok(())
                })
            });
        let pool_opts = if read_snapshot {
            pool_opts
                // Recycling must never turn a long backup into a new view.
                .idle_timeout(None)
                .max_lifetime(None)
                .before_acquire(|conn, _meta| Box::pin(async move {
                    use sqlx::Connection as _;
                    // A caller must not end the transaction and continue as
                    // an ordinary autocommit reader on the same connection.
                    if !conn.is_in_transaction() {
                        return Ok(false);
                    }
                    let intact: bool = sqlx::query_scalar("SELECT current_setting('transaction_isolation') = 'repeatable read' AND current_setting('transaction_read_only') = 'on'")
                        .fetch_one(conn).await?;
                    Ok(intact)
                }))
        } else {
            pool_opts
        };

        let pool = pool_opts
            .connect(database_url)
            .await
            .map_err(|e| StorageError::Connection(e.to_string()))?;

        Ok(Self {
            pool,
            artifact_store: None,
        })
    }

    pub const fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn run_migrations(&self) -> Result<(), StorageError> {
        // Pool connections carry the 30s `statement_timeout` /
        // 60s `idle_in_transaction_session_timeout` guards, which a migration
        // on a large table (index build, backfill) or the wait on the
        // migration advisory lock can legitimately exceed. Run on a dedicated
        // connection with the guards lifted, and detach it afterwards so the
        // relaxed session never returns to the pool.
        let mut conn = self.pool.acquire().await?.detach();
        sqlx::query("SET statement_timeout = 0")
            .execute(&mut conn)
            .await?;
        sqlx::query("SET idle_in_transaction_session_timeout = 0")
            .execute(&mut conn)
            .await?;
        migration_preflight(&mut conn).await;
        // `run_direct` rather than `run(&mut conn)`: the `Acquire<'a>` bound on
        // `run` makes every future that (transitively) awaits this one fail
        // the "`Acquire` is not general enough" Send check — e.g. the axum
        // handlers that build an engine. Same behaviour (lock, apply, unlock).
        sqlx::migrate!("../migrations")
            .run_direct(None, &mut conn, false)
            .await
            .map_err(|e| StorageError::Migration(e.to_string()))?;
        Ok(())
    }
}

/// Row estimate above which the lock-heavy migrations 103-105 are flagged.
const HEAVY_MIGRATION_ROWS: i64 = 1_000_000;

/// Warnings for pending lock-heavy migrations on a large install. Migrations
/// 103-105 build plain (non-`CONCURRENTLY`) indexes on `task_instances` /
/// `worker_tasks`, backfill `worker_tasks.tenant_id` in one transaction, and
/// drop an index under an exclusive lock. They cannot be edited once
/// released (checksums), so operators are told to pre-create the indexes
/// online instead (statements are in each migration's header comment).
fn heavy_migration_warnings(
    max_applied: i64,
    task_instances_rows: i64,
    worker_tasks_rows: i64,
) -> Vec<String> {
    let mut out = Vec::new();
    if max_applied < 103 && worker_tasks_rows >= HEAVY_MIGRATION_ROWS {
        out.push(format!(
            "migration 103 will backfill worker_tasks.tenant_id in a single transaction and build indexes on ~{worker_tasks_rows} rows; pre-create the indexes with CREATE INDEX CONCURRENTLY (see migrations/103 header) and consider upgrading in a low-traffic window"
        ));
    }
    if max_applied < 105 && task_instances_rows >= HEAVY_MIGRATION_ROWS {
        out.push(format!(
            "migrations 104/105 will build three indexes on task_instances (~{task_instances_rows} rows) and drop idx_instances_idempotency under a short exclusive lock; pre-create the indexes with CREATE INDEX CONCURRENTLY (see migrations/104 header)"
        ));
    }
    out
}

/// Best-effort, read-only check run before migrating: warn about pending
/// lock-heavy migrations on large tables. Never fails the migration.
async fn migration_preflight(conn: &mut sqlx::PgConnection) {
    let applied: Result<Option<i64>, _> =
        sqlx::query_scalar("SELECT MAX(version) FROM _sqlx_migrations WHERE success")
            .fetch_one(&mut *conn)
            .await;
    let Ok(Some(max_applied)) = applied else {
        return; // fresh database (no tracking table / nothing applied)
    };
    let mut rows = [0_i64; 2];
    for (slot, table) in ["task_instances", "worker_tasks"].into_iter().enumerate() {
        rows[slot] = sqlx::query_scalar::<_, i64>(
            "SELECT COALESCE(reltuples, 0)::bigint FROM pg_class WHERE oid = to_regclass($1)",
        )
        .bind(table)
        .fetch_optional(&mut *conn)
        .await
        .ok()
        .flatten()
        .unwrap_or(0);
    }
    for warning in heavy_migration_warnings(max_applied, rows[0], rows[1]) {
        tracing::warn!("{warning}");
    }
}

// ============================================================================
// Sub-trait 1: SequenceStore
// ============================================================================

#[async_trait]
impl crate::SequenceStore for PostgresStorage {
    async fn create_sequence(&self, seq: &SequenceDefinition) -> Result<(), StorageError> {
        sequences::create(self, seq).await
    }

    async fn get_sequence(
        &self,
        id: SequenceId,
    ) -> Result<Option<SequenceDefinition>, StorageError> {
        sequences::get(self, id).await
    }

    async fn get_sequences(
        &self,
        ids: &[SequenceId],
    ) -> Result<Vec<SequenceDefinition>, StorageError> {
        sequences::get_many(self, ids).await
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

    async fn list_embed_visible_sequences(
        &self,
        tenant_id: &TenantId,
        sub_tenant: &str,
        tenant_level: &crate::EmbedTenantLevel,
        limit: u32,
    ) -> Result<Vec<SequenceDefinition>, StorageError> {
        sequences::list_embed_visible(self, tenant_id, sub_tenant, tenant_level, limit).await
    }

    async fn count_sub_tenant_sequence_names(
        &self,
        tenant_id: &TenantId,
        sub_tenant: &str,
    ) -> Result<u64, StorageError> {
        sequences::count_sub_tenant_names(self, tenant_id, sub_tenant).await
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

    async fn replace_sequence(
        &self,
        old_id: SequenceId,
        new: &SequenceDefinition,
    ) -> Result<(), StorageError> {
        sequences::replace(self, old_id, new).await
    }

    // `pg_advisory_lock` is session-scoped: only the connection that took it
    // can release it. Check out a dedicated connection from the pool and
    // hand it to the returned guard, which spawns the unlock + connection
    // return on `Drop` -- release is no longer the caller's responsibility
    // (closes M8: a panic or early return between acquire and release used
    // to strand the pooled connection permanently). Each acquire gets its
    // own connection rather than sharing one keyed by tenant in a map, so a
    // second concurrent acquire for the same tenant can no longer clobber
    // the first's entry -- it simply blocks on `pg_advisory_lock` (bounded
    // by the connection's `statement_timeout`, currently 30s) until the
    // first guard drops, which is the real mutual-exclusion semantics this
    // lock exists for.

    async fn create_release(
        &self,
        release: &orch8_types::release::WorkflowRelease,
    ) -> Result<(), StorageError> {
        releases::create(self, release).await
    }

    async fn get_release(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError> {
        releases::get(self, id).await
    }

    async fn list_releases(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::release::WorkflowRelease>, StorageError> {
        releases::list(self, tenant_id, limit).await
    }

    async fn cas_release_state(
        &self,
        id: Uuid,
        expected: orch8_types::release::ReleaseState,
        next: orch8_types::release::ReleaseState,
        canary_percent: Option<u8>,
        canary_started_at: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<bool, StorageError> {
        releases::cas_state(self, id, expected, next, canary_percent, canary_started_at).await
    }

    async fn set_release_validation_summary(
        &self,
        id: Uuid,
        summary: &serde_json::Value,
    ) -> Result<(), StorageError> {
        releases::set_validation_summary(self, id, summary).await
    }

    async fn record_release_decision(
        &self,
        decision: &orch8_types::release::ReleaseDecision,
    ) -> Result<(), StorageError> {
        releases::record_decision(self, decision).await
    }

    async fn list_release_decisions(
        &self,
        release_id: Uuid,
    ) -> Result<Vec<orch8_types::release::ReleaseDecision>, StorageError> {
        releases::list_decisions(self, release_id).await
    }

    async fn find_routing_release_for_sequence(
        &self,
        baseline_sequence_id: SequenceId,
    ) -> Result<Option<orch8_types::release::WorkflowRelease>, StorageError> {
        releases::find_routing(self, baseline_sequence_id).await
    }

    async fn acquire_manifest_lock(
        &self,
        tenant_id: &str,
    ) -> Result<crate::ManifestLockGuard, StorageError> {
        let mut conn = self.pool.acquire().await?;
        // hashtextextended: 64-bit key — the 32-bit hashtext() could falsely
        // serialize two tenants whose ids hash-collide.
        sqlx::query("SELECT pg_advisory_lock(hashtextextended($1, 0))")
            .bind(tenant_id)
            .execute(&mut *conn)
            .await?;
        let tenant_owned = tenant_id.to_string();
        Ok(crate::ManifestLockGuard::new(move || {
            // `Drop` is synchronous, so the unlock has to be spawned. Guard
            // against being dropped outside a runtime (spawn would panic);
            // if the unlock never runs, the pooled connection still holds
            // the session lock until it is recycled, then it clears.
            if let Ok(handle) = tokio::runtime::Handle::try_current() {
                handle.spawn(async move {
                    if let Err(e) = sqlx::query("SELECT pg_advisory_unlock(hashtextextended($1, 0))")
                        .bind(&tenant_owned)
                        .execute(&mut *conn)
                        .await
                    {
                        tracing::warn!(
                            error = %e,
                            tenant_id = %tenant_owned,
                            "failed to release manifest advisory lock; it will clear when the connection resets"
                        );
                    }
                    // `conn` drops here either way, returning it to the pool.
                });
            } else {
                tracing::warn!(
                    tenant_id = %tenant_owned,
                    "manifest lock guard dropped outside a tokio runtime; \
                     advisory lock clears when the connection is recycled"
                );
            }
        }))
    }
}

// ============================================================================
// Sub-trait 2: InstanceStore
// ============================================================================

#[async_trait]
impl crate::InstanceStore for PostgresStorage {
    async fn restore_instance_snapshot(
        &self,
        snapshot: &crate::restore::InstanceSnapshot,
    ) -> Result<bool, StorageError> {
        restore::restore(self, snapshot).await
    }
    async fn create_instance(&self, instance: &TaskInstance) -> Result<(), StorageError> {
        instances::create(self, instance).await
    }

    async fn create_child_instance_claimed(
        &self,
        parent_id: InstanceId,
        child: &TaskInstance,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::create_child(self, parent_id, child, claim_token).await
    }

    async fn adopt_child_instance_claimed(
        &self,
        parent_id: InstanceId,
        child_id: InstanceId,
        expected_activation: &str,
        activation: &str,
        claim_token: DateTime<Utc>,
    ) -> Result<Option<crate::ChildAdoption>, StorageError> {
        claimed_writes::adopt_child(
            self,
            parent_id,
            child_id,
            expected_activation,
            activation,
            claim_token,
        )
        .await
    }

    async fn create_instance_admitted(
        &self,
        instance: &TaskInstance,
        max_active_instances: u64,
    ) -> Result<(), StorageError> {
        instances::create_admitted(self, instance, max_active_instances).await
    }

    async fn create_instances_batch(
        &self,
        instances: &[TaskInstance],
    ) -> Result<u64, StorageError> {
        instances::create_batch(self, instances).await
    }

    async fn create_instances_batch_admitted(
        &self,
        instances: &[TaskInstance],
        limits: &std::collections::HashMap<TenantId, u64>,
    ) -> Result<u64, StorageError> {
        instances::create_batch_admitted(self, instances, limits).await
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

    async fn get_instance_for_caller(
        &self,
        caller: InstanceId,
        tenant: &TenantId,
        target: InstanceId,
    ) -> Result<Option<TaskInstance>, StorageError> {
        instances::get_for_caller(self, caller, tenant, target).await
    }

    async fn get_mobile_instance(
        &self,
        access: &crate::MobileInstanceAccess,
        id: InstanceId,
    ) -> Result<Option<TaskInstance>, StorageError> {
        mobile_access::get(self, access, id).await
    }
    async fn cancel_mobile_instance(
        &self,
        access: &crate::MobileInstanceAccess,
        id: InstanceId,
    ) -> Result<Option<InstanceState>, StorageError> {
        mobile_access::cancel(self, access, id).await
    }

    async fn complete_mobile_step(
        &self,
        access: &crate::MobileInstanceAccess,
        expected_updated_at: DateTime<Utc>,
        context: &orch8_types::context::ExecutionContext,
        signal: &orch8_types::signal::Signal,
    ) -> Result<bool, StorageError> {
        mobile_access::complete(self, access, expected_updated_at, context, signal).await
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

    async fn has_due_higher_priority(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        priority: orch8_types::instance::Priority,
    ) -> Result<bool, StorageError> {
        instances::has_due_higher_priority(self, now, priority).await
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

    async fn conditional_update_claimed_instance_state(
        &self,
        id: InstanceId,
        claimed_at: DateTime<Utc>,
        expected_state: InstanceState,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<bool, StorageError> {
        instances::conditional_update_claimed_state(
            self,
            id,
            claimed_at,
            expected_state,
            new_state,
            next_fire_at,
        )
        .await
    }

    async fn conditional_update_instance_state_with_outbox(
        &self,
        id: InstanceId,
        expected_state: InstanceState,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
        entries: &[orch8_types::webhook_outbox::WebhookOutboxEntry],
    ) -> Result<bool, StorageError> {
        instances::conditional_update_state_with_outbox(
            self,
            id,
            expected_state,
            new_state,
            next_fire_at,
            entries,
        )
        .await
    }

    async fn conditional_update_claimed_instance_state_with_outbox(
        &self,
        id: InstanceId,
        claimed_at: DateTime<Utc>,
        expected_state: InstanceState,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
        entries: &[orch8_types::webhook_outbox::WebhookOutboxEntry],
    ) -> Result<bool, StorageError> {
        instances::conditional_update_claimed_state_with_outbox(
            self,
            id,
            claimed_at,
            expected_state,
            new_state,
            next_fire_at,
            entries,
        )
        .await
    }

    async fn update_instance_context(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
    ) -> Result<(), StorageError> {
        instances::update_context(self, id, context).await
    }

    async fn update_instance_context_cas_claimed(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        expected_updated_at: DateTime<Utc>,
        claim_token: DateTime<Utc>,
    ) -> Result<Option<bool>, StorageError> {
        claimed_writes::context_cas(self, id, context, expected_updated_at, claim_token).await
    }
    async fn merge_context_data_claimed(
        &self,
        id: InstanceId,
        key: &str,
        value: &serde_json::Value,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::merge_context(self, id, key, value, claim_token).await
    }

    async fn update_instance_context_cas(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        expected_updated_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        instances::update_context_cas(self, id, context, expected_updated_at).await
    }

    async fn ensure_instance_run_started(
        &self,
        id: InstanceId,
        run_id: &str,
        started_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        instances::ensure_run_started(self, id, run_id, started_at).await
    }

    async fn ensure_instance_run_started_claimed(
        &self,
        id: InstanceId,
        run_id: &str,
        started_at: DateTime<Utc>,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::ensure_run_started(self, id, run_id, started_at, claim_token).await
    }

    async fn reset_instance_run(&self, id: InstanceId, run_id: &str) -> Result<(), StorageError> {
        instances::reset_run(self, id, run_id).await
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

    async fn update_instance_context_externalized(
        &self,
        id: InstanceId,
        context: &orch8_types::context::ExecutionContext,
        threshold_bytes: u32,
    ) -> Result<(), StorageError> {
        instances::update_context_externalized(self, id, context, threshold_bytes).await
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

    async fn merge_instance_metadata_claimed(
        &self,
        id: InstanceId,
        patch: &serde_json::Value,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::merge_metadata(self, id, patch, claim_token).await
    }

    async fn merge_instance_metadata(
        &self,
        id: InstanceId,
        patch: &serde_json::Value,
    ) -> Result<(), StorageError> {
        instances::merge_metadata(self, id, patch).await
    }

    async fn list_instances(
        &self,
        filter: &InstanceFilter,
        pagination: &Pagination,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        instances::list(self, filter, pagination).await
    }

    async fn list_instances_keyset(
        &self,
        filter: &InstanceFilter,
        before: Option<InstanceId>,
        limit: u32,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        instances::list_keyset(self, filter, before, limit).await
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

    async fn any_instance_in_state(&self, state: InstanceState) -> Result<bool, StorageError> {
        let exists: bool =
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_instances WHERE state = $1)")
                .bind(state.to_string())
                .fetch_one(&self.pool)
                .await?;
        Ok(exists)
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

    async fn find_by_idempotency_key(
        &self,
        tenant_id: &TenantId,
        idempotency_key: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        misc::find_by_idempotency_key(self, tenant_id, idempotency_key).await
    }

    async fn count_running_by_concurrency_key(
        &self,
        tenant_id: &str,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        misc::count_running_by_concurrency_key(self, tenant_id, concurrency_key).await
    }

    async fn count_running_by_concurrency_keys(
        &self,
        keys: &[(&str, &str)],
    ) -> Result<std::collections::HashMap<(String, String), i64>, StorageError> {
        misc::count_running_by_concurrency_keys(self, keys).await
    }

    async fn concurrency_position(
        &self,
        instance_id: InstanceId,
        concurrency_key: &str,
    ) -> Result<i64, StorageError> {
        misc::concurrency_position(self, instance_id, concurrency_key).await
    }

    async fn recover_stale_instances(
        &self,
        stale_threshold: Duration,
    ) -> Result<u64, StorageError> {
        misc::recover_stale_instances(self, stale_threshold).await
    }

    async fn verify_instance_claim(
        &self,
        instance_id: InstanceId,
        tenant_id: &TenantId,
        claimed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM task_instances WHERE id=$1 AND tenant_id=$2 AND state='running' AND claimed_at=$3)")
        .bind(instance_id.into_uuid()).bind(tenant_id.as_str()).bind(claimed_at)
        .fetch_one(&self.pool).await.map_err(Into::into)
    }

    async fn heartbeat_instance(
        &self,
        instance_id: InstanceId,
        claimed_at: DateTime<Utc>,
    ) -> Result<crate::InstanceLease, StorageError> {
        misc::heartbeat_instance(self, instance_id, claimed_at).await
    }

    async fn get_child_instances(
        &self,
        parent_instance_id: InstanceId,
    ) -> Result<Vec<TaskInstance>, StorageError> {
        misc::get_child_instances(self, parent_instance_id).await
    }

    async fn get_child_for_activation(
        &self,
        parent_instance_id: InstanceId,
        block_id: &str,
        activation: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        misc::get_child_for_activation(self, parent_instance_id, block_id, activation).await
    }

    async fn get_child_for_adoption(
        &self,
        parent_instance_id: InstanceId,
        block_id: &str,
        activation_suffix: &str,
    ) -> Result<Option<TaskInstance>, StorageError> {
        misc::get_child_for_adoption(self, parent_instance_id, block_id, activation_suffix).await
    }

    async fn list_orphaned_children(&self, limit: u32) -> Result<Vec<InstanceId>, StorageError> {
        misc::list_orphaned_children(self, limit).await
    }

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
impl crate::ExecutionTreeStore for PostgresStorage {
    async fn create_execution_node(&self, node: &ExecutionNode) -> Result<(), StorageError> {
        execution_tree::create_node(self, node).await
    }

    async fn create_execution_nodes_batch(
        &self,
        nodes: &[ExecutionNode],
    ) -> Result<(), StorageError> {
        execution_tree::create_batch(self, nodes).await
    }

    async fn create_execution_nodes_batch_claimed(
        &self,
        instance_id: InstanceId,
        nodes: &[ExecutionNode],
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::create_nodes(self, instance_id, nodes, claim_token).await
    }
    async fn update_nodes_state_claimed(
        &self,
        instance_id: InstanceId,
        node_ids: &[ExecutionNodeId],
        state: NodeState,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::nodes_state(self, instance_id, node_ids, state, claim_token).await
    }
    async fn batch_activate_nodes_claimed(
        &self,
        instance_id: InstanceId,
        node_ids: &[ExecutionNodeId],
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::activate_nodes(self, instance_id, node_ids, claim_token).await
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
    async fn update_node_state_claimed(
        &self,
        instance_id: InstanceId,
        node_id: ExecutionNodeId,
        state: orch8_types::execution::NodeState,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::node(self, instance_id, node_id, state, claim_token).await
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
impl crate::OutputStore for PostgresStorage {
    async fn save_block_output(&self, output: &BlockOutput) -> Result<(), StorageError> {
        outputs::save(self, output).await
    }

    async fn save_block_output_claimed(
        &self,
        output: &BlockOutput,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::output(self, output, claim_token).await
    }

    async fn get_block_output_claimed(
        &self,
        instance_id: InstanceId,
        tenant: &TenantId,
        block_id: &BlockId,
        claim_token: DateTime<Utc>,
    ) -> Result<crate::ClaimedRead<Option<BlockOutput>>, StorageError> {
        outputs::get_claimed(self, instance_id, tenant, block_id, claim_token).await
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
        keys: &[(InstanceId, &BlockId)],
    ) -> Result<std::collections::HashMap<(InstanceId, BlockId), BlockOutput>, StorageError> {
        outputs::get_batch(self, keys).await
    }

    async fn get_latest_completed_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError> {
        outputs::get_latest_completed(self, instance_id).await
    }

    async fn get_latest_diagnostic_outputs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<BlockOutput>, StorageError> {
        outputs::get_latest_diagnostic(self, instance_id).await
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

    async fn save_output_and_conditional_transition(
        &self,
        output: &BlockOutput,
        expected_state: InstanceState,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
    ) -> Result<bool, StorageError> {
        outputs::save_output_and_conditional_transition(
            self,
            output,
            expected_state,
            new_state,
            next_fire_at,
        )
        .await
    }

    async fn save_output_and_claimed_transition(
        &self,
        output: &BlockOutput,
        new_state: InstanceState,
        next_fire_at: Option<DateTime<Utc>>,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::output_and_transition(self, output, new_state, next_fire_at, claim_token)
            .await
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

    async fn delete_block_outputs_batch_claimed(
        &self,
        instance_id: InstanceId,
        block_ids: &[BlockId],
        claim_token: DateTime<Utc>,
    ) -> Result<Option<u64>, StorageError> {
        claimed_writes::delete_outputs(self, instance_id, block_ids, claim_token).await
    }
    async fn delete_block_output_by_id_claimed(
        &self,
        instance_id: InstanceId,
        id: Uuid,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::delete_output(self, instance_id, id, claim_token).await
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

    async fn get_outputs_page(
        &self,
        instance_id: InstanceId,
        limit: u32,
        offset: u64,
    ) -> Result<Vec<BlockOutput>, StorageError> {
        outputs::get_page(self, instance_id, limit, offset).await
    }

    async fn copy_block_outputs(
        &self,
        src: InstanceId,
        dst: InstanceId,
        block_ids: &[BlockId],
    ) -> Result<u64, StorageError> {
        outputs::copy_for_blocks(self, src, dst, block_ids).await
    }
}

// ============================================================================
// Sub-trait 5: SignalStore
// ============================================================================

#[async_trait]
impl crate::SignalStore for PostgresStorage {
    async fn enqueue_signal(&self, signal: &Signal) -> Result<(), StorageError> {
        signals::enqueue(self, signal).await
    }

    async fn enqueue_signal_for_caller(
        &self,
        caller: InstanceId,
        tenant: &TenantId,
        signal: &Signal,
        claim_token: Option<DateTime<Utc>>,
    ) -> Result<bool, StorageError> {
        signals::enqueue_for_caller(self, caller, tenant, signal, claim_token).await
    }

    async fn enqueue_signal_if_active(&self, signal: &Signal) -> Result<(), StorageError> {
        signals::enqueue_if_active(self, signal).await
    }

    async fn ingest_event(
        &self,
        envelope: &orch8_types::event_correlation::EventEnvelope,
    ) -> Result<bool, StorageError> {
        events::ingest(self, envelope).await
    }

    async fn get_event(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::event_correlation::EventEnvelope>, StorageError> {
        events::get(self, id).await
    }

    async fn get_event_by_identity(
        &self,
        tenant_id: &str,
        event_name: &str,
        producer_event_id: &str,
    ) -> Result<Option<orch8_types::event_correlation::EventEnvelope>, StorageError> {
        events::get_by_identity(self, tenant_id, event_name, producer_event_id).await
    }
    async fn list_events(
        &self,
        tenant_id: &str,
        status: Option<orch8_types::event_correlation::EventStatus>,
        limit: u32,
    ) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError> {
        events::list(self, tenant_id, status, limit, 0).await
    }

    async fn list_events_page(
        &self,
        tenant_id: &str,
        status: Option<orch8_types::event_correlation::EventStatus>,
        limit: u32,
        offset: u32,
    ) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError> {
        events::list(self, tenant_id, status, limit, offset).await
    }

    async fn find_pending_events(
        &self,
        tenant_id: &str,
        event_names: &[String],
        correlation_key: &str,
    ) -> Result<Vec<orch8_types::event_correlation::EventEnvelope>, StorageError> {
        events::find_pending(self, tenant_id, event_names, correlation_key).await
    }

    async fn consume_events(
        &self,
        event_ids: &[Uuid],
        instance_id: InstanceId,
    ) -> Result<u64, StorageError> {
        events::consume(self, event_ids, instance_id).await
    }

    async fn upsert_event_wait(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
    ) -> Result<(), StorageError> {
        events::upsert_wait(self, wait).await
    }

    async fn register_event_wait(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
    ) -> Result<orch8_types::event_correlation::EventWait, StorageError> {
        events::register_wait(self, wait).await
    }
    async fn match_event_wait(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
        event_id: Option<Uuid>,
        notification: &Signal,
    ) -> Result<Option<crate::EventMatch>, StorageError> {
        events::match_wait(self, wait, event_id, notification).await
    }

    async fn upsert_event_wait_claimed(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
        claim_token: DateTime<Utc>,
    ) -> Result<Option<orch8_types::event_correlation::EventWait>, StorageError> {
        claimed_writes::event_wait(self, wait, claim_token).await
    }

    async fn delete_event_waits_for_blocks(
        &self,
        instance_id: InstanceId,
        blocks: &[String],
    ) -> Result<u64, StorageError> {
        events::delete_waits(self, instance_id, blocks).await
    }
    async fn delete_event_waits_for_blocks_claimed(
        &self,
        instance_id: InstanceId,
        blocks: &[String],
        claim_token: DateTime<Utc>,
    ) -> Result<Option<u64>, StorageError> {
        claimed_writes::delete_event_waits(self, instance_id, blocks, claim_token).await
    }
    async fn get_event_wait(
        &self,
        instance_id: InstanceId,
        block_id: &str,
    ) -> Result<Option<orch8_types::event_correlation::EventWait>, StorageError> {
        events::get_wait(self, instance_id, block_id).await
    }

    async fn find_waiting_event_waits(
        &self,
        tenant_id: &str,
        event_name: &str,
        correlation_key: &str,
    ) -> Result<Vec<orch8_types::event_correlation::EventWait>, StorageError> {
        events::find_waiting(self, tenant_id, event_name, correlation_key).await
    }

    async fn update_event_wait(
        &self,
        wait: &orch8_types::event_correlation::EventWait,
        expected_status: orch8_types::event_correlation::WaitStatus,
        expected_matched_event_ids: &[Uuid],
    ) -> Result<bool, StorageError> {
        events::update_wait(self, wait, expected_status, expected_matched_event_ids).await
    }

    async fn expire_events_before(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, StorageError> {
        events::expire_before(self, cutoff).await
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

    async fn mark_signal_delivered_claimed(
        &self,
        instance_id: InstanceId,
        signal_id: Uuid,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::deliver_signal(self, instance_id, signal_id, claim_token).await
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
impl crate::WorkerStore for PostgresStorage {
    async fn claim_worker_tasks_with_authority(
        &self,
        request: &crate::worker_claim::WorkerClaimRequest,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        request.validate()?;
        let principal = Some(&request.principal);
        if let Some(capabilities) = &request.capabilities {
            return workers::claim_matching(
                self,
                &request.handler_name,
                &request.worker_id,
                request.tenant_id.as_ref(),
                request.queue_name.as_deref(),
                crate::worker_claim::ClaimRuntime {
                    capabilities,
                    principal,
                },
                request.limit,
            )
            .await;
        }
        match (request.queue_name.as_deref(), request.tenant_id.as_ref()) {
            (Some(queue), Some(tenant)) => {
                misc::claim_worker_tasks_from_queue_for_tenant(
                    self,
                    queue,
                    &request.handler_name,
                    &request.worker_id,
                    tenant,
                    request.limit,
                    principal,
                )
                .await
            }
            (Some(queue), None) => {
                misc::claim_worker_tasks_from_queue(
                    self,
                    queue,
                    &request.handler_name,
                    &request.worker_id,
                    request.limit,
                    principal,
                )
                .await
            }
            (None, Some(tenant)) => {
                workers::claim_for_tenant(
                    self,
                    &request.handler_name,
                    &request.worker_id,
                    tenant,
                    request.limit,
                    principal,
                )
                .await
            }
            (None, None) => {
                workers::claim(
                    self,
                    &request.handler_name,
                    &request.worker_id,
                    request.limit,
                    principal,
                )
                .await
            }
        }
    }
    async fn get_worker_issued_claim(
        &self,
        tenant: Option<&TenantId>,
        task_id: Uuid,
        claim: &WorkerClaim,
    ) -> Result<Option<crate::worker_claim::WorkerIssuedClaim>, StorageError> {
        worker_claim::get(self, tenant, task_id, claim).await
    }

    async fn create_worker_task(&self, task: &WorkerTask) -> Result<(), StorageError> {
        workers::create(self, task).await
    }

    async fn prepare_worker_dispatch(
        &self,
        instance: &TaskInstance,
        block_id: &BlockId,
        selection: &crate::worker_invocation::WorkerDispatchSelection,
    ) -> Result<Option<crate::worker_invocation::WorkerDispatchFence>, StorageError> {
        worker_dispatch::prepare(self, instance, block_id, selection).await
    }

    async fn admit_worker_dispatch(
        &self,
        intent: &crate::worker_admission::WorkerDispatchIntent,
    ) -> Result<crate::worker_admission::WorkerDispatchPublication, StorageError> {
        worker_admission::create(self, intent).await
    }
    async fn get_worker_dispatch_admission(
        &self,
        tenant_id: &TenantId,
        task_id: Uuid,
    ) -> Result<Option<crate::worker_admission::WorkerDispatchAdmission>, StorageError> {
        worker_admission::get(self, tenant_id, task_id).await
    }
    async fn create_worker_task_bound(
        &self,
        task: &WorkerTask,
        fence: &crate::worker_invocation::WorkerDispatchFence,
    ) -> Result<Option<Uuid>, StorageError> {
        worker_dispatch::create(self, task, fence).await
    }

    async fn get_worker_invocation(
        &self,
        tenant_id: &TenantId,
        task_id: Uuid,
    ) -> Result<Option<crate::worker_invocation::WorkerInvocation>, StorageError> {
        worker_invocations::get(self, tenant_id, task_id).await
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
        workers::claim(self, handler_name, worker_id, limit, None).await
    }

    async fn claim_worker_tasks_for_tenant(
        &self,
        handler_name: &str,
        worker_id: &str,
        tenant_id: &orch8_types::TenantId,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::claim_for_tenant(self, handler_name, worker_id, tenant_id, limit, None).await
    }

    async fn claim_worker_tasks_matching(
        &self,
        handler_name: &str,
        worker_id: &str,
        tenant_id: Option<&orch8_types::TenantId>,
        queue_name: Option<&str>,
        capabilities: &orch8_types::continuity::RuntimeCapabilities,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::claim_matching(
            self,
            handler_name,
            worker_id,
            tenant_id,
            queue_name,
            crate::worker_claim::ClaimRuntime {
                capabilities,
                principal: None,
            },
            limit,
        )
        .await
    }

    async fn get_worker_accepted_outcome(
        &self,
        tenant_id: Option<&TenantId>,
        task_id: Uuid,
        claim: &WorkerClaim,
    ) -> Result<Option<crate::worker_outcome::WorkerAcceptedOutcome>, StorageError> {
        worker_outcome::get(self, tenant_id, task_id, claim).await
    }
    async fn claim_worker_outcome_recovery_page(
        &self,
        limit: u32,
    ) -> Result<Vec<crate::worker_outcome::WorkerOutcomeRecovery>, StorageError> {
        worker_outcome::recovery_page(self, limit).await
    }
    async fn apply_worker_outcome_integration(
        &self,
        integration: &crate::worker_outcome::WorkerOutcomeIntegration,
    ) -> Result<crate::worker_outcome::WorkerOutcomeApplication, StorageError> {
        worker_outcome::apply(self, integration).await
    }
    async fn accept_worker_completion(
        &self,
        task_id: Uuid,
        claim: &WorkerClaim,
        completion: &crate::worker_outcome::WorkerCompletion,
    ) -> Result<bool, StorageError> {
        workers::complete(self, task_id, claim, completion).await
    }

    async fn fail_worker_task(
        &self,
        task_id: Uuid,
        claim: &WorkerClaim,
        message: &str,
        retryable: bool,
    ) -> Result<bool, StorageError> {
        workers::fail(self, task_id, claim, message, retryable).await
    }

    async fn heartbeat_worker_task(
        &self,
        task_id: Uuid,
        claim: &WorkerClaim,
    ) -> Result<bool, StorageError> {
        workers::heartbeat(self, task_id, claim).await
    }

    async fn checkpoint_worker_task(
        &self,
        task_id: Uuid,
        claim: &WorkerClaim,
        expected_seq: u64,
        checkpoint: &serde_json::Value,
    ) -> Result<Option<u64>, StorageError> {
        workers::checkpoint(self, task_id, claim, expected_seq, checkpoint).await
    }

    async fn record_worker_task_attempt_event(
        &self,
        event: &WorkerTaskAttemptEvent,
    ) -> Result<(), StorageError> {
        workers::record_attempt_event(self, event).await
    }

    async fn list_worker_task_attempt_events(
        &self,
        task_id: Uuid,
        limit: u32,
    ) -> Result<Vec<WorkerTaskAttemptEvent>, StorageError> {
        workers::list_attempt_events(self, task_id, limit).await
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
        workers::retry(self, old_task_id, new_task, node_id, instance_id, fire_at).await
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

    async fn list_expired_worker_leases(
        &self,
        default_lease: Duration,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::list_expired_leases(self, default_lease, limit).await
    }

    async fn list_timed_out_worker_tasks(
        &self,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::list_timed_out(self, limit).await
    }

    async fn list_stranded_worker_dispatches(
        &self,
        created_before: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        workers::list_stranded_dispatches(self, created_before, limit).await
    }

    async fn finalize_stranded_worker_dispatch(
        &self,
        task_id: Uuid,
        attempt: u16,
        effect: Option<&crate::worker_invocation::WorkerDispatchEffect>,
        continuity_epoch: Option<u64>,
    ) -> Result<bool, StorageError> {
        workers::finalize_stranded_dispatch(self, task_id, attempt, effect, continuity_epoch).await
    }

    async fn resolve_worker_task_with_evidence(
        &self,
        resolution: &orch8_types::worker::WorkerTaskResolution,
        evidence: &crate::worker_resolution::WorkerResolutionEvidence,
    ) -> Result<bool, StorageError> {
        workers::resolve(self, resolution, evidence).await
    }
    async fn get_worker_resolution_outcome_by_id(
        &self,
        tenant: &TenantId,
        id: Uuid,
    ) -> Result<Option<crate::worker_resolution::WorkerResolutionOutcome>, StorageError> {
        worker_resolution::get_by_id(self, tenant, id).await
    }
    async fn claim_worker_resolution_recovery_page(
        &self,
        limit: u32,
    ) -> Result<Vec<crate::worker_resolution_application::WorkerResolutionRecovery>, StorageError>
    {
        worker_resolution_application::recovery_page(self, limit).await
    }
    async fn apply_worker_resolution_integration(
        &self,
        plan: &crate::worker_resolution_application::WorkerResolutionIntegration,
    ) -> Result<crate::worker_resolution_application::WorkerResolutionApplication, StorageError>
    {
        worker_resolution_application::apply(self, plan).await
    }
    async fn get_worker_resolution_outcome(
        &self,
        tenant_id: Option<&TenantId>,
        task_id: Uuid,
        claim: Option<&WorkerClaim>,
    ) -> Result<Option<crate::worker_resolution::WorkerResolutionOutcome>, StorageError> {
        worker_resolution::get(self, tenant_id, task_id, claim).await
    }

    async fn cancel_worker_tasks_for_block(
        &self,
        instance_id: Uuid,
        block_id: &str,
    ) -> Result<u64, StorageError> {
        workers::cancel_for_block(self, instance_id, block_id).await
    }

    async fn cancel_worker_tasks_for_blocks_claimed(
        &self,
        instance_id: InstanceId,
        block_ids: &[String],
        claim_token: DateTime<Utc>,
    ) -> Result<Option<u64>, StorageError> {
        claimed_writes::cancel_worker_blocks(self, instance_id, block_ids, claim_token).await
    }

    async fn cancel_worker_tasks_for_blocks(
        &self,
        instance_id: Uuid,
        block_ids: &[String],
    ) -> Result<u64, StorageError> {
        workers::cancel_for_blocks(self, instance_id, block_ids).await
    }

    async fn held_worker_tasks_for_reporter(
        &self,
        request: &crate::worker_claim::WorkerReconnectRequest,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        worker_claim::reconnect(self, request).await
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
        window_secs: Option<u32>,
    ) -> Result<orch8_types::worker_filter::WorkerTaskStats, StorageError> {
        workers::stats(self, tenant_id, window_secs).await
    }

    async fn pending_worker_task_depth(
        &self,
        limit: u32,
    ) -> Result<Vec<orch8_types::placement::QueueDepthRow>, StorageError> {
        placement::pending_depth(self, limit).await
    }

    async fn claim_worker_tasks_from_queue(
        &self,
        queue_name: &str,
        handler_name: &str,
        worker_id: &str,
        limit: u32,
    ) -> Result<Vec<WorkerTask>, StorageError> {
        misc::claim_worker_tasks_from_queue(self, queue_name, handler_name, worker_id, limit, None)
            .await
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
            None,
        )
        .await
    }

    async fn upsert_worker_registration(
        &self,
        registration: &orch8_types::worker::WorkerRegistration,
    ) -> Result<(), StorageError> {
        workers::upsert_registration(self, registration).await
    }

    async fn list_worker_registrations(
        &self,
        seen_within_secs: Option<i64>,
    ) -> Result<Vec<orch8_types::worker::WorkerRegistration>, StorageError> {
        workers::list_registrations(self, seen_within_secs).await
    }

    async fn claimed_task_counts_by_worker(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<(String, i64)>, StorageError> {
        workers::claimed_counts_by_worker(self, tenant_id).await
    }

    async fn park_webhook(
        &self,
        entry: &orch8_types::webhook_outbox::WebhookOutboxEntry,
    ) -> Result<(), StorageError> {
        webhook_outbox::park(self, entry).await
    }

    async fn list_webhook_outbox(
        &self,
        limit: u32,
    ) -> Result<Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError> {
        webhook_outbox::list(self, limit).await
    }

    async fn get_webhook_outbox(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError> {
        webhook_outbox::get(self, id).await
    }

    async fn delete_webhook_outbox(&self, id: Uuid) -> Result<(), StorageError> {
        webhook_outbox::delete(self, id).await
    }

    async fn claim_due_webhook_outbox(
        &self,
        now: DateTime<Utc>,
        limit: u32,
    ) -> Result<Vec<orch8_types::webhook_outbox::WebhookOutboxEntry>, StorageError> {
        webhook_outbox::claim_due(self, now, limit).await
    }

    async fn claim_webhook_outbox_row(
        &self,
        id: Uuid,
        claimed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        webhook_outbox::claim_row(self, id, claimed_at).await
    }

    async fn fail_webhook_outbox_attempt(
        &self,
        id: Uuid,
        last_error: &str,
        next_attempt_at: Option<DateTime<Utc>>,
    ) -> Result<(), StorageError> {
        webhook_outbox::fail_attempt(self, id, last_error, next_attempt_at).await
    }

    async fn fail_webhook_outbox_attempt_fenced(
        &self,
        id: Uuid,
        claimed_at: DateTime<Utc>,
        last_error: &str,
        next_attempt_at: Option<DateTime<Utc>>,
    ) -> Result<bool, StorageError> {
        webhook_outbox::fail_attempt_fenced(self, id, claimed_at, last_error, next_attempt_at).await
    }

    async fn complete_webhook_outbox_claim(
        &self,
        id: Uuid,
        claimed_at: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        webhook_outbox::complete_claim(self, id, claimed_at).await
    }

    async fn recover_stale_webhook_claims(
        &self,
        stale_before: DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        webhook_outbox::recover_stale(self, stale_before).await
    }

    async fn delete_parked_webhook_outbox_before(
        &self,
        cutoff: DateTime<Utc>,
    ) -> Result<u64, StorageError> {
        webhook_outbox::delete_parked_before(self, cutoff).await
    }

    async fn record_webhook_attempt(
        &self,
        attempt: &orch8_types::webhook_delivery::WebhookDeliveryAttempt,
    ) -> Result<(), StorageError> {
        webhook_deliveries::record(self, attempt).await
    }

    async fn list_webhook_deliveries(
        &self,
        filter: &orch8_types::webhook_delivery::DeliveryFilter,
        limit: u32,
    ) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliverySummary>, StorageError> {
        webhook_deliveries::list_deliveries(self, filter, limit).await
    }

    async fn get_webhook_delivery_attempts(
        &self,
        delivery_id: Uuid,
    ) -> Result<Vec<orch8_types::webhook_delivery::WebhookDeliveryAttempt>, StorageError> {
        webhook_deliveries::get_attempts(self, delivery_id).await
    }

    async fn delete_webhook_attempts_before(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
    ) -> Result<u64, StorageError> {
        webhook_deliveries::delete_before(self, cutoff).await
    }

    async fn create_queue_routing_rule(
        &self,
        rule: &orch8_types::queue_routing::QueueRoutingRule,
    ) -> Result<(), StorageError> {
        queue_routing::create(self, rule).await
    }

    async fn list_queue_routing_rules(
        &self,
        tenant_id: Option<&TenantId>,
        handler_name: Option<&str>,
    ) -> Result<Vec<orch8_types::queue_routing::QueueRoutingRule>, StorageError> {
        queue_routing::list(self, tenant_id, handler_name).await
    }

    async fn get_queue_routing_rule(
        &self,
        id: Uuid,
    ) -> Result<Option<orch8_types::queue_routing::QueueRoutingRule>, StorageError> {
        queue_routing::get(self, id).await
    }

    async fn delete_queue_routing_rule(&self, id: Uuid) -> Result<(), StorageError> {
        queue_routing::delete(self, id).await
    }

    async fn enqueue_worker_command(
        &self,
        command: &orch8_types::worker::WorkerCommand,
    ) -> Result<(), StorageError> {
        worker_commands::enqueue(self, command).await
    }

    async fn list_worker_commands(
        &self,
        worker_id: &str,
    ) -> Result<Vec<orch8_types::worker::WorkerCommand>, StorageError> {
        worker_commands::list(self, worker_id).await
    }

    async fn delete_worker_command(&self, id: Uuid) -> Result<(), StorageError> {
        worker_commands::delete(self, id).await
    }

    async fn upsert_worker_version_pin(
        &self,
        pin: &orch8_types::worker::WorkerVersionPin,
    ) -> Result<(), StorageError> {
        worker_version_pins::upsert(self, pin).await
    }

    async fn get_worker_version_pin(
        &self,
        tenant_id: &str,
        handler_name: &str,
    ) -> Result<Option<orch8_types::worker::WorkerVersionPin>, StorageError> {
        worker_version_pins::get(self, tenant_id, handler_name).await
    }

    async fn list_worker_version_pins(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<orch8_types::worker::WorkerVersionPin>, StorageError> {
        worker_version_pins::list(self, tenant_id).await
    }

    async fn delete_worker_version_pin(
        &self,
        tenant_id: &str,
        handler_name: &str,
    ) -> Result<(), StorageError> {
        worker_version_pins::delete(self, tenant_id, handler_name).await
    }

    async fn upsert_queue_dispatch(
        &self,
        config: &orch8_types::queue_dispatch::QueueDispatchConfig,
    ) -> Result<(), StorageError> {
        queue_dispatch::upsert(self, config).await
    }

    async fn set_queue_dispatch(
        &self,
        config: &orch8_types::queue_dispatch::QueueDispatchConfig,
        preserve_secret: bool,
    ) -> Result<orch8_types::queue_dispatch::QueueDispatchConfig, StorageError> {
        queue_dispatch::set(self, config, preserve_secret).await
    }

    async fn get_queue_dispatch(
        &self,
        tenant_id: &str,
        queue_name: &str,
    ) -> Result<Option<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError> {
        queue_dispatch::get(self, tenant_id, queue_name).await
    }

    async fn list_queue_dispatch(
        &self,
        tenant_id: Option<&str>,
    ) -> Result<Vec<orch8_types::queue_dispatch::QueueDispatchConfig>, StorageError> {
        queue_dispatch::list(self, tenant_id).await
    }

    async fn delete_queue_dispatch(
        &self,
        tenant_id: &str,
        queue_name: &str,
    ) -> Result<(), StorageError> {
        queue_dispatch::delete(self, tenant_id, queue_name).await
    }

    async fn append_step_logs(
        &self,
        instance_id: InstanceId,
        block_id: &BlockId,
        entries: &[orch8_types::step_log::StepLogEntry],
    ) -> Result<(), StorageError> {
        step_logs::append(self, instance_id, block_id, entries).await
    }

    async fn list_step_logs(
        &self,
        instance_id: InstanceId,
    ) -> Result<Vec<orch8_types::step_log::StepLog>, StorageError> {
        step_logs::list(self, instance_id).await
    }
}

// ============================================================================
// Sub-trait 7: SchedulingStore
// ============================================================================

#[async_trait]
impl crate::SchedulingStore for PostgresStorage {
    async fn create_cron_schedule(&self, schedule: &CronSchedule) -> Result<(), StorageError> {
        cron::create(self, schedule).await
    }

    async fn get_cron_schedule(&self, id: Uuid) -> Result<Option<CronSchedule>, StorageError> {
        cron::get(self, id).await
    }

    async fn list_all_cron_schedules(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<CronSchedule>, StorageError> {
        cron::list_all(self, tenant_id).await
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

    async fn record_cron_skip(
        &self,
        id: Uuid,
        now: DateTime<Utc>,
        next_fire_at: DateTime<Utc>,
    ) -> Result<(), StorageError> {
        cron::record_skip(self, id, now, next_fire_at).await
    }

    async fn active_instance_ids_for_cron(
        &self,
        cron_id: Uuid,
        limit: u32,
    ) -> Result<Vec<InstanceId>, StorageError> {
        cron::active_instance_ids_for_cron(self, cron_id, limit).await
    }

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

    async fn get_placement_policies(
        &self,
        tenant_id: &TenantId,
    ) -> Result<orch8_types::placement::PlacementPolicies, StorageError> {
        placement::get_policies(self, tenant_id).await
    }

    async fn put_placement_policies(
        &self,
        tenant_id: &TenantId,
        policies: &orch8_types::placement::PlacementPolicies,
    ) -> Result<(), StorageError> {
        placement::put_policies(self, tenant_id, policies).await
    }

    async fn upsert_rate_budget(
        &self,
        budget: &orch8_types::placement::RateBudget,
    ) -> Result<orch8_types::placement::RateBudget, StorageError> {
        placement::upsert_budget(self, budget).await
    }

    async fn list_rate_budgets(
        &self,
        tenant_id: &TenantId,
    ) -> Result<Vec<orch8_types::placement::RateBudget>, StorageError> {
        placement::list_budgets(self, tenant_id).await
    }

    async fn delete_rate_budget(
        &self,
        tenant_id: &TenantId,
        key: &str,
    ) -> Result<bool, StorageError> {
        placement::delete_budget(self, tenant_id, key).await
    }

    async fn take_rate_budget_token(
        &self,
        tenant_id: &TenantId,
        key: &str,
        now: DateTime<Utc>,
    ) -> Result<orch8_types::placement::RateBudgetCheck, StorageError> {
        placement::take_budget_token(self, tenant_id, key, now).await
    }
}

// ============================================================================
// Sub-trait 8: AdminStore
// ============================================================================

#[async_trait]
impl crate::AdminStore for PostgresStorage {
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

    async fn create_plugin(
        &self,
        plugin: &orch8_types::plugin::PluginDef,
    ) -> Result<(), StorageError> {
        plugins::create(self, plugin).await
    }

    async fn get_plugin(
        &self,
        tenant_id: Option<&TenantId>,
        name: &str,
    ) -> Result<Option<orch8_types::plugin::PluginDef>, StorageError> {
        plugins::get(self, tenant_id, name).await
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

    async fn create_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        triggers::create(self, trigger).await
    }

    async fn get_trigger(
        &self,
        tenant_id: Option<&TenantId>,
        slug: &str,
    ) -> Result<Option<orch8_types::trigger::TriggerDef>, StorageError> {
        triggers::get(self, tenant_id, slug).await
    }

    async fn list_triggers(
        &self,
        tenant_id: Option<&TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::trigger::TriggerDef>, StorageError> {
        triggers::list(self, tenant_id, limit).await
    }

    async fn list_all_triggers(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<orch8_types::trigger::TriggerDef>, StorageError> {
        triggers::list_all(self, tenant_id).await
    }

    async fn update_trigger(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
    ) -> Result<(), StorageError> {
        triggers::update(self, trigger).await
    }

    async fn update_trigger_cas(
        &self,
        trigger: &orch8_types::trigger::TriggerDef,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        triggers::update_cas(self, trigger, expected_updated_at).await
    }
    async fn delete_trigger(&self, slug: &str) -> Result<(), StorageError> {
        triggers::delete(self, slug).await
    }

    async fn claim_webhook_nonce(
        &self,
        slug: &str,
        nonce: &str,
        expires_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        triggers::claim_nonce(self, slug, nonce, expires_at).await
    }

    async fn create_approval_tokens(
        &self,
        tokens: &[orch8_types::approval_link::ApprovalActionToken],
    ) -> Result<(), StorageError> {
        approval_tokens::create(self, tokens).await
    }

    async fn get_approval_token(
        &self,
        token_hash: &str,
    ) -> Result<Option<orch8_types::approval_link::ApprovalActionToken>, StorageError> {
        approval_tokens::get(self, token_hash).await
    }

    async fn consume_approval_token(
        &self,
        token_hash: &str,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<Option<orch8_types::approval_link::ApprovalActionToken>, StorageError> {
        approval_tokens::consume(self, token_hash, now).await
    }

    async fn has_live_approval_tokens(
        &self,
        instance_id: orch8_types::ids::InstanceId,
        block_id: &orch8_types::ids::BlockId,
        channel: orch8_types::approval_link::ApprovalChannel,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        approval_tokens::has_live(self, instance_id, block_id, channel, now).await
    }

    async fn create_progress_share(
        &self,
        share: &orch8_types::progress_share::ProgressShare,
    ) -> Result<(), StorageError> {
        progress_shares::create(self, share).await
    }

    async fn get_progress_share_by_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<orch8_types::progress_share::ProgressShare>, StorageError> {
        progress_shares::get_by_hash(self, token_hash).await
    }

    async fn list_progress_shares(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        instance_id: orch8_types::ids::InstanceId,
    ) -> Result<Vec<orch8_types::progress_share::ProgressShare>, StorageError> {
        progress_shares::list(self, tenant_id, instance_id).await
    }

    async fn revoke_progress_share(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        instance_id: orch8_types::ids::InstanceId,
        share_id: uuid::Uuid,
        now: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        progress_shares::revoke(self, tenant_id, instance_id, share_id, now).await
    }

    async fn create_alert_rule(
        &self,
        rule: &orch8_types::alert::AlertRule,
    ) -> Result<(), StorageError> {
        alert_rules::create(self, rule).await
    }

    async fn get_alert_rule(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
        id: uuid::Uuid,
    ) -> Result<Option<orch8_types::alert::AlertRule>, StorageError> {
        alert_rules::get(self, tenant_id, id).await
    }

    async fn list_alert_rules(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
        limit: u32,
    ) -> Result<Vec<orch8_types::alert::AlertRule>, StorageError> {
        alert_rules::list(self, tenant_id, limit).await
    }

    async fn list_all_alert_rules(
        &self,
        tenant_id: Option<&orch8_types::ids::TenantId>,
    ) -> Result<Vec<orch8_types::alert::AlertRule>, StorageError> {
        alert_rules::list_all(self, tenant_id).await
    }

    async fn update_alert_rule(
        &self,
        rule: &orch8_types::alert::AlertRule,
    ) -> Result<bool, StorageError> {
        alert_rules::update(self, rule).await
    }

    async fn delete_alert_rule(
        &self,
        tenant_id: &orch8_types::ids::TenantId,
        id: uuid::Uuid,
    ) -> Result<bool, StorageError> {
        alert_rules::delete(self, tenant_id, id).await
    }

    async fn get_alert_rule_state(
        &self,
        rule_id: uuid::Uuid,
    ) -> Result<Option<orch8_types::alert::AlertRuleState>, StorageError> {
        alert_rules::get_state(self, rule_id).await
    }

    async fn cas_alert_rule_state(
        &self,
        state: &orch8_types::alert::AlertRuleState,
        expected_version: i64,
    ) -> Result<bool, StorageError> {
        alert_rules::cas_state(self, state, expected_version).await
    }

    async fn get_trigger_poll_state(
        &self,
        slug: &str,
    ) -> Result<Option<orch8_types::trigger::TriggerPollState>, StorageError> {
        triggers::get_poll_state(self, slug).await
    }

    async fn upsert_trigger_poll_state(
        &self,
        state: &orch8_types::trigger::TriggerPollState,
    ) -> Result<(), StorageError> {
        triggers::upsert_poll_state(self, state).await
    }

    async fn try_acquire_trigger_poll_lease(
        &self,
        slug: &str,
        owner: &str,
        now: chrono::DateTime<chrono::Utc>,
        lease_until: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        triggers::try_acquire_poll_lease(self, slug, owner, now, lease_until).await
    }

    async fn create_credential(
        &self,
        credential: &orch8_types::credential::CredentialDef,
    ) -> Result<(), StorageError> {
        credentials::create(self, credential).await
    }

    async fn get_credential(
        &self,
        tenant_id: Option<&TenantId>,
        id: &str,
    ) -> Result<Option<orch8_types::credential::CredentialDef>, StorageError> {
        credentials::get(self, tenant_id, id).await
    }

    async fn get_credential_for_claim(
        &self,
        instance_id: InstanceId,
        tenant_id: &TenantId,
        token: DateTime<Utc>,
        id: &str,
    ) -> Result<crate::ClaimedRead<Option<orch8_types::credential::CredentialDef>>, StorageError>
    {
        credentials::get_for_claim(self, instance_id, tenant_id, token, id).await
    }

    async fn list_all_credentials(
        &self,
        tenant_id: Option<&TenantId>,
    ) -> Result<Vec<orch8_types::credential::CredentialDef>, StorageError> {
        credentials::list_all(self, tenant_id).await
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

    async fn update_credential_cas(
        &self,
        credential: &orch8_types::credential::CredentialDef,
        expected_updated_at: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        credentials::update_cas(self, credential, expected_updated_at).await
    }

    async fn claim_credential_refresh(
        &self,
        id: &str,
        now: chrono::DateTime<chrono::Utc>,
        lease_until: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        credentials::claim_refresh(self, id, now, lease_until).await
    }

    async fn record_credential_refresh_failure(
        &self,
        id: &str,
        now: chrono::DateTime<chrono::Utc>,
        backoff: fn(u32) -> chrono::TimeDelta,
    ) -> Result<u32, StorageError> {
        credentials::record_refresh_failure(self, id, now, backoff).await
    }

    async fn reset_credential_refresh_failures(&self, id: &str) -> Result<(), StorageError> {
        credentials::reset_refresh_failures(self, id).await
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

    async fn list_tenant_changes(
        &self,
        tenant_id: &TenantId,
        after: Option<orch8_types::audit::ChangeCursor>,
        limit: u32,
    ) -> Result<Vec<orch8_types::audit::AuditLogEntry>, StorageError> {
        audit::list_changes(self, tenant_id, after, limit).await
    }

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
        rollback::create_rollback_policy(
            self,
            tenant_id,
            sequence_name,
            error_rate_threshold,
            time_window_secs,
            cooldown_secs,
            confirmation_window_secs,
            webhook_url,
        )
        .await
    }

    async fn get_rollback_policy(
        &self,
        tenant_id: &str,
        sequence_name: &str,
    ) -> Result<Option<orch8_types::rollback::RollbackPolicy>, StorageError> {
        rollback::get_rollback_policy(self, tenant_id, sequence_name).await
    }

    async fn list_rollback_policies(
        &self,
        tenant_id: Option<&str>,
        limit: u32,
    ) -> Result<Vec<orch8_types::rollback::RollbackPolicy>, StorageError> {
        rollback::list_rollback_policies(self, tenant_id, limit).await
    }

    async fn delete_rollback_policy(
        &self,
        tenant_id: &str,
        sequence_name: &str,
    ) -> Result<(), StorageError> {
        rollback::delete_rollback_policy(self, tenant_id, sequence_name).await
    }

    async fn record_rollback(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        error_rate: f64,
        threshold: f64,
        reason: &str,
    ) -> Result<(), StorageError> {
        rollback::record_rollback(
            self,
            tenant_id,
            sequence_name,
            error_rate,
            threshold,
            reason,
        )
        .await
    }

    async fn query_error_stats(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        window_secs: i64,
    ) -> Result<(i64, i64), StorageError> {
        rollback::query_error_stats(self, tenant_id, sequence_name, window_secs).await
    }

    async fn set_rollback_min_samples(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        min_samples: i32,
    ) -> Result<(), StorageError> {
        rollback::set_rollback_min_samples(self, tenant_id, sequence_name, min_samples).await
    }

    async fn query_error_rate(
        &self,
        tenant_id: &str,
        sequence_name: &str,
        window_secs: i64,
    ) -> Result<Option<f64>, StorageError> {
        rollback::query_error_rate(self, tenant_id, sequence_name, window_secs).await
    }

    async fn list_rollback_history(
        &self,
        tenant_id: Option<&str>,
        sequence_name: Option<&str>,
        limit: u32,
    ) -> Result<Vec<orch8_types::rollback::RollbackHistory>, StorageError> {
        rollback::list_rollback_history(self, tenant_id, sequence_name, limit).await
    }

    async fn ping(&self) -> Result<(), StorageError> {
        misc::ping(self).await
    }
}

// TelemetryStore is implemented in telemetry.rs
// ============================================================================
// Sub-trait 10: ResourceStore
// ============================================================================

#[async_trait]
impl crate::ResourceStore for PostgresStorage {
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

    async fn put_artifact_with_id(
        &self,
        instance_id: InstanceId,
        artifact_id: Uuid,
        content_type: &str,
        bytes: bytes::Bytes,
    ) -> Result<orch8_types::artifact::ArtifactRef, StorageError> {
        crate::artifacts::require_store(self.artifact_store.as_ref())?
            .put_with_id(&instance_id.to_string(), artifact_id, content_type, bytes)
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

    async fn list_instance_gc_candidates(
        &self,
        cutoff: chrono::DateTime<chrono::Utc>,
        limit: u32,
    ) -> Result<Vec<InstanceId>, StorageError> {
        instances::list_instance_gc_candidates(self, cutoff, limit).await
    }

    async fn purge_terminal_instances(&self, ids: &[InstanceId]) -> Result<u64, StorageError> {
        instances::purge_terminal_instances(self, ids).await
    }

    async fn delete_expired_auxiliary(
        &self,
        retention: &crate::AuxiliaryRetention,
        limit: u32,
    ) -> Result<u64, StorageError> {
        misc::delete_expired_auxiliary(self, retention, limit).await
    }

    async fn set_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.set_instance_kv_impl(instance_id, key, value).await
    }

    async fn set_instance_kv_claimed(
        &self,
        instance_id: InstanceId,
        key: &str,
        value: &serde_json::Value,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::cache(self, instance_id, key, value, claim_token).await
    }

    async fn get_instance_kv_claimed(
        &self,
        instance_id: InstanceId,
        tenant: &TenantId,
        key: &str,
        claim_token: DateTime<Utc>,
    ) -> Result<crate::ClaimedRead<Option<serde_json::Value>>, StorageError> {
        self.get_instance_kv_claimed_impl(instance_id, tenant, key, claim_token)
            .await
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

    async fn delete_instance_kv_claimed(
        &self,
        instance_id: InstanceId,
        key: &str,
        claim_token: chrono::DateTime<chrono::Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::delete_kv(self, instance_id, key, claim_token).await
    }

    async fn delete_instance_kv(
        &self,
        instance_id: InstanceId,
        key: &str,
    ) -> Result<(), StorageError> {
        self.delete_instance_kv_impl(instance_id, key).await
    }

    async fn delete_instance_kv_batch(
        &self,
        instance_id: InstanceId,
        keys: &[String],
    ) -> Result<(), StorageError> {
        self.delete_instance_kv_batch_impl(instance_id, keys).await
    }

    async fn set_shared_knowledge(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
        value: &serde_json::Value,
    ) -> Result<(), StorageError> {
        self.set_shared_knowledge_impl(tenant_id, namespace, key, value)
            .await
    }

    async fn list_shared_knowledge(
        &self,
        tenant_id: &str,
        namespace: &str,
        limit: u32,
    ) -> Result<std::collections::HashMap<String, serde_json::Value>, StorageError> {
        self.list_shared_knowledge_impl(tenant_id, namespace, limit)
            .await
    }

    async fn get_shared_knowledge(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        self.get_shared_knowledge_impl(tenant_id, namespace, key)
            .await
    }

    async fn delete_shared_knowledge(
        &self,
        tenant_id: &str,
        namespace: &str,
        key: &str,
    ) -> Result<(), StorageError> {
        self.delete_shared_knowledge_impl(tenant_id, namespace, key)
            .await
    }

    async fn delete_shared_knowledge_batch(
        &self,
        tenant_id: &str,
        namespace: &str,
        keys: &[String],
    ) -> Result<(), StorageError> {
        self.delete_shared_knowledge_batch_impl(tenant_id, namespace, keys)
            .await
    }

    async fn save_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
    ) -> Result<(), StorageError> {
        externalized::save(self, instance_id, ref_key, payload).await
    }

    async fn save_externalized_state_claimed(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
        payload: &serde_json::Value,
        claim_token: DateTime<Utc>,
    ) -> Result<bool, StorageError> {
        claimed_writes::externalized(self, instance_id, ref_key, payload, claim_token).await
    }

    async fn batch_save_externalized_state(
        &self,
        instance_id: InstanceId,
        entries: &[(String, serde_json::Value)],
    ) -> Result<(), StorageError> {
        externalized::batch_save(self, instance_id, entries).await
    }

    async fn list_externalized_entries(
        &self,
        instance_id: InstanceId,
        after: Option<&str>,
        limit: u32,
    ) -> Result<Vec<crate::externalizing::ExternalizedEntry>, StorageError> {
        externalized_snapshot::list(self, instance_id, after, limit).await
    }
    async fn restore_externalized_entry(
        &self,
        entry: &crate::externalizing::ExternalizedEntry,
    ) -> Result<(), StorageError> {
        externalized_snapshot::restore(self, entry).await
    }
    async fn get_externalized_state(
        &self,
        instance_id: InstanceId,
        ref_key: &str,
    ) -> Result<Option<serde_json::Value>, StorageError> {
        externalized::get(self, instance_id, ref_key).await
    }

    async fn batch_get_externalized_state(
        &self,
        refs: &[(InstanceId, String)],
    ) -> Result<std::collections::HashMap<(InstanceId, String), serde_json::Value>, StorageError>
    {
        externalized::batch_get(self, refs).await
    }

    async fn delete_externalized_state(&self, ref_key: &str) -> Result<(), StorageError> {
        externalized::delete(self, ref_key).await
    }

    async fn delete_expired_externalized_state(&self, limit: u32) -> Result<u64, StorageError> {
        externalized::delete_expired(self, limit).await
    }

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

// MobileSyncStore is implemented in mobile_sync.rs

/// Finish before returning: dropping a transaction only queues its rollback.
async fn finish_transaction<T>(
    tx: sqlx::Transaction<'_, sqlx::Postgres>,
    result: Result<T, StorageError>,
) -> Result<T, StorageError> {
    match result {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(error) => {
            let _ = tx.rollback().await;
            Err(error)
        }
    }
}

#[cfg(test)]
mod migration_preflight_tests {
    use super::*;

    #[test]
    fn large_tables_with_pending_heavy_migrations_warn() {
        let w = heavy_migration_warnings(102, 5_000_000, 5_000_000);
        assert_eq!(w.len(), 2);
        assert!(w[0].contains("migration 103") && w[1].contains("104/105"));
    }

    #[test]
    fn small_or_already_migrated_installs_stay_quiet() {
        assert!(heavy_migration_warnings(102, 1000, 1000).is_empty());
        assert!(heavy_migration_warnings(111, 9_000_000, 9_000_000).is_empty());
        assert_eq!(heavy_migration_warnings(103, 9_000_000, 9_000_000).len(), 1);
    }
}
