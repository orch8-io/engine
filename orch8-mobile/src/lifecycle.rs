//! Instance lifecycle management: start, cancel, complete, query, and GC.
//!
//! Extracted from `MobileEngine` to separate instance orchestration from
//! the `UniFFI` facade and tick loop.

use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, info, warn};

/// How long a finished instance (and its outputs, tree and logs) is kept on
/// the device before the periodic GC deletes it (L1-06): 30 days.
pub const TERMINAL_INSTANCE_RETENTION_SECS: u64 = 2_592_000;

/// Terminal instances deleted per GC pass on the device.
const TERMINAL_PURGE_BATCH: u32 = 200;

use orch8_storage::StorageBackend;
#[cfg(test)]
use orch8_types::ids::TenantId;
use orch8_types::ids::{InstanceId, Namespace};
use orch8_types::instance::{InstanceState, TaskInstance};

use crate::error::MobileError;
use crate::storage::{ActiveInstanceProjection, MobileStorage};

/// Manages instance lifecycle operations: creation, cancellation,
/// completion, querying, and garbage collection.
pub struct InstanceLifecycleManager {
    storage: Arc<dyn StorageBackend>,
    mobile_storage: Arc<MobileStorage>,
    max_concurrent_instances: u32,
    /// When `compact_storage` last ran (MC-07).
    last_compaction: std::sync::Mutex<Option<std::time::Instant>>,
    /// When finished delegations were last swept (MC-09).
    last_delegation_sweep: std::sync::Mutex<Option<std::time::Instant>>,
}

/// Finished-delegation retention sweeps run at most this often.
const DELEGATION_SWEEP_INTERVAL: Duration = Duration::from_secs(60 * 60);

/// Compaction (incremental vacuum + WAL truncate) runs when a purge freed rows,
/// otherwise at most this often.
const IDLE_COMPACTION_INTERVAL: Duration = Duration::from_secs(60 * 60);

impl InstanceLifecycleManager {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        mobile_storage: Arc<MobileStorage>,
        max_concurrent_instances: u32,
    ) -> Self {
        Self {
            storage,
            mobile_storage,
            max_concurrent_instances,
            last_compaction: std::sync::Mutex::new(None),
            last_delegation_sweep: std::sync::Mutex::new(None),
        }
    }

    /// MC-09: delete delegations resolved/abandoned more than
    /// [`crate::delegation::FINISHED_RETENTION`] ago, at most hourly. A
    /// missing table (delegation never initialized) is not an error.
    async fn sweep_finished_delegations(&self, now: std::time::Instant) {
        {
            let mut last = self
                .last_delegation_sweep
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if last.is_some_and(|at| now.duration_since(at) < DELEGATION_SWEEP_INTERVAL) {
                return;
            }
            *last = Some(now);
        }
        let cutoff = chrono::Utc::now() - crate::delegation::FINISHED_RETENTION;
        match crate::delegation::purge_finished(self.mobile_storage.pool(), cutoff).await {
            Ok(0) => {}
            Ok(removed) => info!(removed, "purged finished delegations past retention"),
            Err(error) => debug!(%error, "finished-delegation sweep skipped"),
        }
    }

    /// MC-07: whether the GC pass should compact storage now. Always after a
    /// purge that deleted rows; otherwise only once per
    /// [`IDLE_COMPACTION_INTERVAL`] (a 60 s no-op `wal_checkpoint(TRUNCATE)` +
    /// `incremental_vacuum` wakes the disk for nothing).
    fn compaction_due(&self, deleted: u64, now: std::time::Instant) -> bool {
        let mut last = self
            .last_compaction
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let due =
            deleted > 0 || last.is_none_or(|at| now.duration_since(at) >= IDLE_COMPACTION_INTERVAL);
        if due {
            *last = Some(now);
        }
        due
    }

    pub(crate) fn mobile_storage(&self) -> &MobileStorage {
        &self.mobile_storage
    }

    /// Remove stale durable dedup mappings at startup. Every start consults
    /// storage, so separate engine handles cannot retain stale cached IDs.
    pub async fn hydrate_dedup(&self) -> Result<(), MobileError> {
        if let Err(error) = self.mobile_storage.prune_stale_dedup().await {
            warn!(%error, "failed to prune stale dedup keys");
        }
        Ok(())
    }

    /// Start a new workflow instance.
    #[allow(clippy::too_many_lines)]
    pub async fn start(
        &self,
        sequence_name: &str,
        input: &str,
        dedup_key: Option<&str>,
    ) -> Result<String, MobileError> {
        if let Some(key) = dedup_key
            && let Some(id) = self.mobile_storage.active_dedup_instance(key).await?
        {
            return Ok(id);
        }

        let tenant = crate::mobile_tenant_id();
        let ns = Namespace::new("default");
        let seq = self
            .storage
            .get_sequence_by_name(&tenant, &ns, sequence_name, None)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("sequence '{sequence_name}' not found"),
            })?;

        let input_data: serde_json::Value =
            serde_json::from_str(input).map_err(|e| MobileError::InvalidInput {
                message: format!("invalid input JSON: {e}"),
            })?;

        let instance_id = InstanceId::new();
        let now = chrono::Utc::now();

        let context = orch8_types::context::ExecutionContext {
            data: input_data,
            ..orch8_types::context::ExecutionContext::default()
        };

        let instance = TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: instance_id,
            sequence_id: seq.id,
            tenant_id: tenant.clone(),
            namespace: ns,
            state: InstanceState::Scheduled,
            next_fire_at: Some(now),
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".to_string(),
            metadata: serde_json::json!({}),
            context,
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: dedup_key.map(std::string::ToString::to_string),
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: now,
            updated_at: now,
        };

        let id_str = self
            .mobile_storage
            .start_legacy_instance(instance, dedup_key, self.max_concurrent_instances)
            .await?;

        info!(instance_id = %id_str, sequence = %sequence_name, "started mobile instance");
        Ok(id_str)
    }

    /// Cancel a running instance.
    pub async fn cancel_instance(&self, instance_id: &str) -> Result<(), MobileError> {
        let id = parse_instance_id(instance_id)?;

        let final_state = self
            .storage
            .cancel_mobile_instance(&orch8_storage::MobileInstanceAccess::Legacy, id)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("instance '{instance_id}' not found"),
            })?;
        if final_state != InstanceState::Cancelled {
            debug!(instance_id, state = %final_state, "cancel ignored: instance already terminal");
            return Ok(());
        }

        if let Err(error) = self.mobile_storage.remove_dedup(instance_id).await {
            warn!(instance_id, %error, "failed to remove persisted mobile dedup entry");
        }

        info!(instance_id = %instance_id, "cancelled mobile instance");
        Ok(())
    }

    /// Complete a step that transitioned to Waiting state.
    ///
    /// Enqueues a `human_input:{step_name}` signal so the engine's normal
    /// tick loop picks it up via `check_human_input`, which validates the
    /// value against `effective_choices` and stores it under the step's
    /// `store_as` key in `context.data`.
    pub async fn complete_step(
        &self,
        instance_id: &str,
        step_name: &str,
        output: &str,
    ) -> Result<(), MobileError> {
        let id = parse_instance_id(instance_id)?;

        let inst = self
            .storage
            .get_mobile_instance(&orch8_storage::MobileInstanceAccess::Legacy, id)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("instance '{instance_id}' not found"),
            })?;

        if inst.state != InstanceState::Waiting {
            return Err(MobileError::InvalidInput {
                message: format!("instance is in {:?} state, expected Waiting", inst.state),
            });
        }

        let payload: serde_json::Value = serde_json::from_str(output)?;

        // Merge output into instance context so it's immediately available.
        let mut context = inst.context;
        if let serde_json::Value::Object(map) = &payload {
            for (k, v) in map {
                context.data[k] = v.clone();
            }
        } else {
            context.data[step_name] = payload.clone();
        }
        // CAS on `updated_at` (L3-14): a concurrent tick's context merge or a
        // cancel since the read must not be overwritten.
        if !self
            .storage
            .update_instance_context_cas(id, &context, inst.updated_at)
            .await?
        {
            return Err(MobileError::InvalidInput {
                message: format!("instance '{instance_id}' changed concurrently; retry"),
            });
        }

        let signal = orch8_types::signal::Signal {
            id: uuid::Uuid::now_v7(),
            instance_id: id,
            signal_type: orch8_types::signal::SignalType::Custom(format!(
                "human_input:{step_name}"
            )),
            payload,
            delivered: false,
            created_at: chrono::Utc::now(),
            delivered_at: None,
        };
        self.storage.enqueue_signal(&signal).await?;
        // Only wake it if it is still parked; a cancel that won the race
        // stands (the signal is then simply never consumed).
        self.storage
            .conditional_update_instance_state(
                id,
                InstanceState::Waiting,
                InstanceState::Scheduled,
                Some(chrono::Utc::now()),
            )
            .await?;

        debug!(instance_id = %instance_id, step_name = %step_name, "enqueued human_input signal");
        Ok(())
    }

    /// Get a single instance by ID, returning the instance and its sequence name.
    pub async fn get_instance(
        &self,
        instance_id: &str,
    ) -> Result<(TaskInstance, String), MobileError> {
        let id = parse_instance_id(instance_id)?;
        let inst = self
            .storage
            .get_mobile_instance(&orch8_storage::MobileInstanceAccess::Legacy, id)
            .await?
            .ok_or_else(|| MobileError::NotFound {
                message: format!("instance '{instance_id}' not found"),
            })?;

        let seq = self
            .storage
            .get_sequence(inst.sequence_id)
            .await?
            .map(|s| s.name)
            .unwrap_or_default();

        Ok((inst, seq))
    }

    /// List all non-terminal instances with their sequence names.
    pub(crate) async fn active_instances(
        &self,
    ) -> Result<Vec<(ActiveInstanceProjection, String)>, MobileError> {
        self.mobile_storage
            .list_active_instance_summaries(100)
            .await
            .map_err(Into::into)
    }

    /// Garbage-collect instances that have exceeded their max lifetime.
    /// Returns the number of instances expired.
    pub async fn gc_expired_instances(&self, max_lifetime_secs: u64) -> Result<u64, MobileError> {
        let max_lifetime = Duration::from_secs(max_lifetime_secs);
        // A lifetime beyond the clock's range cannot expire any representable
        // instance. Do not wrap, panic, or silently substitute a shorter TTL.
        let cutoff = chrono::Duration::from_std(max_lifetime)
            .ok()
            .and_then(|lifetime| chrono::Utc::now().checked_sub_signed(lifetime))
            .unwrap_or(chrono::DateTime::<chrono::Utc>::MIN_UTC);

        let instance_ids = self
            .mobile_storage
            .list_expired_active_instance_ids(cutoff, 50)
            .await?;
        let mut expired = 0;

        for instance_id in instance_ids {
            // CAS from the current state (L3-14): an instance that completed
            // since the listing must keep its terminal state.
            let result = match self.storage.get_instance(instance_id).await {
                Ok(Some(inst)) if !inst.state.is_terminal() => {
                    self.storage
                        .conditional_update_instance_state(
                            instance_id,
                            inst.state,
                            InstanceState::Failed,
                            None,
                        )
                        .await
                }
                Ok(_) => Ok(false),
                Err(e) => Err(e),
            };
            match result {
                Ok(true) => {
                    info!(%instance_id, "expired instance (exceeded max lifetime)");
                    expired += 1;
                }
                Ok(false) => {}
                Err(e) => warn!(%instance_id, error = %e, "failed to expire instance"),
            }
        }

        Ok(expired)
    }

    /// Delete terminal instances (and their history) that finished more than
    /// `retention_secs` ago, then give the freed space back to the OS
    /// (L1-06, L1-18). Bounded per call; a backlog drains over later ticks.
    /// `retention_secs == 0` keeps everything.
    pub async fn purge_terminal_instances(&self, retention_secs: u64) -> Result<u64, MobileError> {
        if retention_secs == 0 {
            return Ok(0);
        }
        let Some(cutoff) = chrono::Duration::from_std(Duration::from_secs(retention_secs))
            .ok()
            .and_then(|retention| chrono::Utc::now().checked_sub_signed(retention))
        else {
            return Ok(0);
        };
        let deleted = self
            .storage
            .delete_terminal_instances(cutoff, TERMINAL_PURGE_BATCH)
            .await?;
        if deleted > 0 {
            info!(deleted, "purged terminal instances past retention");
        }
        let now = std::time::Instant::now();
        self.sweep_finished_delegations(now).await;
        if self.compaction_due(deleted, now) {
            self.storage.compact_storage().await?;
        }
        Ok(deleted)
    }

    /// Remove a dedup entry associated with the given instance ID.
    pub async fn cleanup_dedup(&self, instance_id: &str) {
        if let Err(error) = self.mobile_storage.remove_dedup(instance_id).await {
            warn!(instance_id, %error, "failed to remove persisted mobile dedup entry");
        }
    }
}

fn parse_instance_id(s: &str) -> Result<InstanceId, MobileError> {
    let uuid = uuid::Uuid::parse_str(s).map_err(|e| MobileError::InvalidInput {
        message: format!("invalid instance ID '{s}': {e}"),
    })?;
    Ok(InstanceId::from_uuid(uuid))
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn setup() -> (
        InstanceLifecycleManager,
        Arc<MobileStorage>,
        tempfile::TempDir,
    ) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("test.db").to_string_lossy().to_string();
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::file_mobile(&path)
                .await
                .unwrap(),
        );
        let storage: Arc<dyn StorageBackend> = sqlite.clone();
        let mobile_storage = Arc::new(MobileStorage::new(sqlite));
        let lifecycle = InstanceLifecycleManager::new(storage.clone(), mobile_storage.clone(), 10);
        (lifecycle, mobile_storage, dir)
    }

    #[tokio::test]
    async fn mc07_idle_gc_passes_do_not_compact_more_than_hourly() {
        let (lifecycle, _ms, _dir) = setup().await;
        let t0 = std::time::Instant::now();
        assert!(
            lifecycle.compaction_due(0, t0),
            "first pass establishes a baseline"
        );
        assert!(!lifecycle.compaction_due(0, t0 + Duration::from_secs(60)));
        assert!(!lifecycle.compaction_due(0, t0 + Duration::from_secs(59 * 60)));
        assert!(
            lifecycle.compaction_due(3, t0 + Duration::from_secs(120)),
            "a purge that freed rows compacts immediately"
        );
        assert!(!lifecycle.compaction_due(0, t0 + Duration::from_secs(150)));
        assert!(lifecycle.compaction_due(0, t0 + Duration::from_secs(120 + 3600)));
    }

    #[tokio::test]
    async fn opt03_mobile_file_db_uses_wal_with_normal_synchronous() {
        let (_lifecycle, ms, _dir) = setup().await;
        let sync: i64 = sqlx::query_scalar("PRAGMA synchronous")
            .fetch_one(ms.pool())
            .await
            .unwrap();
        assert_eq!(sync, 1, "NORMAL");
        let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
            .fetch_one(ms.pool())
            .await
            .unwrap();
        assert_eq!(mode, "wal");
    }

    async fn seed_sequence(storage: &Arc<dyn StorageBackend>, name: &str) {
        let seq = orch8_types::sequence::SequenceDefinition {
            embed: None,
            sub_tenant: None,
            schema: None,
            schema_version: orch8_types::sequence::SEQUENCE_SCHEMA_VERSION,
            id: orch8_types::ids::SequenceId::new(),
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: Namespace::new("default"),
            name: name.to_string(),
            version: 1,
            deprecated: false,
            status: orch8_types::sequence::SequenceStatus::Production,
            blocks: vec![orch8_types::sequence::BlockDefinition::Step(Box::new(
                orch8_types::sequence::StepDef {
                    id: orch8_types::ids::BlockId::new("s1"),
                    handler: "noop".to_string(),
                    params: serde_json::json!({}),
                    delay: None,
                    retry: None,
                    timeout: None,
                    rate_limit_key: None,
                    rate_budget: None,
                    placement: None,
                    send_window: None,
                    context_access: None,
                    cancellable: true,
                    wait_for_input: None,
                    queue_name: None,
                    deadline: None,
                    on_deadline_breach: None,
                    fallback_handler: None,
                    cache_key: None,
                    output_schema: None,
                    when: None,
                    compensation: None,
                },
            ))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            placement: None,
            created_at: chrono::Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();
    }

    /// L3-14: cancelling an instance that already completed must not
    /// overwrite its terminal state.
    #[tokio::test]
    async fn cancel_does_not_overwrite_completed_instance() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let id = lifecycle.start("seq", "{}", None).await.unwrap();
        let parsed = parse_instance_id(&id).unwrap();
        lifecycle
            .storage
            .update_instance_state(parsed, InstanceState::Completed, None)
            .await
            .unwrap();
        lifecycle.cancel_instance(&id).await.unwrap();
        let after = lifecycle
            .storage
            .get_instance(parsed)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(after.state, InstanceState::Completed);
    }

    /// L1-06: finished instances older than the retention are deleted on the
    /// device; recent and live ones stay.
    #[tokio::test]
    async fn purge_terminal_instances_deletes_only_old_finished_runs() {
        let (lifecycle, mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let old = parse_instance_id(&lifecycle.start("seq", "{}", None).await.unwrap()).unwrap();
        let recent = parse_instance_id(&lifecycle.start("seq", "{}", None).await.unwrap()).unwrap();
        let live = parse_instance_id(&lifecycle.start("seq", "{}", None).await.unwrap()).unwrap();
        for id in [old, recent] {
            lifecycle
                .storage
                .update_instance_state(id, InstanceState::Completed, None)
                .await
                .unwrap();
        }
        let long_ago = (chrono::Utc::now() - chrono::Duration::days(40))
            .to_rfc3339_opts(chrono::SecondsFormat::Micros, true);
        for id in [old, live] {
            sqlx::query("UPDATE task_instances SET updated_at = ?1 WHERE id = ?2")
                .bind(&long_ago)
                .bind(id.into_uuid().to_string())
                .execute(mobile_storage.pool())
                .await
                .unwrap();
        }

        let deleted = lifecycle
            .purge_terminal_instances(TERMINAL_INSTANCE_RETENTION_SECS)
            .await
            .unwrap();
        assert_eq!(deleted, 1);
        assert!(lifecycle.storage.get_instance(old).await.unwrap().is_none());
        assert!(
            lifecycle
                .storage
                .get_instance(recent)
                .await
                .unwrap()
                .is_some()
        );
        assert!(
            lifecycle
                .storage
                .get_instance(live)
                .await
                .unwrap()
                .is_some()
        );
        assert_eq!(lifecycle.purge_terminal_instances(0).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn hydrate_dedup_loads_from_storage() {
        let (lifecycle, mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let existing = lifecycle.start("seq", "{}", Some("dk1")).await.unwrap();

        let reloaded = InstanceLifecycleManager::new(lifecycle.storage.clone(), mobile_storage, 10);
        reloaded.hydrate_dedup().await.unwrap();

        let id = reloaded.start("seq", "{}", Some("dk1")).await.unwrap();
        assert_eq!(id, existing);
    }

    #[tokio::test]
    async fn hydrate_dedup_prunes_terminal_crash_leftovers() {
        let (lifecycle, mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let terminal = lifecycle
            .start("seq", "{}", Some("dk-terminal"))
            .await
            .unwrap();
        lifecycle
            .storage
            .update_instance_state(
                parse_instance_id(&terminal).unwrap(),
                InstanceState::Completed,
                None,
            )
            .await
            .unwrap();

        let reloaded =
            InstanceLifecycleManager::new(lifecycle.storage.clone(), mobile_storage.clone(), 10);
        reloaded.hydrate_dedup().await.unwrap();

        assert!(
            mobile_storage
                .get_dedup_instance("dk-terminal")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn start_creates_instance_and_returns_id() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle
            .start("seq", r#"{"key":"val"}"#, None)
            .await
            .unwrap();
        assert!(!id.is_empty());

        let (inst, seq_name) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(seq_name, "seq");
        assert_eq!(inst.state, InstanceState::Scheduled);
        assert_eq!(inst.context.data["key"], "val");
    }

    #[tokio::test]
    async fn extreme_lifetimes_do_not_expire_existing_instances() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let id = lifecycle.start("seq", "{}", None).await.unwrap();
        let (mut old, _) = lifecycle.get_instance(&id).await.unwrap();
        old.id = InstanceId::new();
        old.created_at = chrono::Utc::now() - chrono::Duration::days(2);
        lifecycle.storage.create_instance(&old).await.unwrap();
        for lifetime in [u64::MAX, i64::MAX as u64 / 1000] {
            assert_eq!(lifecycle.gc_expired_instances(lifetime).await.unwrap(), 0);
        }
        assert_eq!(lifecycle.gc_expired_instances(86_400).await.unwrap(), 1);
    }

    #[tokio::test]
    async fn start_enforces_max_concurrent_instances() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        // Create a lifecycle manager with max_concurrent_instances = 2.
        let limited = InstanceLifecycleManager::new(
            lifecycle.storage.clone(),
            lifecycle.mobile_storage.clone(),
            2,
        );

        limited.start("seq", "{}", None).await.unwrap();
        limited.start("seq", "{}", None).await.unwrap();

        let result = limited.start("seq", "{}", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("max concurrent instances"),
            "unexpected error: {err}"
        );
    }

    #[tokio::test]
    async fn start_with_dedup_returns_same_id() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id1 = lifecycle.start("seq", "{}", Some("dedup-1")).await.unwrap();
        let id2 = lifecycle.start("seq", "{}", Some("dedup-1")).await.unwrap();
        assert_eq!(id1, id2);
    }

    #[tokio::test]
    async fn concurrent_starts_with_dedup_return_the_same_instance() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let (first, second) = tokio::join!(
            lifecycle.start("seq", "{}", Some("concurrent-dedup")),
            lifecycle.start("seq", "{}", Some("concurrent-dedup")),
        );
        assert_eq!(first.unwrap(), second.unwrap());
    }

    #[tokio::test]
    async fn concurrent_admission_does_not_overbook_across_managers() {
        let (lifecycle, mobile, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let first = InstanceLifecycleManager::new(lifecycle.storage.clone(), mobile.clone(), 1);
        let independent = Arc::new(
            orch8_storage::sqlite::SqliteStorage::file_mobile(
                &_dir.path().join("test.db").to_string_lossy(),
            )
            .await
            .unwrap(),
        );
        let second = InstanceLifecycleManager::new(
            independent.clone(),
            Arc::new(MobileStorage::new(independent)),
            1,
        );
        let (a, b) = tokio::join!(
            first.start("seq", "{}", None),
            second.start("seq", "{}", None)
        );
        assert_eq!(usize::from(a.is_ok()) + usize::from(b.is_ok()), 1);
    }

    #[tokio::test]
    async fn concurrent_terminal_key_reuse_creates_only_one_fresh_run() {
        let (lifecycle, mobile, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let old = lifecycle.start("seq", "{}", Some("reused")).await.unwrap();
        lifecycle
            .storage
            .update_instance_state(
                parse_instance_id(&old).unwrap(),
                InstanceState::Completed,
                None,
            )
            .await
            .unwrap();
        lifecycle.cleanup_dedup(&old).await;
        let other = InstanceLifecycleManager::new(lifecycle.storage.clone(), mobile, 10);
        let (a, b) = tokio::join!(
            lifecycle.start("seq", "{}", Some("reused")),
            other.start("seq", "{}", Some("reused"))
        );
        assert_eq!(a.unwrap(), b.unwrap());
    }

    #[tokio::test]
    async fn dedup_write_failure_rolls_back_the_instance() {
        let (lifecycle, mobile, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        sqlx::query("CREATE TRIGGER fail_dedup BEFORE INSERT ON mobile_dedup BEGIN SELECT RAISE(ABORT, 'injected dedup failure'); END").execute(mobile.pool()).await.unwrap();
        assert!(lifecycle.start("seq", "{}", Some("failure")).await.is_err());
        let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM task_instances")
            .fetch_one(mobile.pool())
            .await
            .unwrap();
        assert_eq!(count, 0, "failed start must not leave a runnable orphan");
        sqlx::query("DROP TRIGGER fail_dedup")
            .execute(mobile.pool())
            .await
            .unwrap();
        let id = lifecycle.start("seq", "{}", Some("failure")).await.unwrap();
        assert_eq!(
            mobile
                .get_dedup_instance("failure")
                .await
                .unwrap()
                .as_deref(),
            Some(id.as_str())
        );
    }

    #[tokio::test]
    async fn paused_runs_consume_capacity_but_retry_still_succeeds() {
        let (lifecycle, mobile, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let limited = InstanceLifecycleManager::new(lifecycle.storage.clone(), mobile, 1);
        let id = limited.start("seq", "{}", Some("paused")).await.unwrap();
        limited
            .storage
            .update_instance_state(parse_instance_id(&id).unwrap(), InstanceState::Paused, None)
            .await
            .unwrap();
        assert!(matches!(
            limited.start("seq", "{}", None).await,
            Err(MobileError::ResourceLimit { .. })
        ));
        assert_eq!(
            limited.start("seq", "{}", Some("paused")).await.unwrap(),
            id
        );
    }

    #[tokio::test]
    async fn another_manager_terminal_transition_does_not_leave_a_cached_dead_id() {
        let (lifecycle, mobile, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let id = lifecycle.start("seq", "{}", Some("shared")).await.unwrap();
        let other = InstanceLifecycleManager::new(lifecycle.storage.clone(), mobile, 10);
        other.cancel_instance(&id).await.unwrap();
        let fresh = lifecycle.start("seq", "{}", Some("shared")).await.unwrap();
        assert_ne!(fresh, id);
        // Cleanup of the old run cannot erase its replacement's mapping.
        other.cleanup_dedup(&id).await;
        assert_eq!(
            lifecycle.start("seq", "{}", Some("shared")).await.unwrap(),
            fresh
        );
    }

    #[tokio::test]
    async fn empty_legacy_dedup_keys_are_serialized_too() {
        let (lifecycle, _mobile, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;
        let (a, b) = tokio::join!(
            lifecycle.start("seq", "{}", Some("")),
            lifecycle.start("seq", "{}", Some(""))
        );
        assert_eq!(a.unwrap(), b.unwrap());
    }

    #[tokio::test]
    async fn cancel_instance_sets_state_and_cleans_dedup() {
        let (lifecycle, mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", Some("dk1")).await.unwrap();
        lifecycle.cancel_instance(&id).await.unwrap();

        let (inst, _) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(inst.state, InstanceState::Cancelled);

        // Dedup should be cleaned up in both memory and storage.
        assert!(
            mobile_storage
                .get_dedup_instance("dk1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn complete_step_transitions_waiting_to_scheduled() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle
            .start("seq", r#"{"name":"Alice"}"#, None)
            .await
            .unwrap();

        // Manually set state to Waiting (simulating scheduler pause).
        let parsed = parse_instance_id(&id).unwrap();
        lifecycle
            .storage
            .update_instance_state(parsed, InstanceState::Waiting, Some(chrono::Utc::now()))
            .await
            .unwrap();

        lifecycle
            .complete_step(&id, "s1", r#"{"age":30}"#)
            .await
            .unwrap();

        let (inst, _) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(inst.state, InstanceState::Scheduled);
        assert_eq!(inst.context.data["name"], "Alice");
        assert_eq!(inst.context.data["age"], 30);
    }

    #[tokio::test]
    async fn complete_step_rejects_non_waiting() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", None).await.unwrap();
        let result = lifecycle.complete_step(&id, "s1", "{}").await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("expected Waiting"), "unexpected error: {err}");
    }

    #[tokio::test]
    async fn active_instances_filters_terminal_states() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id1 = lifecycle.start("seq", "{}", None).await.unwrap();
        let id2 = lifecycle.start("seq", "{}", None).await.unwrap();

        let active = lifecycle.active_instances().await.unwrap();
        assert_eq!(active.len(), 2);
        assert!(
            active
                .iter()
                .all(|(_, sequence_name)| sequence_name == "seq")
        );
        assert!(
            active
                .iter()
                .all(|(instance, _)| instance.state == InstanceState::Scheduled)
        );

        // Cancel one instance — it should no longer appear in active_instances.
        lifecycle.cancel_instance(&id1).await.unwrap();
        let active = lifecycle.active_instances().await.unwrap();
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].0.id.to_string(), id2);
    }

    #[tokio::test]
    async fn gc_expired_instances_fails_old_instances() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        // Create an instance directly with an old created_at timestamp.
        let old_instance = TaskInstance {
            claimed_at: None,
            sub_tenant: None,
            id: InstanceId::new(),
            sequence_id: {
                let seq = lifecycle
                    .storage
                    .get_sequence_by_name(
                        &TenantId::new("mobile").unwrap(),
                        &Namespace::new("default"),
                        "seq",
                        None,
                    )
                    .await
                    .unwrap()
                    .unwrap();
                seq.id
            },
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: Namespace::new("default"),
            state: InstanceState::Scheduled,
            next_fire_at: Some(chrono::Utc::now()),
            priority: orch8_types::instance::Priority::Normal,
            timezone: "UTC".to_string(),
            metadata: serde_json::json!({}),
            context: orch8_types::context::ExecutionContext::default(),
            concurrency_key: None,
            max_concurrency: None,
            idempotency_key: None,
            session_id: None,
            parent_instance_id: None,
            budget: None,
            created_at: chrono::Utc::now() - chrono::Duration::hours(48),
            updated_at: chrono::Utc::now() - chrono::Duration::hours(48),
        };
        lifecycle
            .storage
            .create_instance(&old_instance)
            .await
            .unwrap();

        let expired = lifecycle.gc_expired_instances(86_400).await.unwrap();
        assert_eq!(expired, 1);

        let (inst, _) = lifecycle
            .get_instance(&old_instance.id.to_string())
            .await
            .unwrap();
        assert_eq!(inst.state, InstanceState::Failed);
    }

    #[tokio::test]
    async fn gc_expired_instances_skips_recent_instances() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", None).await.unwrap();

        let expired = lifecycle.gc_expired_instances(86_400).await.unwrap();
        assert_eq!(expired, 0);

        let (inst, _) = lifecycle.get_instance(&id).await.unwrap();
        assert_eq!(inst.state, InstanceState::Scheduled);
    }

    #[tokio::test]
    async fn cleanup_dedup_removes_entry() {
        let (lifecycle, mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id = lifecycle.start("seq", "{}", Some("dk1")).await.unwrap();
        assert_eq!(
            mobile_storage.get_dedup_instance("dk1").await.unwrap(),
            Some(id.clone())
        );

        lifecycle.cleanup_dedup(&id).await;

        assert!(
            mobile_storage
                .get_dedup_instance("dk1")
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn get_instance_not_found() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        let result = lifecycle
            .get_instance(&uuid::Uuid::new_v4().to_string())
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn start_rejects_unknown_sequence() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        let result = lifecycle.start("unknown", "{}", None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("not found"), "unexpected error: {err}");
    }

    /// After a run with a dedup key reaches a terminal state and its dedup
    /// entry is cleaned up, reusing the key must start a FRESH run. The
    /// storage-level unique index on `idempotency_key` is never cleared, so
    /// without special handling the conflict path would return the dead
    /// instance id and the host would believe a workflow started while
    /// nothing ever runs.
    #[tokio::test]
    async fn start_with_dedup_key_after_terminal_starts_fresh_instance() {
        let (lifecycle, _mobile_storage, _dir) = setup().await;
        seed_sequence(&lifecycle.storage, "seq").await;

        let id1 = lifecycle.start("seq", "{}", Some("dk-term")).await.unwrap();

        // Simulate the run reaching a terminal state and the tick loop's
        // dedup cleanup firing.
        let parsed = parse_instance_id(&id1).unwrap();
        lifecycle
            .storage
            .update_instance_state(parsed, InstanceState::Completed, None)
            .await
            .unwrap();
        lifecycle.cleanup_dedup(&id1).await;

        let id2 = lifecycle.start("seq", "{}", Some("dk-term")).await.unwrap();
        assert_ne!(
            id1, id2,
            "terminal dedup conflict must not return the dead instance id"
        );

        let (inst, _) = lifecycle.get_instance(&id2).await.unwrap();
        assert_eq!(inst.state, InstanceState::Scheduled);

        // While the fresh run is active, the key still dedupes against it.
        let id3 = lifecycle.start("seq", "{}", Some("dk-term")).await.unwrap();
        assert_eq!(id2, id3);
    }
}
