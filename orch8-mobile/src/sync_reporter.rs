use std::collections::HashMap;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use sqlx::SqlitePool;
use tracing::{debug, warn};

use crate::lifecycle::InstanceLifecycleManager;
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::clock::SharedClock;
use orch8_types::ids::{BlockId, InstanceId, SequenceId};
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

use crate::storage::{MobileStorage, SyncExecutionStepProjection, SyncInstanceProjection};

/// Bounds for the server-suggested sync interval. A buggy (or malicious, if
/// the API key/channel is ever abused) response must not be able to turn the
/// device into a sync hot-loop (`0` → a POST every tick) or stop syncing
/// entirely (a huge value).
const MIN_SYNC_INTERVAL_SECS: u32 = 5;
const MAX_SYNC_INTERVAL_SECS: u32 = 3600;

/// Batched status + approval reporter that syncs with the server on a
/// configurable wall-clock cadence. Receives commands from the server and executes
/// them locally.
pub(crate) struct SyncReporter {
    pool: SqlitePool,
    http: OnceLock<reqwest::Client>,
    sync_url: String,
    device_id: String,
    api_key: String,
    sync_interval_secs: AtomicU64,
    last_sync_attempt: StdMutex<chrono::DateTime<chrono::Utc>>,
    clock: SharedClock,
    push_generation: AtomicU64,
    completed_push_generation: AtomicU64,
}

#[derive(serde::Serialize)]
struct SyncRequest<'a> {
    device_id: &'a str,
    status_updates: Vec<serde_json::Value>,
    approval_requests: Vec<serde_json::Value>,
    step_delegations: Vec<serde_json::Value>,
    command_acks: Vec<String>,
}

#[derive(serde::Deserialize)]
struct SyncResponse {
    #[serde(default)]
    commands: Vec<CommandEntry>,
    #[serde(default = "default_interval")]
    sync_interval_secs: u32,
}

const fn default_interval() -> u32 {
    30
}

#[derive(serde::Deserialize, Clone)]
struct CommandEntry {
    id: String,
    #[serde(rename = "type")]
    command_type: String,
    payload: serde_json::Value,
}

/// Outcome of executing a server command. Drives whether the command is
/// acked (server stops redelivering) or left un-acked for redelivery.
enum CommandOutcome {
    /// Side effects applied, or the command is permanently invalid (bad
    /// payload, unknown type) — ack it so the server stops redelivering.
    Done,
    /// Transient failure (storage error, resource limit, sequence not synced
    /// yet) — do not ack; the server will redeliver and we re-execute.
    Retryable,
}

type OutboxEntry = (String, String);

struct ScanOutboxEntries {
    statuses: Vec<OutboxEntry>,
    approvals: Vec<OutboxEntry>,
}

impl SyncReporter {
    pub fn new(pool: SqlitePool, sync_url: String, device_id: String, api_key: String) -> Self {
        Self::new_with_clock(pool, sync_url, device_id, api_key, SharedClock::default())
    }

    fn new_with_clock(
        pool: SqlitePool,
        sync_url: String,
        device_id: String,
        api_key: String,
        clock: SharedClock,
    ) -> Self {
        let now = clock.now();

        Self {
            pool,
            http: OnceLock::new(),
            sync_url,
            device_id,
            api_key,
            sync_interval_secs: AtomicU64::new(u64::from(default_interval())),
            last_sync_attempt: StdMutex::new(now),
            clock,
            push_generation: AtomicU64::new(0),
            completed_push_generation: AtomicU64::new(0),
        }
    }

    fn http_client(&self) -> &reqwest::Client {
        self.http
            .get_or_init(|| crate::build_mobile_http_client(Duration::from_secs(15)))
    }

    /// Called by host app when a silent push notification arrives.
    pub fn on_push_received(&self) {
        self.request_immediate_sync();
    }

    fn request_immediate_sync(&self) {
        *self
            .last_sync_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) =
            self.clock.now() - chrono::Duration::seconds(i64::from(MIN_SYNC_INTERVAL_SECS));
        self.push_generation.fetch_add(1, Ordering::Release);
    }

    /// Atomically claim a due sync attempt. Wall-clock scheduling keeps server
    /// intervals stable when the scheduler backs off while idle or on battery.
    pub fn should_sync(&self) -> bool {
        let interval = self.current_sync_interval();
        let now = self.clock.now();
        let mut last = self
            .last_sync_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let due =
            now - *last >= chrono::Duration::from_std(interval).unwrap_or(chrono::Duration::MAX);
        if due {
            *last = now;
        }
        due
    }

    /// Time until the next sync attempt is due, without claiming that attempt.
    /// The tick controller uses this to sleep directly to the wall-clock
    /// deadline when no workflow needs scheduler polling.
    pub fn next_sync_delay(&self) -> Duration {
        let interval = self.current_sync_interval();
        let now = self.clock.now();
        let last = *self
            .last_sync_attempt
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let elapsed = (now - last).to_std().unwrap_or(Duration::ZERO);
        interval.saturating_sub(elapsed)
    }

    fn current_sync_interval(&self) -> Duration {
        let seconds = if self.has_forced_sync() {
            u64::from(MIN_SYNC_INTERVAL_SECS)
        } else {
            self.sync_interval_secs.load(Ordering::Relaxed)
        };
        Duration::from_secs(seconds)
    }

    fn has_forced_sync(&self) -> bool {
        self.push_generation.load(Ordering::Acquire)
            != self.completed_push_generation.load(Ordering::Acquire)
    }

    fn mark_pushes_completed(&self, attempted_generation: u64) {
        self.completed_push_generation
            .fetch_max(attempted_generation, Ordering::AcqRel);
    }

    /// Initialize the outbox tables. Called once on engine startup.
    pub async fn init_tables(&self) {
        let result = sqlx::query(
            "CREATE TABLE IF NOT EXISTS sync_outbox (
                id            INTEGER PRIMARY KEY AUTOINCREMENT,
                entry_type    TEXT NOT NULL,
                instance_id   TEXT NOT NULL,
                payload       TEXT NOT NULL,
                created_at    TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE (entry_type, instance_id) ON CONFLICT REPLACE
            )",
        )
        .execute(&self.pool)
        .await;

        if let Err(e) = result {
            warn!(error = %e, "failed to create sync_outbox table");
        }

        let result = sqlx::query(
            "CREATE TABLE IF NOT EXISTS sync_command_acks (
                command_id TEXT PRIMARY KEY,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )",
        )
        .execute(&self.pool)
        .await;

        if let Err(e) = result {
            warn!(error = %e, "failed to create sync_command_acks table");
        }

        // H-16: durable idempotency record, independent of `sync_command_acks`
        // (which is just the ack outbox and gets its rows deleted as soon as
        // an ack is included in an outbound request — it can't double as a
        // "has this command_id ever executed" record without losing that
        // history the moment the ack is sent).
        let result = sqlx::query(
            "CREATE TABLE IF NOT EXISTS sync_executed_commands (
                command_id TEXT PRIMARY KEY,
                executed_at TEXT NOT NULL
            )",
        )
        .execute(&self.pool)
        .await;

        if let Err(e) = result {
            warn!(error = %e, "failed to create sync_executed_commands table");
        }
    }

    /// Queue a step delegation request to the server.
    /// The server resolves `credentials://` references and returns
    /// resolved params as a `step_result` command.
    #[allow(dead_code)]
    pub async fn queue_step_delegation(
        &self,
        request_id: &str,
        instance_id: &str,
        block_id: &str,
        handler: &str,
        params: &serde_json::Value,
    ) {
        let payload = serde_json::json!({
            "request_id": request_id,
            "instance_id": instance_id,
            "block_id": block_id,
            "handler": handler,
            "params": params,
        });
        let key = format!("{instance_id}:{block_id}");
        if let Err(e) = sqlx::query(
            "INSERT OR REPLACE INTO sync_outbox (entry_type, instance_id, payload) VALUES ('delegation', ?, ?)",
        )
        .bind(&key)
        .bind(payload.to_string())
        .execute(&self.pool)
        .await
        {
            warn!(error = %e, instance_id, block_id, "failed to queue mobile step delegation");
            return;
        }
        self.request_immediate_sync();
    }

    /// Scan storage for active instances and queue status updates + approval
    /// requests. Coalescing in the outbox table ensures duplicates are harmless.
    pub async fn scan_and_queue(
        &self,
        storage: &Arc<dyn StorageBackend>,
        mobile_storage: &MobileStorage,
        sequence_cache: &Arc<SequenceCache>,
    ) -> bool {
        let instances = match mobile_storage.list_sync_instances(100).await {
            Ok(list) => list,
            Err(e) => {
                debug!(error = %e, "scan_and_queue: failed to list instances");
                return false;
            }
        };

        let instance_ids: Vec<_> = instances.iter().map(|instance| instance.id).collect();
        let execution_steps = match mobile_storage
            .list_sync_execution_steps(&instance_ids)
            .await
        {
            Ok(steps) => Some(group_execution_steps(steps)),
            Err(error) => {
                debug!(%error, "scan_and_queue: failed to load execution steps");
                None
            }
        };

        let timestamp = chrono::Utc::now().to_rfc3339();
        let entries = collect_scan_entries(
            storage.as_ref(),
            sequence_cache,
            instances,
            execution_steps.as_ref(),
            &timestamp,
        )
        .await;

        self.queue_scan_entries(&entries.statuses, &entries.approvals)
            .await
    }

    async fn queue_scan_entries(
        &self,
        status_entries: &[(String, String)],
        approval_entries: &[(String, String)],
    ) -> bool {
        let mut transaction = match self.pool.begin().await {
            Ok(transaction) => transaction,
            Err(error) => {
                warn!(%error, "failed to begin mobile status outbox batch");
                return false;
            }
        };

        let result = async {
            insert_outbox_entries(
                &mut transaction,
                "INSERT OR REPLACE INTO sync_outbox (entry_type, instance_id, payload) ",
                "status",
                status_entries,
            )
            .await?;
            insert_outbox_entries(
                &mut transaction,
                "INSERT OR IGNORE INTO sync_outbox (entry_type, instance_id, payload) ",
                "approval",
                approval_entries,
            )
            .await?;
            transaction.commit().await
        }
        .await;
        if let Err(error) = result {
            warn!(%error, "failed to commit mobile status/approval outbox batch");
            return false;
        }
        true
    }

    /// Execute one sync cycle: drain outbox, POST to server, process commands.
    #[allow(clippy::too_many_lines)]
    pub async fn sync_once(
        &self,
        storage: &Arc<dyn StorageBackend>,
        lifecycle: &Arc<InstanceLifecycleManager>,
    ) -> bool {
        // Capture only pushes known when this request begins. A newer push
        // arriving during the HTTP round trip must remain pending afterward.
        let attempted_push_generation = self.push_generation.load(Ordering::Acquire);
        let pending = tokio::try_join!(
            sqlx::query_as::<_, (i64, String)>(
                "SELECT id, payload FROM sync_outbox WHERE entry_type = 'status' ORDER BY id LIMIT 100",
            )
            .fetch_all(&self.pool),
            sqlx::query_as::<_, (i64, String)>(
                "SELECT id, payload FROM sync_outbox WHERE entry_type = 'approval' ORDER BY id LIMIT 50",
            )
            .fetch_all(&self.pool),
            sqlx::query_as::<_, (i64, String)>(
                "SELECT id, payload FROM sync_outbox WHERE entry_type = 'delegation' ORDER BY id LIMIT 20",
            )
            .fetch_all(&self.pool),
            sqlx::query_as::<_, (String,)>(
                "SELECT command_id FROM sync_command_acks ORDER BY created_at LIMIT 100",
            )
            .fetch_all(&self.pool),
        );
        let (status_rows, approval_rows, delegation_rows, ack_rows) = match pending {
            Ok(rows) => rows,
            Err(error) => {
                warn!(%error, "failed to read pending mobile sync data");
                return false;
            }
        };

        let status_updates: Vec<serde_json::Value> = status_rows
            .iter()
            .filter_map(|(_, p)| serde_json::from_str(p).ok())
            .collect();

        let approval_requests: Vec<serde_json::Value> = approval_rows
            .iter()
            .filter_map(|(_, p)| serde_json::from_str(p).ok())
            .collect();

        let step_delegations: Vec<serde_json::Value> = delegation_rows
            .iter()
            .filter_map(|(_, p)| serde_json::from_str(p).ok())
            .collect();

        let command_acks: Vec<String> = ack_rows.iter().map(|(id,)| id.clone()).collect();

        let req = SyncRequest {
            device_id: &self.device_id,
            status_updates,
            approval_requests,
            step_delegations,
            command_acks: command_acks.clone(),
        };

        let result = self
            .http_client()
            .post(&self.sync_url)
            .header("x-api-key", &self.api_key)
            .header("x-device-id", &self.device_id)
            .json(&req)
            .send()
            .await;

        let resp = match result {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                warn!(status = %r.status(), "sync request failed");
                return false;
            }
            Err(e) => {
                debug!(error = %e, "sync request error (offline?)");
                return false;
            }
        };

        let sync_resp: SyncResponse = match resp.json().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to parse sync response");
                return false;
            }
        };
        let commands_received = !sync_resp.commands.is_empty();

        // Clean up sent outbox entries.
        let sent_status_ids: Vec<i64> = status_rows.iter().map(|(id, _)| *id).collect();
        let sent_approval_ids: Vec<i64> = approval_rows.iter().map(|(id, _)| *id).collect();
        let sent_delegation_ids: Vec<i64> = delegation_rows.iter().map(|(id, _)| *id).collect();

        let mut sent_outbox_ids = Vec::with_capacity(
            sent_status_ids.len() + sent_approval_ids.len() + sent_delegation_ids.len(),
        );
        sent_outbox_ids.extend_from_slice(&sent_status_ids);
        sent_outbox_ids.extend_from_slice(&sent_approval_ids);
        sent_outbox_ids.extend_from_slice(&sent_delegation_ids);
        if let Err(e) = delete_outbox_rows(&self.pool, &sent_outbox_ids).await {
            warn!(error = %e, "failed to delete sent sync outbox rows");
        }
        if let Err(e) = delete_command_acks(&self.pool, &command_acks).await {
            warn!(error = %e, "failed to delete sync command acknowledgements");
        }

        // Process commands from server.
        for cmd in &sync_resp.commands {
            // H-16: durably mark the command as executed *before* running its
            // side effects, and skip execution entirely if it's already
            // marked. The server may redeliver a command whose ack it never
            // received (lost response, restart before our ack round-trips) —
            // without this check that redelivery re-runs side effects (e.g.
            // starting a duplicate workflow instance, or double-cancelling)
            // instead of converging as a no-op.
            let insert_result = sqlx::query(
                "INSERT INTO sync_executed_commands (command_id, executed_at) VALUES (?, ?)",
            )
            .bind(&cmd.id)
            .bind(chrono::Utc::now().to_rfc3339())
            .execute(&self.pool)
            .await;

            let outcome = match insert_result {
                Ok(_) => Some(self.execute_command(cmd, storage, lifecycle).await),
                Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
                    debug!(command_id = %cmd.id, "command already executed — skipping duplicate delivery");
                    // Already executed: fall through to re-record the ack so
                    // the server stops redelivering.
                    None
                }
                Err(e) => {
                    // Without the idempotency marker, executing is unsafe (a
                    // redelivery would re-run side effects) — skip and let the
                    // server redeliver once storage is healthy again.
                    warn!(error = %e, command_id = %cmd.id, "failed to record command idempotency marker; skipping execution until redelivery");
                    continue;
                }
            };

            if let Some(CommandOutcome::Retryable) = outcome {
                // Transient failure: roll back the idempotency marker so the
                // server's redelivery re-executes the command, and don't ack —
                // acking here would tell the server the side effects happened
                // when they did not (e.g. a "start workflow" that never ran).
                if let Err(e) =
                    sqlx::query("DELETE FROM sync_executed_commands WHERE command_id = ?")
                        .bind(&cmd.id)
                        .execute(&self.pool)
                        .await
                {
                    warn!(error = %e, command_id = %cmd.id, "failed to roll back idempotency marker after retryable command failure");
                }
                continue;
            }

            if let Err(e) =
                sqlx::query("INSERT OR IGNORE INTO sync_command_acks (command_id) VALUES (?)")
                    .bind(&cmd.id)
                    .execute(&self.pool)
                    .await
            {
                warn!(error = %e, command_id = %cmd.id, "failed to record sync command ack");
            }
        }

        // Prune old idempotency records so the table doesn't grow forever.
        // 30 days comfortably outlives any plausible ack-redelivery window.
        if let Err(e) = sqlx::query("DELETE FROM sync_executed_commands WHERE executed_at < ?")
            .bind((chrono::Utc::now() - chrono::Duration::days(30)).to_rfc3339())
            .execute(&self.pool)
            .await
        {
            warn!(error = %e, "failed to prune old sync_executed_commands rows");
        }

        // Update sync interval from server hint, clamped to a sane range: a
        // response of `0` would make `should_sync` true on every tick (a sync
        // POST every ~100ms), and a huge value would effectively disable sync.
        let clamped_secs = sync_resp
            .sync_interval_secs
            .clamp(MIN_SYNC_INTERVAL_SECS, MAX_SYNC_INTERVAL_SECS);
        if clamped_secs != sync_resp.sync_interval_secs {
            warn!(
                requested = sync_resp.sync_interval_secs,
                clamped = clamped_secs,
                "server sync_interval_secs out of range — clamped"
            );
        }
        self.sync_interval_secs
            .store(u64::from(clamped_secs), Ordering::Relaxed);

        debug!(
            status_sent = sent_status_ids.len(),
            approvals_sent = sent_approval_ids.len(),
            delegations_sent = sent_delegation_ids.len(),
            commands_received = sync_resp.commands.len(),
            next_sync_secs = clamped_secs,
            "sync complete"
        );

        // Mark only the pushes covered by this successful round-trip. `fetch_max`
        // also keeps concurrent sync completions from moving the marker backward.
        self.mark_pushes_completed(attempted_push_generation);
        commands_received
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_command(
        &self,
        cmd: &CommandEntry,
        storage: &Arc<dyn StorageBackend>,
        lifecycle: &Arc<InstanceLifecycleManager>,
    ) -> CommandOutcome {
        match cmd.command_type.as_str() {
            "complete_step" => {
                let instance_id = cmd.payload.get("instance_id").and_then(|v| v.as_str());
                let step_name = cmd.payload.get("step_name").and_then(|v| v.as_str());
                let output = cmd.payload.get("output");

                if let (Some(iid), Some(step)) = (instance_id, step_name) {
                    debug!(
                        instance_id = %iid,
                        step_name = %step,
                        "executing complete_step command from server"
                    );

                    let inst_id = if let Ok(u) = uuid::Uuid::parse_str(iid) {
                        orch8_types::ids::InstanceId::from_uuid(u)
                    } else {
                        warn!(instance_id = %iid, "invalid UUID in complete_step command");
                        return CommandOutcome::Done;
                    };

                    let signal = orch8_types::signal::Signal {
                        id: uuid::Uuid::now_v7(),
                        instance_id: inst_id,
                        signal_type: orch8_types::signal::SignalType::Custom(format!(
                            "human_input:{step}"
                        )),
                        payload: output.cloned().unwrap_or_else(|| serde_json::json!({})),
                        delivered: false,
                        created_at: chrono::Utc::now(),
                        delivered_at: None,
                    };
                    if let Err(e) = storage.enqueue_signal(&signal).await {
                        warn!(error = %e, "failed to enqueue complete_step signal");
                        return CommandOutcome::Retryable;
                    }
                } else {
                    warn!("complete_step command missing instance_id or step_name");
                }
                CommandOutcome::Done
            }
            "cancel_instance" => {
                let instance_id = cmd.payload.get("instance_id").and_then(|v| v.as_str());
                if let Some(iid) = instance_id {
                    debug!(instance_id = %iid, "executing cancel_instance command from server");
                    let id = if let Ok(u) = uuid::Uuid::parse_str(iid) {
                        orch8_types::ids::InstanceId::from_uuid(u)
                    } else {
                        warn!(instance_id = %iid, "invalid UUID in cancel_instance command");
                        return CommandOutcome::Done;
                    };
                    if let Err(e) = storage
                        .update_instance_state(id, InstanceState::Cancelled, None)
                        .await
                    {
                        warn!(error = %e, "failed to cancel instance from server command");
                        return CommandOutcome::Retryable;
                    }
                } else {
                    warn!("cancel_instance command missing instance_id");
                }
                CommandOutcome::Done
            }
            "start_workflow" => {
                let sequence_name = cmd.payload.get("sequence_name").and_then(|v| v.as_str());
                let input = cmd
                    .payload
                    .get("input")
                    .map_or_else(|| "{}".to_string(), std::string::ToString::to_string);
                let dedup_key = cmd.payload.get("dedup_key").and_then(|v| v.as_str());

                if let Some(name) = sequence_name {
                    debug!(
                        sequence_name = %name,
                        "executing start_workflow command from server"
                    );
                    match lifecycle.start(name, &input, dedup_key).await {
                        Ok(id) => {
                            debug!(instance_id = %id, sequence_name = %name, "workflow started from server command");
                        }
                        // Permanently invalid input won't succeed on redelivery.
                        Err(e @ crate::error::MobileError::InvalidInput { .. }) => {
                            warn!(error = %e, sequence_name = %name, "start_workflow command has invalid input — not retrying");
                        }
                        Err(e) => {
                            warn!(error = %e, sequence_name = %name, "failed to start workflow from server command");
                            return CommandOutcome::Retryable;
                        }
                    }
                } else {
                    warn!("start_workflow command missing sequence_name");
                }
                CommandOutcome::Done
            }
            "update_sequence" => {
                let instance_id = cmd.payload.get("instance_id").and_then(|v| v.as_str());
                let policy = cmd
                    .payload
                    .get("policy")
                    .and_then(|v| v.as_str())
                    .unwrap_or("restart");

                if let Some(iid) = instance_id {
                    debug!(
                        instance_id = %iid,
                        policy = %policy,
                        "executing update_sequence command from server"
                    );
                    let inst_id = if let Ok(u) = uuid::Uuid::parse_str(iid) {
                        orch8_types::ids::InstanceId::from_uuid(u)
                    } else {
                        warn!(instance_id = %iid, "invalid UUID in update_sequence");
                        return CommandOutcome::Done;
                    };

                    match policy {
                        "restart" => {
                            // Cancel existing, start fresh with same sequence
                            if let Err(e) = storage
                                .update_instance_state(inst_id, InstanceState::Cancelled, None)
                                .await
                            {
                                warn!(error = %e, "update_sequence(restart): cancel failed");
                                return CommandOutcome::Retryable;
                            } else if let Some(seq_name) =
                                cmd.payload.get("sequence_name").and_then(|v| v.as_str())
                            {
                                let input = cmd.payload.get("input").map_or_else(
                                    || "{}".to_string(),
                                    std::string::ToString::to_string,
                                );
                                match lifecycle.start(seq_name, &input, None).await {
                                    Ok(new_id) => {
                                        debug!(old = %iid, new = %new_id, "update_sequence(restart): restarted");
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "update_sequence(restart): start failed");
                                        return CommandOutcome::Retryable;
                                    }
                                }
                            }
                        }
                        "fail" => {
                            if let Err(e) = storage
                                .update_instance_state(inst_id, InstanceState::Failed, None)
                                .await
                            {
                                warn!(error = %e, "update_sequence(fail): failed");
                                return CommandOutcome::Retryable;
                            }
                        }
                        "cancel" => {
                            if let Err(e) = storage
                                .update_instance_state(inst_id, InstanceState::Cancelled, None)
                                .await
                            {
                                warn!(error = %e, "update_sequence(cancel): failed");
                                return CommandOutcome::Retryable;
                            }
                        }
                        "graceful" => {
                            // Graceful: let current step finish, then apply new
                            // sequence version. Mark instance for version bump.
                            debug!(instance_id = %iid, "update_sequence(graceful): flagged for graceful update");
                            // For now, we reload the sequence cache to pick up new versions.
                            // The next tick will use the latest sequence version.
                        }
                        "skip_executed" => {
                            // Cancel old, start new with same sequence; executed steps
                            // will be skipped by the engine if the execution tree
                            // carries forward completed node states.
                            if let Err(e) = storage
                                .update_instance_state(inst_id, InstanceState::Cancelled, None)
                                .await
                            {
                                warn!(error = %e, "update_sequence(skip_executed): cancel failed");
                                return CommandOutcome::Retryable;
                            } else if let Some(seq_name) =
                                cmd.payload.get("sequence_name").and_then(|v| v.as_str())
                            {
                                let input = cmd.payload.get("input").map_or_else(
                                    || "{}".to_string(),
                                    std::string::ToString::to_string,
                                );
                                match lifecycle.start(seq_name, &input, None).await {
                                    Ok(new_id) => {
                                        debug!(old = %iid, new = %new_id, "update_sequence(skip_executed): restarted with skip");
                                    }
                                    Err(e) => {
                                        warn!(error = %e, "update_sequence(skip_executed): start failed");
                                        return CommandOutcome::Retryable;
                                    }
                                }
                            }
                        }
                        unknown => {
                            warn!(policy = %unknown, "update_sequence: unknown policy");
                        }
                    }
                } else {
                    warn!("update_sequence command missing instance_id");
                }
                CommandOutcome::Done
            }
            "step_result" => {
                let request_id = cmd.payload.get("request_id").and_then(|v| v.as_str());
                let instance_id = cmd.payload.get("instance_id").and_then(|v| v.as_str());
                let block_id = cmd.payload.get("block_id").and_then(|v| v.as_str());
                let success = cmd
                    .payload
                    .get("success")
                    .and_then(serde_json::Value::as_bool)
                    .unwrap_or(false);

                if let (Some(rid), Some(iid), Some(bid)) = (request_id, instance_id, block_id) {
                    if success {
                        let resolved_params = cmd
                            .payload
                            .get("resolved_params")
                            .cloned()
                            .unwrap_or_default();
                        debug!(
                            request_id = %rid,
                            instance_id = %iid,
                            block_id = %bid,
                            "step_result: credentials resolved, delivering params to step"
                        );
                        let inst_id = if let Ok(u) = uuid::Uuid::parse_str(iid) {
                            orch8_types::ids::InstanceId::from_uuid(u)
                        } else {
                            warn!(instance_id = %iid, "invalid UUID in step_result");
                            return CommandOutcome::Done;
                        };
                        let signal = orch8_types::signal::Signal {
                            id: uuid::Uuid::now_v7(),
                            instance_id: inst_id,
                            signal_type: orch8_types::signal::SignalType::Custom(format!(
                                "delegation_result:{bid}"
                            )),
                            payload: serde_json::json!({
                                "resolved_params": resolved_params,
                                "request_id": rid,
                            }),
                            delivered: false,
                            created_at: chrono::Utc::now(),
                            delivered_at: None,
                        };
                        if let Err(e) = storage.enqueue_signal(&signal).await {
                            warn!(error = %e, "failed to enqueue step_result signal");
                            return CommandOutcome::Retryable;
                        }
                    } else {
                        let error = cmd
                            .payload
                            .get("error")
                            .and_then(|v| v.as_str())
                            .unwrap_or("unknown");
                        warn!(
                            request_id = %rid,
                            instance_id = %iid,
                            error = %error,
                            "step_result: delegation failed"
                        );
                    }
                } else {
                    warn!("step_result command missing request_id, instance_id, or block_id");
                }
                CommandOutcome::Done
            }
            other => {
                warn!(command_type = %other, "unknown command type from server");
                CommandOutcome::Done
            }
        }
    }
}

async fn delete_outbox_rows(pool: &SqlitePool, ids: &[i64]) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new("DELETE FROM sync_outbox WHERE id IN (");
    let mut separated = query.separated(",");
    for id in ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");
    query.build().execute(pool).await?;
    Ok(())
}

async fn delete_command_acks(pool: &SqlitePool, ids: &[String]) -> Result<(), sqlx::Error> {
    if ids.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new("DELETE FROM sync_command_acks WHERE command_id IN (");
    let mut separated = query.separated(",");
    for id in ids {
        separated.push_bind(id);
    }
    separated.push_unseparated(")");
    query.build().execute(pool).await?;
    Ok(())
}

async fn insert_outbox_entries(
    transaction: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    insert_sql: &str,
    entry_type: &str,
    entries: &[OutboxEntry],
) -> Result<(), sqlx::Error> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut query = sqlx::QueryBuilder::new(insert_sql);
    query.push_values(entries, |mut row, (instance_id, payload)| {
        row.push_bind(entry_type)
            .push_bind(instance_id)
            .push_bind(payload);
    });
    query.build().execute(&mut **transaction).await?;
    Ok(())
}

fn group_execution_steps(
    steps: Vec<SyncExecutionStepProjection>,
) -> HashMap<InstanceId, Vec<SyncExecutionStepProjection>> {
    let mut grouped = HashMap::new();
    for step in steps {
        grouped
            .entry(step.instance_id)
            .or_insert_with(Vec::new)
            .push(step);
    }
    grouped
}

async fn collect_scan_entries(
    storage: &dyn StorageBackend,
    sequence_cache: &SequenceCache,
    instances: Vec<SyncInstanceProjection>,
    execution_steps: Option<&HashMap<InstanceId, Vec<SyncExecutionStepProjection>>>,
    timestamp: &str,
) -> ScanOutboxEntries {
    let mut sequences: HashMap<SequenceId, Option<Arc<SequenceDefinition>>> = HashMap::new();
    let mut entries = ScanOutboxEntries {
        statuses: Vec::with_capacity(instances.len()),
        approvals: Vec::new(),
    };

    for instance in instances {
        if let std::collections::hash_map::Entry::Vacant(entry) =
            sequences.entry(instance.sequence_id)
        {
            entry.insert(
                sequence_cache
                    .get_by_id(storage, instance.sequence_id)
                    .await
                    .ok(),
            );
        }
        let sequence = sequences
            .get(&instance.sequence_id)
            .and_then(Option::as_deref);
        let steps = execution_steps.and_then(|grouped| {
            build_steps_payload(
                grouped.get(&instance.id).map_or(&[], Vec::as_slice),
                sequence,
            )
        });

        entries
            .statuses
            .push(build_status_entry(&instance, sequence, steps, timestamp));
        if let Some(approval) = build_approval_entry(&instance, sequence) {
            entries.approvals.push(approval);
        }
    }
    entries
}

fn build_status_entry(
    instance: &SyncInstanceProjection,
    sequence: Option<&SequenceDefinition>,
    steps: Option<serde_json::Value>,
    timestamp: &str,
) -> OutboxEntry {
    let instance_id = instance.id.to_string();
    let handler = instance.current_step.as_ref().and_then(|step_id| {
        sequence.and_then(|definition| find_handler(&definition.blocks, step_id))
    });
    let payload = status_payload(
        &instance_id,
        sequence.map(|definition| definition.name.as_str()),
        &format!("{:?}", instance.state),
        instance.current_step.as_ref().map(BlockId::as_str),
        handler.as_deref(),
        steps,
        timestamp,
    );
    (instance_id, payload)
}

fn build_approval_entry(
    instance: &SyncInstanceProjection,
    sequence: Option<&SequenceDefinition>,
) -> Option<OutboxEntry> {
    if instance.state != InstanceState::Waiting {
        return None;
    }
    let step_id = instance.current_step.as_ref()?;
    let instance_id = instance.id.to_string();
    let (prompt, choices, store_as, timeout) = sequence
        .and_then(|definition| find_wait_info(&definition.blocks, step_id))
        .unwrap_or((None, None, None, None));
    let payload = approval_payload(
        &instance_id,
        step_id.as_str(),
        sequence.map(|definition| definition.name.as_str()),
        prompt.as_deref(),
        choices.as_deref(),
        store_as.as_deref(),
        timeout,
    );
    Some((format!("{instance_id}:{step_id}"), payload))
}

fn status_payload(
    instance_id: &str,
    sequence_name: Option<&str>,
    state: &str,
    current_step: Option<&str>,
    handler: Option<&str>,
    steps: Option<serde_json::Value>,
    timestamp: &str,
) -> String {
    serde_json::json!({
        "instance_id": instance_id,
        "sequence_name": sequence_name,
        "state": state,
        "current_step": current_step,
        "handler": handler,
        "steps": steps,
        "timestamp": timestamp,
    })
    .to_string()
}

#[allow(clippy::too_many_arguments)]
fn approval_payload(
    instance_id: &str,
    block_id: &str,
    sequence_name: Option<&str>,
    prompt: Option<&str>,
    choices: Option<&str>,
    store_as: Option<&str>,
    timeout_seconds: Option<i64>,
) -> String {
    serde_json::json!({
        "instance_id": instance_id,
        "block_id": block_id,
        "sequence_name": sequence_name,
        "prompt": prompt,
        "choices": choices.and_then(|value| serde_json::from_str::<serde_json::Value>(value).ok()),
        "store_as": store_as,
        "timeout_seconds": timeout_seconds,
    })
    .to_string()
}

fn build_steps_payload(
    tree: &[SyncExecutionStepProjection],
    seq: Option<&SequenceDefinition>,
) -> Option<serde_json::Value> {
    let mut entries: Vec<serde_json::Value> = Vec::new();

    if let Some(seq) = seq {
        let flat = flatten_blocks(&seq.blocks);

        // Index tree nodes by block id so each block lookup is O(1) instead
        // of a linear scan of the tree for every block.
        let nodes_by_block: std::collections::HashMap<&str, _> =
            tree.iter().map(|n| (n.block_id.as_str(), n)).collect();

        for (block_id, block_type, handler) in &flat {
            let node = nodes_by_block.get(block_id.as_str());
            let (state, started_at, completed_at) = match node {
                Some(n) => (
                    n.state.clone(),
                    n.started_at.clone(),
                    n.completed_at.clone(),
                ),
                None => ("pending".into(), None, None),
            };
            entries.push(serde_json::json!({
                "block_id": block_id.as_str(),
                "block_type": block_type,
                "state": state,
                "handler": handler,
                "started_at": started_at,
                "completed_at": completed_at,
            }));
        }
    } else {
        for node in tree {
            entries.push(serde_json::json!({
                "block_id": node.block_id.as_str(),
                "block_type": node.block_type,
                "state": node.state,
                "handler": null,
                "started_at": node.started_at,
                "completed_at": node.completed_at,
            }));
        }
    }

    if entries.is_empty() {
        return None;
    }
    Some(serde_json::Value::Array(entries))
}

fn flatten_blocks(blocks: &[BlockDefinition]) -> Vec<(BlockId, String, Option<String>)> {
    let mut out = Vec::new();
    for b in blocks {
        match b {
            BlockDefinition::Step(sd) => {
                out.push((sd.id.clone(), "step".into(), Some(sd.handler.clone())));
            }
            BlockDefinition::Parallel(p) => {
                out.push((p.id.clone(), "parallel".into(), None));
                for branch in &p.branches {
                    out.extend(flatten_blocks(branch));
                }
            }
            BlockDefinition::Race(r) => {
                out.push((r.id.clone(), "race".into(), None));
                for branch in &r.branches {
                    out.extend(flatten_blocks(branch));
                }
            }
            BlockDefinition::Loop(l) => {
                out.push((l.id.clone(), "loop".into(), None));
                out.extend(flatten_blocks(&l.body));
            }
            BlockDefinition::ForEach(fe) => {
                out.push((fe.id.clone(), "for_each".into(), None));
                out.extend(flatten_blocks(&fe.body));
            }
            BlockDefinition::Router(rt) => {
                out.push((rt.id.clone(), "router".into(), None));
                for route in &rt.routes {
                    out.extend(flatten_blocks(&route.blocks));
                }
                if let Some(ref def) = rt.default {
                    out.extend(flatten_blocks(def));
                }
            }
            BlockDefinition::TryCatch(tc) => {
                out.push((tc.id.clone(), "try_catch".into(), None));
                out.extend(flatten_blocks(&tc.try_block));
                out.extend(flatten_blocks(&tc.catch_block));
                if let Some(ref fin) = tc.finally_block {
                    out.extend(flatten_blocks(fin));
                }
            }
            BlockDefinition::SubSequence(ss) => {
                out.push((ss.id.clone(), "sub_sequence".into(), None));
            }
            BlockDefinition::ABSplit(ab) => {
                out.push((ab.id.clone(), "ab_split".into(), None));
                for variant in &ab.variants {
                    out.extend(flatten_blocks(&variant.blocks));
                }
            }
            BlockDefinition::CancellationScope(cs) => {
                out.push((cs.id.clone(), "cancellation_scope".into(), None));
                out.extend(flatten_blocks(&cs.blocks));
            }
            BlockDefinition::Saga(saga) => {
                out.push((saga.id.clone(), "saga".into(), None));
                for step in &saga.steps {
                    out.extend(flatten_blocks(std::slice::from_ref(step.action.as_ref())));
                    if let Some(comp) = &step.compensation {
                        out.extend(flatten_blocks(std::slice::from_ref(comp.as_ref())));
                    }
                }
            }
        }
    }
    out
}

fn find_handler(
    blocks: &[orch8_types::sequence::BlockDefinition],
    step_id: &BlockId,
) -> Option<String> {
    blocks.iter().find_map(|b| {
        if let orch8_types::sequence::BlockDefinition::Step(sd) = b
            && sd.id == *step_id
        {
            return Some(sd.handler.clone());
        }
        None
    })
}

#[allow(clippy::type_complexity)]
fn find_wait_info(
    blocks: &[orch8_types::sequence::BlockDefinition],
    step_id: &BlockId,
) -> Option<(Option<String>, Option<String>, Option<String>, Option<i64>)> {
    blocks.iter().find_map(|b| {
        if let orch8_types::sequence::BlockDefinition::Step(sd) = b
            && sd.id == *step_id
        {
            return sd.wait_for_input.as_ref().map(|w| {
                let choices_json = w
                    .choices
                    .as_ref()
                    .and_then(|c| serde_json::to_string(c).ok());
                #[allow(clippy::cast_possible_wrap)]
                let timeout_secs = w.timeout.map(|d| d.as_secs() as i64);
                (
                    Some(w.prompt.clone()),
                    choices_json,
                    w.store_as.clone(),
                    timeout_secs,
                )
            });
        }
        None
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::clock::{Clock, ManualClock};
    use orch8_types::filter::{InstanceFilter, Pagination};
    use orch8_types::ids::{SequenceId, TenantId};
    use orch8_types::sequence::{SequenceStatus, StepDef};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    async fn setup(
        sync_url: String,
    ) -> (
        SyncReporter,
        Arc<dyn StorageBackend>,
        Arc<InstanceLifecycleManager>,
    ) {
        let sqlite = Arc::new(
            orch8_storage::sqlite::SqliteStorage::in_memory()
                .await
                .unwrap(),
        );
        let storage: Arc<dyn StorageBackend> = sqlite.clone();
        let mobile_storage = Arc::new(crate::storage::MobileStorage::new(sqlite));
        let lifecycle = Arc::new(InstanceLifecycleManager::new(
            storage.clone(),
            mobile_storage,
            10,
        ));

        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let reporter = SyncReporter::new(pool, sync_url, "device-1".to_string(), "key".to_string());
        reporter.init_tables().await;

        (reporter, storage, lifecycle)
    }

    #[tokio::test]
    async fn sync_read_failure_keeps_forced_retry_throttled() {
        let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        assert!(reporter.http.get().is_none());
        reporter.on_push_received();
        assert!(reporter.should_sync(), "push should be immediately due");
        reporter.pool.close().await;

        assert!(!reporter.sync_once(&storage, &lifecycle).await);

        assert!(
            !reporter.should_sync(),
            "failed forced sync must wait before retrying"
        );
        assert!(
            reporter.http.get().is_none(),
            "outbox read failure must not initialize networking"
        );
    }

    async fn seed_sequence(storage: &Arc<dyn StorageBackend>, name: &str) {
        let seq = SequenceDefinition {
            id: SequenceId::new(),
            tenant_id: TenantId::new("mobile").unwrap(),
            namespace: orch8_types::ids::Namespace::new("default"),
            name: name.to_string(),
            version: 1,
            deprecated: false,
            status: SequenceStatus::Production,
            blocks: vec![BlockDefinition::Step(Box::new(StepDef {
                id: BlockId::new("s1"),
                handler: "noop".to_string(),
                params: serde_json::json!({}),
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
                output_schema: None,
                when: None,
                compensation: None,
            }))],
            interceptors: None,
            input_schema: None,
            sla: None,
            on_failure: None,
            on_cancel: None,
            created_at: chrono::Utc::now(),
        };
        storage.create_sequence(&seq).await.unwrap();
    }

    #[tokio::test]
    async fn status_scan_reports_outbox_write_failure_for_retry() {
        let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        seed_sequence(&storage, "wf-a").await;
        lifecycle.start("wf-a", "{}", None).await.unwrap();
        reporter.pool.close().await;
        let sequence_cache = Arc::new(SequenceCache::new(50, Duration::from_secs(3600)));

        assert!(
            !reporter
                .scan_and_queue(&storage, lifecycle.mobile_storage(), &sequence_cache)
                .await
        );
    }

    #[tokio::test]
    async fn status_scan_batches_and_coalesces_status_and_approval_rows() {
        let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        let sequence: SequenceDefinition = serde_json::from_value(serde_json::json!({
            "id": SequenceId::new(),
            "tenant_id": "mobile",
            "namespace": "default",
            "name": "approval-flow",
            "version": 1,
            "deprecated": false,
            "blocks": [{
                "type": "step",
                "id": "review",
                "handler": "human_review",
                "params": {},
                "wait_for_input": {
                    "prompt": "Approve?",
                    "store_as": "decision"
                }
            }],
            "created_at": chrono::Utc::now()
        }))
        .unwrap();
        storage.create_sequence(&sequence).await.unwrap();
        let instance_id_text = lifecycle.start("approval-flow", "{}", None).await.unwrap();
        let instance_id = InstanceId::from_uuid(uuid::Uuid::parse_str(&instance_id_text).unwrap());
        let mut instance = storage.get_instance(instance_id).await.unwrap().unwrap();
        instance.context.runtime.current_step = Some(BlockId::new("review"));
        storage
            .update_instance_context(instance_id, &instance.context)
            .await
            .unwrap();
        storage
            .update_instance_state(instance_id, InstanceState::Waiting, None)
            .await
            .unwrap();

        let sequence_cache = Arc::new(SequenceCache::new(50, Duration::from_secs(3600)));
        for _ in 0..2 {
            assert!(
                reporter
                    .scan_and_queue(&storage, lifecycle.mobile_storage(), &sequence_cache)
                    .await
            );
        }

        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT entry_type, payload FROM sync_outbox ORDER BY entry_type")
                .fetch_all(&reporter.pool)
                .await
                .unwrap();
        assert_eq!(rows.len(), 2, "repeated scans must coalesce both rows");

        let approval: serde_json::Value = serde_json::from_str(&rows[0].1).unwrap();
        assert_eq!(rows[0].0, "approval");
        assert_eq!(approval["block_id"], "review");
        assert_eq!(approval["prompt"], "Approve?");

        let status: serde_json::Value = serde_json::from_str(&rows[1].1).unwrap();
        assert_eq!(rows[1].0, "status");
        assert_eq!(status["instance_id"], instance_id_text);
        assert_eq!(status["state"], "Waiting");
        assert_eq!(status["current_step"], "review");
        assert_eq!(status["handler"], "human_review");
        assert_eq!(status["steps"][0]["block_id"], "review");
    }

    async fn count_instances(storage: &Arc<dyn StorageBackend>) -> usize {
        let filter = InstanceFilter::default();
        let pagination = Pagination {
            offset: 0,
            limit: 100,
            sort_ascending: true,
        };
        storage
            .list_instances(&filter, &pagination)
            .await
            .unwrap()
            .len()
    }

    /// H-16: redelivering the same `command_id` (e.g. because the server
    /// never received our ack for it) must not re-execute the command's side
    /// effects. Uses `start_workflow` because it's the starkest observable
    /// case — a re-execution creates a second, distinct instance rather than
    /// silently converging like an idempotent state write would.
    #[tokio::test]
    async fn redelivered_command_is_not_executed_twice() {
        let body = serde_json::json!({
            "commands": [{
                "id": "cmd-1",
                "type": "start_workflow",
                "payload": { "sequence_name": "wf-a", "input": "{}" }
            }],
            "sync_interval_secs": 30
        })
        .to_string();

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let sync_url = format!("http://127.0.0.1:{port}/sync");

        let server_body = body.clone();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let _ = socket.read(&mut buf).await.unwrap();
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    server_body.len(),
                    server_body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let (reporter, storage, lifecycle) = setup(sync_url).await;
        seed_sequence(&storage, "wf-a").await;

        // Two independent sync cycles — each fetches its own copy of the
        // same command from the "server", simulating a redelivery of a
        // command whose ack the server never confirmed.
        assert!(reporter.sync_once(&storage, &lifecycle).await);
        assert!(reporter.sync_once(&storage, &lifecycle).await);
        assert!(reporter.http.get().is_some());

        server.await.unwrap();

        assert_eq!(
            count_instances(&storage).await,
            1,
            "a redelivered command must not start a second instance"
        );
    }

    #[tokio::test]
    async fn batch_cleanup_removes_only_the_acknowledged_rows() {
        let (reporter, _storage, _lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        sqlx::query(
            "INSERT INTO sync_outbox (entry_type, instance_id, payload) VALUES ('status', 'i1', '{}'), ('approval', 'i2', '{}'), ('status', 'i3', '{}')",
        )
        .execute(&reporter.pool)
        .await
        .unwrap();
        sqlx::query("INSERT INTO sync_command_acks (command_id) VALUES ('a1'), ('a2'), ('a3')")
            .execute(&reporter.pool)
            .await
            .unwrap();

        delete_outbox_rows(&reporter.pool, &[1, 3]).await.unwrap();
        delete_command_acks(&reporter.pool, &["a1".into(), "a3".into()])
            .await
            .unwrap();

        let remaining_outbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM sync_outbox")
            .fetch_one(&reporter.pool)
            .await
            .unwrap();
        let remaining_acks: Vec<String> =
            sqlx::query_scalar("SELECT command_id FROM sync_command_acks ORDER BY command_id")
                .fetch_all(&reporter.pool)
                .await
                .unwrap();
        assert_eq!(remaining_outbox, 1);
        assert_eq!(remaining_acks, vec!["a2"]);
    }

    /// Spawn a mock sync server that answers `count` requests, each with the
    /// corresponding body from `bodies` (cycling the last one if short).
    async fn spawn_mock_server(
        bodies: Vec<String>,
        count: usize,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("http://127.0.0.1:{port}/sync");
        let server = tokio::spawn(async move {
            for i in 0..count {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let _ = socket.read(&mut buf).await.unwrap();
                let body = &bodies[i.min(bodies.len() - 1)];
                let response = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
                    body.len(),
                    body
                );
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });
        (url, server)
    }

    async fn ack_count(pool: &SqlitePool, command_id: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM sync_command_acks WHERE command_id = ?")
            .bind(command_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    async fn marker_count(pool: &SqlitePool, command_id: &str) -> i64 {
        sqlx::query_scalar("SELECT COUNT(*) FROM sync_executed_commands WHERE command_id = ?")
            .bind(command_id)
            .fetch_one(pool)
            .await
            .unwrap()
    }

    /// A command that fails with a transient error (here: sequence not synced
    /// yet) must NOT be acked and must NOT keep its idempotency marker —
    /// otherwise the server's redelivery would be deduped away and the command
    /// would silently never execute.
    #[tokio::test]
    async fn retryable_command_failure_is_not_acked_and_reexecutes_on_redelivery() {
        let body = serde_json::json!({
            "commands": [{
                "id": "cmd-retry",
                "type": "start_workflow",
                "payload": { "sequence_name": "wf-late", "input": "{}" }
            }],
            "sync_interval_secs": 30
        })
        .to_string();
        let (sync_url, server) = spawn_mock_server(vec![body], 2).await;

        let (reporter, storage, lifecycle) = setup(sync_url).await;
        // NOTE: sequence "wf-late" is deliberately not seeded yet.

        reporter.sync_once(&storage, &lifecycle).await;

        assert_eq!(
            ack_count(&reporter.pool, "cmd-retry").await,
            0,
            "failed command must not be acked"
        );
        assert_eq!(
            marker_count(&reporter.pool, "cmd-retry").await,
            0,
            "idempotency marker must be rolled back so redelivery re-executes"
        );
        assert_eq!(count_instances(&storage).await, 0);

        // The server redelivers the un-acked command; now it can succeed.
        seed_sequence(&storage, "wf-late").await;
        reporter.sync_once(&storage, &lifecycle).await;

        server.await.unwrap();

        assert_eq!(
            count_instances(&storage).await,
            1,
            "redelivered command must execute"
        );
        assert_eq!(ack_count(&reporter.pool, "cmd-retry").await, 1);
        assert_eq!(marker_count(&reporter.pool, "cmd-retry").await, 1);
    }

    /// A permanently invalid command (unknown type, malformed payload) must
    /// be acked immediately — redelivering it would never succeed — and the
    /// idempotency marker must be retained so a redelivery is skipped rather
    /// than re-executed.
    #[tokio::test]
    async fn permanently_invalid_command_is_acked_and_not_retried() {
        let body = serde_json::json!({
            "commands": [
                {
                    "id": "cmd-unknown",
                    "type": "bogus_command",
                    "payload": {}
                },
                {
                    "id": "cmd-malformed",
                    "type": "cancel_instance",
                    "payload": { "instance_id": "not-a-uuid" }
                }
            ],
            "sync_interval_secs": 30
        })
        .to_string();
        let (sync_url, server) = spawn_mock_server(vec![body], 2).await;

        let (reporter, storage, lifecycle) = setup(sync_url).await;

        reporter.sync_once(&storage, &lifecycle).await;

        // Permanently invalid commands are acked so the server stops
        // redelivering, and their markers are retained.
        assert_eq!(ack_count(&reporter.pool, "cmd-unknown").await, 1);
        assert_eq!(ack_count(&reporter.pool, "cmd-malformed").await, 1);
        assert_eq!(marker_count(&reporter.pool, "cmd-unknown").await, 1);
        assert_eq!(marker_count(&reporter.pool, "cmd-malformed").await, 1);

        // Redelivery (server never confirmed the ack): deduped by the marker,
        // ack re-recorded, still exactly one marker row each.
        reporter.sync_once(&storage, &lifecycle).await;

        server.await.unwrap();

        assert_eq!(marker_count(&reporter.pool, "cmd-unknown").await, 1);
        assert_eq!(marker_count(&reporter.pool, "cmd-malformed").await, 1);
        assert_eq!(ack_count(&reporter.pool, "cmd-unknown").await, 1);
        assert_eq!(ack_count(&reporter.pool, "cmd-malformed").await, 1);
    }

    /// A buggy or malicious server must not be able to set a sync interval of
    /// 0 (a sync POST every tick) or an absurdly large one (sync never runs).
    #[tokio::test]
    async fn server_sync_interval_hint_is_clamped() {
        let bodies = vec![
            serde_json::json!({ "sync_interval_secs": 0 }).to_string(),
            serde_json::json!({ "sync_interval_secs": 100_000 }).to_string(),
        ];
        let (sync_url, server) = spawn_mock_server(bodies, 2).await;

        let (reporter, storage, lifecycle) = setup(sync_url).await;

        reporter.sync_once(&storage, &lifecycle).await;
        assert_eq!(
            reporter.sync_interval_secs.load(Ordering::Relaxed),
            u64::from(MIN_SYNC_INTERVAL_SECS),
            "zero interval must clamp up to the floor"
        );

        reporter.sync_once(&storage, &lifecycle).await;
        assert_eq!(
            reporter.sync_interval_secs.load(Ordering::Relaxed),
            u64::from(MAX_SYNC_INTERVAL_SECS),
            "huge interval must clamp down to the ceiling"
        );

        server.await.unwrap();
    }

    /// A push-triggered forced sync that fails (offline) must stay pending so
    /// the wakeup isn't wasted; it clears only after a successful round-trip.
    #[tokio::test]
    async fn force_sync_survives_failed_sync_and_clears_after_success() {
        // Unreachable server: the sync attempt fails.
        let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        reporter.on_push_received();
        reporter.sync_once(&storage, &lifecycle).await;
        assert!(
            reporter.has_forced_sync(),
            "force_sync must survive a failed sync attempt"
        );

        // Reachable server: the forced sync succeeds and the flag clears.
        let body = serde_json::json!({ "sync_interval_secs": 30 }).to_string();
        let (sync_url, server) = spawn_mock_server(vec![body], 1).await;
        let (reporter2, storage2, lifecycle2) = setup(sync_url).await;
        reporter2.on_push_received();
        reporter2.sync_once(&storage2, &lifecycle2).await;
        server.await.unwrap();
        assert!(
            !reporter2.has_forced_sync(),
            "force_sync must clear after a successful sync"
        );
    }

    /// Forced (push-triggered) syncs run immediately but cannot hot-loop the
    /// radio while offline; subsequent attempts wait for the retry interval.
    #[tokio::test]
    async fn should_sync_throttles_forced_retries() {
        let (reporter, _storage, _lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        reporter.on_push_received();
        assert!(reporter.should_sync());
        assert!(!reporter.should_sync());
    }

    #[tokio::test]
    async fn next_sync_delay_tracks_deadline_without_claiming_it() {
        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let start = chrono::Utc::now();
        let manual = Arc::new(ManualClock::new(start));
        let clock = SharedClock::from_arc(Arc::clone(&manual) as Arc<dyn Clock>);
        let reporter = SyncReporter::new_with_clock(
            pool,
            "http://127.0.0.1:1/sync".into(),
            "device-1".into(),
            "key".into(),
            clock,
        );

        assert_eq!(reporter.next_sync_delay(), Duration::from_secs(30));
        manual.advance(chrono::Duration::seconds(23));
        assert_eq!(reporter.next_sync_delay(), Duration::from_secs(7));
        assert!(!reporter.should_sync());
        manual.advance(chrono::Duration::seconds(7));
        assert_eq!(reporter.next_sync_delay(), Duration::ZERO);
        assert!(reporter.should_sync());
        assert_eq!(reporter.next_sync_delay(), Duration::from_secs(30));

        reporter.on_push_received();
        assert_eq!(reporter.next_sync_delay(), Duration::ZERO);
    }

    #[tokio::test]
    async fn push_arriving_during_sync_remains_pending() {
        let (reporter, _storage, _lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        reporter.on_push_received();
        let attempted = reporter.push_generation.load(Ordering::Acquire);

        reporter.on_push_received();
        reporter.mark_pushes_completed(attempted);

        assert!(reporter.has_forced_sync());
        assert!(reporter.should_sync());
    }
}
