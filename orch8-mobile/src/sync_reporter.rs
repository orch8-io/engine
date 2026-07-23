use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use sqlx::SqlitePool;
use tracing::{debug, warn};

use crate::lifecycle::InstanceLifecycleManager;
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::filter::{InstanceFilter, Pagination};
use orch8_types::ids::{BlockId, InstanceId};
use orch8_types::instance::InstanceState;
use orch8_types::sequence::{BlockDefinition, SequenceDefinition};

/// Bounds for the server-suggested sync interval. A buggy (or malicious, if
/// the API key/channel is ever abused) response must not be able to turn the
/// device into a sync hot-loop (`0` → a POST every tick) or stop syncing
/// entirely (a huge value).
const MIN_SYNC_INTERVAL_SECS: u32 = 5;
const MAX_SYNC_INTERVAL_SECS: u32 = 3600;

/// Batched status + approval reporter that syncs with the server on a
/// configurable tick cadence. Receives commands from the server and executes
/// them locally.
pub(crate) struct SyncReporter {
    pool: SqlitePool,
    http: reqwest::Client,
    sync_url: String,
    device_id: String,
    api_key: String,
    tick_interval_ms: u64,
    tick_counter: AtomicU64,
    sync_interval_ticks: AtomicU64,
    force_sync: AtomicBool,
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

impl SyncReporter {
    pub fn new(
        pool: SqlitePool,
        sync_url: String,
        device_id: String,
        api_key: String,
        tick_interval_ms: u64,
    ) -> Self {
        // The builder only uses constants, so failure is a programming error.
        #[allow(clippy::expect_used)]
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .dns_resolver(Arc::new(orch8_engine::handlers::builtin::SsrfGuardResolver))
            .build()
            .expect("reqwest client");

        let default_sync_interval_ticks = 30 * 1000 / tick_interval_ms.max(1);

        Self {
            pool,
            http,
            sync_url,
            device_id,
            api_key,
            tick_interval_ms,
            tick_counter: AtomicU64::new(0),
            sync_interval_ticks: AtomicU64::new(default_sync_interval_ticks),
            force_sync: AtomicBool::new(false),
        }
    }

    /// Called by host app when a silent push notification arrives.
    pub fn on_push_received(&self) {
        self.force_sync.store(true, Ordering::Relaxed);
    }

    /// Check if it's time to sync. Called from the tick loop.
    pub fn should_sync(&self) -> bool {
        let count = self.tick_counter.fetch_add(1, Ordering::Relaxed);
        if self.force_sync.load(Ordering::Relaxed) {
            // A forced sync retries promptly, but not in a hot loop: space
            // attempts ~5s apart so a push-triggered sync while offline
            // doesn't hammer the radio every tick.
            let retry_ticks = (5_000 / self.tick_interval_ms.max(1)).max(1);
            return count >= retry_ticks;
        }
        count >= self.sync_interval_ticks.load(Ordering::Relaxed)
    }

    /// Reset the tick counter after a sync attempt. Deliberately does NOT
    /// clear `force_sync`: a push-triggered sync that fails (offline, server
    /// error) must stay pending so the wakeup isn't wasted — the flag is
    /// cleared only after a successful round-trip in `sync_once`.
    fn reset_counter(&self) {
        self.tick_counter.store(0, Ordering::Relaxed);
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

    /// Write a status update to the outbox. Coalesces per `instance_id`.
    pub async fn queue_status(
        &self,
        instance_id: &str,
        sequence_name: Option<&str>,
        state: &str,
        current_step: Option<&str>,
        handler: Option<&str>,
        steps: Option<serde_json::Value>,
    ) {
        let payload = serde_json::json!({
            "instance_id": instance_id,
            "sequence_name": sequence_name,
            "state": state,
            "current_step": current_step,
            "handler": handler,
            "steps": steps,
            "timestamp": chrono::Utc::now().to_rfc3339(),
        });
        if let Err(e) = sqlx::query(
            "INSERT INTO sync_outbox (entry_type, instance_id, payload) VALUES ('status', ?, ?)",
        )
        .bind(instance_id)
        .bind(payload.to_string())
        .execute(&self.pool)
        .await
        {
            warn!(error = %e, instance_id, "failed to queue mobile status update");
        }
    }

    /// Write an approval request to the outbox.
    #[allow(clippy::too_many_arguments)]
    pub async fn queue_approval(
        &self,
        instance_id: &str,
        block_id: &str,
        sequence_name: Option<&str>,
        prompt: Option<&str>,
        choices: Option<&str>,
        store_as: Option<&str>,
        timeout_seconds: Option<i64>,
    ) {
        let payload = serde_json::json!({
            "instance_id": instance_id,
            "block_id": block_id,
            "sequence_name": sequence_name,
            "prompt": prompt,
            "choices": choices.and_then(|c| serde_json::from_str::<serde_json::Value>(c).ok()),
            "store_as": store_as,
            "timeout_seconds": timeout_seconds,
        });
        let key = format!("{instance_id}:{block_id}");
        if let Err(e) = sqlx::query(
            "INSERT OR IGNORE INTO sync_outbox (entry_type, instance_id, payload) VALUES ('approval', ?, ?)",
        )
        .bind(&key)
        .bind(payload.to_string())
        .execute(&self.pool)
        .await
        {
            warn!(error = %e, instance_id, block_id, "failed to queue mobile approval request");
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
        self.force_sync
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// Scan storage for active instances and queue status updates + approval
    /// requests. Coalescing in the outbox table ensures duplicates are harmless.
    pub async fn scan_and_queue(
        &self,
        storage: &Arc<dyn StorageBackend>,
        sequence_cache: &Arc<SequenceCache>,
    ) {
        let filter = InstanceFilter {
            states: Some(vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
                InstanceState::Completed,
                InstanceState::Failed,
                InstanceState::Cancelled,
            ]),
            ..Default::default()
        };
        let pagination = Pagination {
            offset: 0,
            limit: 100,
            sort_ascending: false,
        };

        let instances = match storage.list_instances(&filter, &pagination).await {
            Ok(list) => list,
            Err(e) => {
                debug!(error = %e, "scan_and_queue: failed to list instances");
                return;
            }
        };

        for inst in &instances {
            let id_str = inst.id.to_string();
            let current_step: Option<&BlockId> = inst.context.runtime.current_step.as_ref();
            let step_str = current_step.map(BlockId::as_str);
            let state_str = format!("{:?}", inst.state);

            let seq = sequence_cache
                .get_by_id(storage.as_ref(), inst.sequence_id)
                .await
                .ok();

            let seq_name = seq.as_ref().map(|s| s.name.clone());

            let handler = current_step
                .and_then(|step_id| seq.as_ref().and_then(|s| find_handler(&s.blocks, step_id)));

            let steps = build_steps_payload(storage.as_ref(), inst.id, seq.as_deref()).await;

            self.queue_status(
                &id_str,
                seq_name.as_deref(),
                &state_str,
                step_str,
                handler.as_deref(),
                steps,
            )
            .await;

            if inst.state == InstanceState::Waiting
                && let Some(step_id) = current_step
            {
                let wait_info = seq
                    .as_ref()
                    .and_then(|s| find_wait_info(&s.blocks, step_id));

                let (prompt, choices, store_as, timeout) =
                    wait_info.unwrap_or((None, None, None, None));

                self.queue_approval(
                    &id_str,
                    step_id.as_str(),
                    seq_name.as_deref(),
                    prompt.as_deref(),
                    choices.as_deref(),
                    store_as.as_deref(),
                    timeout,
                )
                .await;
            }
        }
    }

    /// Execute one sync cycle: drain outbox, POST to server, process commands.
    #[allow(clippy::too_many_lines)]
    pub async fn sync_once(
        &self,
        storage: &Arc<dyn StorageBackend>,
        lifecycle: &Arc<InstanceLifecycleManager>,
    ) {
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
                self.reset_counter();
                return;
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
            .http
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
                self.reset_counter();
                return;
            }
            Err(e) => {
                debug!(error = %e, "sync request error (offline?)");
                self.reset_counter();
                return;
            }
        };

        let sync_resp: SyncResponse = match resp.json().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "failed to parse sync response");
                self.reset_counter();
                return;
            }
        };

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
        let new_interval_ticks =
            (u64::from(clamped_secs) * 1000 / self.tick_interval_ms.max(1)).max(1);
        self.sync_interval_ticks
            .store(new_interval_ticks, Ordering::Relaxed);

        debug!(
            status_sent = sent_status_ids.len(),
            approvals_sent = sent_approval_ids.len(),
            delegations_sent = sent_delegation_ids.len(),
            commands_received = sync_resp.commands.len(),
            next_sync_secs = clamped_secs,
            "sync complete"
        );

        // Successful round-trip: clear any pending push-triggered forced sync.
        self.force_sync.store(false, Ordering::Relaxed);
        self.reset_counter();
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

async fn build_steps_payload(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    seq: Option<&SequenceDefinition>,
) -> Option<serde_json::Value> {
    let tree = match storage.get_execution_tree(instance_id).await {
        Ok(tree) => tree,
        Err(e) => {
            debug!(instance_id = %instance_id, error = %e, "failed to load execution tree for steps payload");
            return None;
        }
    };

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
                    n.state.to_string(),
                    n.started_at.map(|t| t.to_rfc3339()),
                    n.completed_at.map(|t| t.to_rfc3339()),
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
        for node in &tree {
            entries.push(serde_json::json!({
                "block_id": node.block_id.as_str(),
                "block_type": node.block_type.to_string(),
                "state": node.state.to_string(),
                "handler": null,
                "started_at": node.started_at.map(|t| t.to_rfc3339()),
                "completed_at": node.completed_at.map(|t| t.to_rfc3339()),
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
        let sequence_cache = Arc::new(SequenceCache::new(50, Duration::from_secs(3600)));
        let lifecycle = Arc::new(InstanceLifecycleManager::new(
            storage.clone(),
            mobile_storage,
            sequence_cache,
            10,
        ));

        let pool = SqlitePool::connect("sqlite::memory:").await.unwrap();
        let reporter = SyncReporter::new(
            pool,
            sync_url,
            "device-1".to_string(),
            "key".to_string(),
            1000,
        );
        reporter.init_tables().await;

        (reporter, storage, lifecycle)
    }

    #[tokio::test]
    async fn sync_read_failure_resets_counter_without_sending_partial_data() {
        let (reporter, storage, lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        reporter.tick_counter.store(42, Ordering::Relaxed);
        reporter.pool.close().await;

        reporter.sync_once(&storage, &lifecycle).await;

        assert_eq!(reporter.tick_counter.load(Ordering::Relaxed), 0);
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
        reporter.sync_once(&storage, &lifecycle).await;
        reporter.sync_once(&storage, &lifecycle).await;

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

        assert_eq!(ack_count(&reporter.pool, "cmd-retry").await, 0, "failed command must not be acked");
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

        assert_eq!(count_instances(&storage).await, 1, "redelivered command must execute");
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

        // setup() uses tick_interval_ms = 1000, so ticks == seconds.
        let (reporter, storage, lifecycle) = setup(sync_url).await;

        reporter.sync_once(&storage, &lifecycle).await;
        assert_eq!(
            reporter.sync_interval_ticks.load(Ordering::Relaxed),
            u64::from(MIN_SYNC_INTERVAL_SECS),
            "zero interval must clamp up to the floor"
        );

        reporter.sync_once(&storage, &lifecycle).await;
        assert_eq!(
            reporter.sync_interval_ticks.load(Ordering::Relaxed),
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
            reporter.force_sync.load(Ordering::Relaxed),
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
            !reporter2.force_sync.load(Ordering::Relaxed),
            "force_sync must clear after a successful sync"
        );
    }

    /// Forced (push-triggered) syncs retry promptly but not in a hot loop:
    /// attempts are spaced ~5s apart. `setup()` uses `tick_interval_ms` = 1000,
    /// so the retry floor is 5 ticks.
    #[tokio::test]
    async fn should_sync_throttles_forced_retries() {
        let (reporter, _storage, _lifecycle) = setup("http://127.0.0.1:1/sync".into()).await;
        reporter.on_push_received();
        let results: Vec<bool> = (0..6).map(|_| reporter.should_sync()).collect();
        assert_eq!(results, vec![false, false, false, false, false, true]);
    }
}
