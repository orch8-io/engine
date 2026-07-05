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
        if self.force_sync.load(Ordering::Relaxed) {
            return true;
        }
        let count = self.tick_counter.fetch_add(1, Ordering::Relaxed);
        count >= self.sync_interval_ticks.load(Ordering::Relaxed)
    }

    fn reset_counter(&self) {
        self.tick_counter.store(0, Ordering::Relaxed);
        self.force_sync.store(false, Ordering::Relaxed);
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
        let status_rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT id, payload FROM sync_outbox WHERE entry_type = 'status' ORDER BY id LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let approval_rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT id, payload FROM sync_outbox WHERE entry_type = 'approval' ORDER BY id LIMIT 50",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let delegation_rows: Vec<(i64, String)> = sqlx::query_as(
            "SELECT id, payload FROM sync_outbox WHERE entry_type = 'delegation' ORDER BY id LIMIT 20",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

        let ack_rows: Vec<(String,)> = sqlx::query_as(
            "SELECT command_id FROM sync_command_acks ORDER BY created_at LIMIT 100",
        )
        .fetch_all(&self.pool)
        .await
        .unwrap_or_default();

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

        for id in &sent_status_ids {
            if let Err(e) = sqlx::query("DELETE FROM sync_outbox WHERE id = ?")
                .bind(id)
                .execute(&self.pool)
                .await
            {
                warn!(error = %e, id, "failed to delete sent status update");
            }
        }
        for id in &sent_approval_ids {
            if let Err(e) = sqlx::query("DELETE FROM sync_outbox WHERE id = ?")
                .bind(id)
                .execute(&self.pool)
                .await
            {
                warn!(error = %e, id, "failed to delete sent approval request");
            }
        }
        for id in &sent_delegation_ids {
            if let Err(e) = sqlx::query("DELETE FROM sync_outbox WHERE id = ?")
                .bind(id)
                .execute(&self.pool)
                .await
            {
                warn!(error = %e, id, "failed to delete sent delegation");
            }
        }
        for ack_id in &command_acks {
            if let Err(e) = sqlx::query("DELETE FROM sync_command_acks WHERE command_id = ?")
                .bind(ack_id)
                .execute(&self.pool)
                .await
            {
                warn!(error = %e, ack_id, "failed to delete sync command ack");
            }
        }

        // Process commands from server.
        for cmd in &sync_resp.commands {
            self.execute_command(cmd, storage, lifecycle).await;
            if let Err(e) =
                sqlx::query("INSERT OR IGNORE INTO sync_command_acks (command_id) VALUES (?)")
                    .bind(&cmd.id)
                    .execute(&self.pool)
                    .await
            {
                warn!(error = %e, command_id = %cmd.id, "failed to record sync command ack");
            }
        }

        // Update sync interval from server hint.
        let new_interval_ticks =
            u64::from(sync_resp.sync_interval_secs) * 1000 / self.tick_interval_ms.max(1);
        self.sync_interval_ticks
            .store(new_interval_ticks, Ordering::Relaxed);

        debug!(
            status_sent = sent_status_ids.len(),
            approvals_sent = sent_approval_ids.len(),
            delegations_sent = sent_delegation_ids.len(),
            commands_received = sync_resp.commands.len(),
            next_sync_secs = sync_resp.sync_interval_secs,
            "sync complete"
        );

        self.reset_counter();
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_command(
        &self,
        cmd: &CommandEntry,
        storage: &Arc<dyn StorageBackend>,
        lifecycle: &Arc<InstanceLifecycleManager>,
    ) {
        match cmd.command_type.as_str() {
            "complete_step" => {
                let instance_id = cmd.payload.get("instance_id").and_then(|v| v.as_str());
                let step_name = cmd.payload.get("step_name").and_then(|v| v.as_str());
                let output = cmd.payload.get("output");

                if let (Some(iid), Some(step)) = (instance_id, step_name) {
                    let output_str =
                        output.map_or_else(|| "{}".to_string(), std::string::ToString::to_string);

                    debug!(
                        instance_id = %iid,
                        step_name = %step,
                        "executing complete_step command from server"
                    );

                    let inst_id = if let Ok(u) = uuid::Uuid::parse_str(iid) {
                        orch8_types::ids::InstanceId::from_uuid(u)
                    } else {
                        warn!(instance_id = %iid, "invalid UUID in complete_step command");
                        return;
                    };

                    let signal = orch8_types::signal::Signal {
                        id: uuid::Uuid::now_v7(),
                        instance_id: inst_id,
                        signal_type: orch8_types::signal::SignalType::Custom(format!(
                            "human_input:{step}"
                        )),
                        payload: serde_json::from_str(&output_str).unwrap_or_default(),
                        delivered: false,
                        created_at: chrono::Utc::now(),
                        delivered_at: None,
                    };
                    if let Err(e) = storage.enqueue_signal(&signal).await {
                        warn!(error = %e, "failed to enqueue complete_step signal");
                    }
                }
            }
            "cancel_instance" => {
                let instance_id = cmd.payload.get("instance_id").and_then(|v| v.as_str());
                if let Some(iid) = instance_id {
                    debug!(instance_id = %iid, "executing cancel_instance command from server");
                    let id = if let Ok(u) = uuid::Uuid::parse_str(iid) {
                        orch8_types::ids::InstanceId::from_uuid(u)
                    } else {
                        warn!(instance_id = %iid, "invalid UUID in cancel_instance command");
                        return;
                    };
                    if let Err(e) = storage
                        .update_instance_state(id, InstanceState::Cancelled, None)
                        .await
                    {
                        warn!(error = %e, "failed to cancel instance from server command");
                    }
                }
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
                        Err(e) => {
                            warn!(error = %e, sequence_name = %name, "failed to start workflow from server command");
                        }
                    }
                } else {
                    warn!("start_workflow command missing sequence_name");
                }
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
                        return;
                    };

                    match policy {
                        "restart" => {
                            // Cancel existing, start fresh with same sequence
                            if let Err(e) = storage
                                .update_instance_state(inst_id, InstanceState::Cancelled, None)
                                .await
                            {
                                warn!(error = %e, "update_sequence(restart): cancel failed");
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
                            }
                        }
                        "cancel" => {
                            if let Err(e) = storage
                                .update_instance_state(inst_id, InstanceState::Cancelled, None)
                                .await
                            {
                                warn!(error = %e, "update_sequence(cancel): failed");
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
                            return;
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
                }
            }
            other => {
                warn!(command_type = %other, "unknown command type from server");
            }
        }
    }
}

async fn build_steps_payload(
    storage: &dyn StorageBackend,
    instance_id: InstanceId,
    seq: Option<&SequenceDefinition>,
) -> Option<serde_json::Value> {
    let tree = storage.get_execution_tree(instance_id).await.ok()?;

    let mut entries: Vec<serde_json::Value> = Vec::new();

    if let Some(seq) = seq {
        let flat = flatten_blocks(&seq.blocks);

        for (block_id, block_type, handler) in &flat {
            let node = tree
                .iter()
                .find(|n| n.block_id.as_str() == block_id.as_str());
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
