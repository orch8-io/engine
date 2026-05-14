//! `orch8-mobile` — Embedded workflow engine for mobile apps.
//!
//! Runs orch8 workflows on-device with `SQLite` storage, enabling server-configurable
//! user journeys (onboarding, promotions, feature flows) without app store deployments.
#![allow(
    // UniFFI requires owned String params in exported functions — can't use &str.
    clippy::needless_pass_by_value,
    // UniFFI-generated code uses these patterns.
    clippy::used_underscore_binding,
)]

mod config;
mod error;
mod handlers;
mod runtime;
mod storage;
mod sync;
mod telemetry;

use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
use std::time::Duration;

use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use orch8_engine::handlers::HandlerRegistry;
use orch8_engine::scheduler::{tick_once, TickOnceResult};
use orch8_engine::sequence_cache::SequenceCache;
use orch8_storage::StorageBackend;
use orch8_types::ids::{InstanceId, Namespace, TenantId};
use orch8_types::instance::{InstanceState, TaskInstance};
use orch8_types::sequence::SequenceDefinition;

pub use crate::config::MobileEngineConfig;
pub use crate::error::{HandlerError, MobileError, SyncError, TokenProvider};
pub use crate::handlers::{EngineListener, StepHandler};
pub use crate::sync::{RootKey, SyncResult};
pub use crate::telemetry::{DeviceContext, FlushResult, TelemetryEventRecord};

uniffi::setup_scaffolding!();

/// Result of a single tick execution, returned to the host app.
#[derive(Debug, Clone, uniffi::Record)]
pub struct TickResult {
    pub instances_advanced: u32,
    pub steps_executed: u32,
    pub has_pending_work: bool,
}

impl From<TickOnceResult> for TickResult {
    fn from(r: TickOnceResult) -> Self {
        Self {
            instances_advanced: r.instances_advanced,
            steps_executed: r.steps_executed,
            has_pending_work: r.has_pending_work,
        }
    }
}

/// Summary of a stored sequence, returned by `loaded_sequences()`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct SequenceInfo {
    pub name: String,
    pub version: i32,
}

/// Summary of a running instance, returned by `active_instances()`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct InstanceSummary {
    pub instance_id: String,
    pub sequence_name: String,
    pub state: String,
    pub created_at: String,
}

/// Snapshot of a single instance's state.
#[derive(Debug, Clone, uniffi::Record)]
pub struct InstanceState_ {
    pub instance_id: String,
    pub sequence_name: String,
    pub state: String,
    pub context: String,
    pub created_at: String,
    pub updated_at: String,
}

/// The mobile workflow engine. Opaque handle for the host app.
#[derive(uniffi::Object)]
pub struct MobileEngine {
    storage: Arc<dyn StorageBackend>,
    #[allow(dead_code)]
    sqlite: Arc<orch8_storage::sqlite::SqliteStorage>,
    mobile_storage: Arc<storage::MobileStorage>,
    handlers: StdRwLock<Arc<HandlerRegistry>>,
    config: MobileEngineConfig,
    scheduler_config: orch8_types::config::SchedulerConfig,
    semaphore: Arc<Semaphore>,
    sequence_cache: Arc<SequenceCache>,
    cancel: CancellationToken,
    tick_mutex: Arc<Mutex<()>>,
    tick_loop_cancel: StdMutex<CancellationToken>,
    runtime: runtime::MobileRuntime,
    listener: Arc<Mutex<Option<Arc<dyn EngineListener>>>>,
    dedup_keys: Arc<Mutex<HashMap<String, String>>>,
    notified_terminals: Arc<Mutex<HashSet<String>>>,
    notified_waiting: Arc<Mutex<HashSet<String>>>,
    dirty: Arc<AtomicBool>,
    telemetry: Arc<telemetry::TelemetryManager>,
    sync_orchestrator: Arc<Mutex<Option<sync::SyncOrchestrator>>>,
}

#[uniffi::export]
impl MobileEngine {
    /// Create a new mobile engine backed by a `SQLite` database at `db_path`.
    #[uniffi::constructor]
    pub fn new(db_path: String, config: MobileEngineConfig) -> Result<Arc<Self>, MobileError> {
        let rt = runtime::MobileRuntime::new().map_err(|e| MobileError::Engine { message: e })?;

        let (storage, sqlite) = rt.block_on(async {
            let s = orch8_storage::sqlite::SqliteStorage::file_mobile(&db_path)
                .await
                .map_err(MobileError::from)?;
            let arc = Arc::new(s);
            Ok::<_, MobileError>((arc.clone() as Arc<dyn StorageBackend>, arc))
        })?;

        let mobile_storage = Arc::new(storage::MobileStorage::new(sqlite.clone()));

        // Hydrate dedup keys from persistent storage.
        let dedup_map = rt.block_on(async {
            // We don't have a list_all_dedup method, so start with empty.
            // In a full implementation, we'd query all rows from mobile_dedup.
            HashMap::<String, String>::new()
        });

        let scheduler_config = config.to_scheduler_config();
        let semaphore = Arc::new(Semaphore::new(config.max_concurrent_steps as usize));
        let sequence_cache = Arc::new(SequenceCache::new(
            u64::from(config.max_stored_sequences),
            Duration::from_secs(3600),
        ));

        let telemetry_mgr = Arc::new(telemetry::TelemetryManager::new(
            mobile_storage.clone(),
            config.telemetry_enabled,
            DeviceContext {
                device_id: String::new(),
                os_name: String::new(),
                os_version: String::new(),
                app_version: String::new(),
                sdk_version: config.sdk_version.clone(),
            },
        ));

        // Build sync orchestrator if root key is configured.
        let sync_orch = if config.root_public_key.is_empty() {
            None
        } else {
            match sync::RootKey::from_base64(&config.root_public_key) {
                Ok(root_key) => Some(sync::SyncOrchestrator::new(
                    mobile_storage.clone(),
                    storage.clone(),
                    root_key,
                    config.sdk_version.clone(),
                )),
                Err(e) => {
                    warn!(error = %e, "invalid root_public_key — sync disabled");
                    None
                }
            }
        };

        info!(db_path = %db_path, "mobile engine initialized");

        Ok(Arc::new(Self {
            storage,
            sqlite,
            mobile_storage,
            handlers: StdRwLock::new(Arc::new(HandlerRegistry::new())),
            config,
            scheduler_config,
            semaphore,
            sequence_cache,
            cancel: CancellationToken::new(),
            tick_mutex: Arc::new(Mutex::new(())),
            tick_loop_cancel: StdMutex::new(CancellationToken::new()),
            runtime: rt,
            listener: Arc::new(Mutex::new(None)),
            dedup_keys: Arc::new(Mutex::new(dedup_map)),
            notified_terminals: Arc::new(Mutex::new(HashSet::new())),
            notified_waiting: Arc::new(Mutex::new(HashSet::new())),
            dirty: Arc::new(AtomicBool::new(false)),
            telemetry: telemetry_mgr,
            sync_orchestrator: Arc::new(Mutex::new(sync_orch)),
        }))
    }

    /// Register a native step handler. Must be called before `resume()`.
    pub fn register_handler(
        &self,
        name: String,
        handler: Arc<dyn StepHandler>,
    ) -> Result<(), MobileError> {
        let timeout = Duration::from_millis(self.config.handler_timeout_ms);
        let mut guard = self
            .handlers
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let registry = Arc::get_mut(&mut guard).ok_or_else(|| MobileError::Engine {
            message: "cannot register handlers after engine has started".to_string(),
        })?;
        handlers::register_foreign_handler(registry, &name, handler, timeout);
        debug!(handler = %name, "registered mobile handler");
        Ok(())
    }

    /// Set the event listener for engine lifecycle events.
    pub fn set_listener(&self, listener: Arc<dyn EngineListener>) {
        self.runtime.block_on(async {
            *self.listener.lock().await = Some(listener);
        });
    }

    /// Execute a single tick.
    pub fn tick_once(&self) -> Result<TickResult, MobileError> {
        let handlers = self
            .handlers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        self.runtime.block_on(async {
            let _guard = self.tick_mutex.lock().await;

            if self.cancel.is_cancelled() {
                return Err(MobileError::Shutdown);
            }

            let result = tick_once(
                &self.storage,
                &handlers,
                &self.semaphore,
                &self.scheduler_config,
                &self.sequence_cache,
                &self.cancel,
            )
            .await?;

            self.fire_terminal_events().await;
            self.fire_step_pending_events().await;
            self.gc_expired_instances().await;

            Ok(result.into())
        })
    }

    /// Start a foreground tick loop.
    pub fn resume(&self) {
        {
            let mut guard = self
                .tick_loop_cancel
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard.cancel();
            *guard = CancellationToken::new();
        }

        if self.dirty.swap(false, Ordering::AcqRel) {
            info!("dirty flag set — recovering stale instances before resuming");
            self.runtime.block_on(async {
                let threshold = self.scheduler_config.stale_instance_threshold_secs;
                if let Err(e) = orch8_engine::recovery::recover_stale_instances(
                    self.storage.as_ref(),
                    threshold,
                )
                .await
                {
                    warn!(error = %e, "stale instance recovery failed");
                }
            });
        }

        let storage = Arc::clone(&self.storage);
        let handlers = self
            .handlers
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let semaphore = Arc::clone(&self.semaphore);
        let config = self.scheduler_config.clone();
        let seq_cache = Arc::clone(&self.sequence_cache);
        let cancel = self.cancel.clone();
        let tick_mutex = Arc::clone(&self.tick_mutex);
        let loop_cancel = self
            .tick_loop_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let tick_interval = Duration::from_millis(self.config.tick_interval_ms);
        let tick_budget = Duration::from_millis(self.config.max_tick_duration_ms);
        let listener = Arc::clone(&self.listener);
        let dedup_keys = Arc::clone(&self.dedup_keys);
        let notified = Arc::clone(&self.notified_terminals);
        let notified_waiting = Arc::clone(&self.notified_waiting);
        let seq_cache_for_events = Arc::clone(&self.sequence_cache);
        let max_lifetime_secs = self.config.max_instance_lifetime_secs;
        let storage_for_gc = Arc::clone(&self.storage);

        self.runtime.handle().spawn(async move {
            let mut ticker = tokio::time::interval(tick_interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut tick_count: u64 = 0;

            loop {
                tokio::select! {
                    () = cancel.cancelled() => break,
                    () = loop_cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        let _guard = tick_mutex.lock().await;
                        let tick_result = tokio::time::timeout(
                            tick_budget,
                            tick_once(&storage, &handlers, &semaphore, &config, &seq_cache, &cancel),
                        ).await;
                        match tick_result {
                            Ok(Ok(_)) => {}
                            Ok(Err(e)) => warn!(error = %e, "mobile tick error"),
                            Err(_) => {
                                warn!(budget_ms = tick_budget.as_millis(), "tick exceeded duration budget");
                            }
                        }
                        fire_terminal_events_inner(
                            &storage, &listener, &dedup_keys, &notified,
                        ).await;
                        fire_step_pending_events_inner(
                            &storage, &listener, &seq_cache_for_events, &notified_waiting,
                        ).await;

                        tick_count += 1;
                        if tick_count.is_multiple_of(60) {
                            gc_expired_instances_inner(&storage_for_gc, max_lifetime_secs).await;
                        }
                    }
                }
            }
            info!("mobile tick loop stopped");
        });

        info!("mobile tick loop started");
    }

    /// Pause the foreground tick loop.
    pub fn pause(&self) {
        self.tick_loop_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancel();
        self.runtime.block_on(async {
            let timeout = Duration::from_millis(self.config.max_tick_duration_ms);
            if tokio::time::timeout(timeout, self.tick_mutex.lock())
                .await
                .is_ok()
            {
                debug!("mobile engine paused cleanly");
            } else {
                self.dirty.store(true, Ordering::Release);
                warn!("pause timed out waiting for current tick — marked dirty for recovery");
            }
        });
    }

    /// Start a new workflow instance.
    pub fn start(
        &self,
        sequence_name: String,
        input: String,
        dedup_key: Option<String>,
    ) -> Result<String, MobileError> {
        self.runtime.block_on(async {
            if let Some(ref key) = dedup_key {
                let dedup = self.dedup_keys.lock().await;
                if let Some(existing_id) = dedup.get(key) {
                    return Ok(existing_id.clone());
                }
                // Also check persistent storage.
                drop(dedup);
                if let Ok(Some(persisted_id)) = self.mobile_storage.get_dedup_instance(key).await {
                    self.dedup_keys
                        .lock()
                        .await
                        .insert(key.clone(), persisted_id.clone());
                    return Ok(persisted_id);
                }
            }

            let active = self.count_active_instances().await?;
            if active >= u64::from(self.config.max_concurrent_instances) {
                return Err(MobileError::ResourceLimit {
                    message: format!(
                        "max concurrent instances ({}) reached",
                        self.config.max_concurrent_instances
                    ),
                });
            }

            let tenant = TenantId::new("mobile").expect("valid tenant");
            let ns = Namespace::new("default");
            let seq = self
                .storage
                .get_sequence_by_name(&tenant, &ns, &sequence_name, None)
                .await?
                .ok_or_else(|| MobileError::NotFound {
                    message: format!("sequence '{sequence_name}' not found"),
                })?;

            let input_data: serde_json::Value = serde_json::from_str(&input)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new()));

            let instance_id = InstanceId::new();
            let now = chrono::Utc::now();

            let context = orch8_types::context::ExecutionContext {
                data: input_data,
                ..orch8_types::context::ExecutionContext::default()
            };

            let instance = TaskInstance {
                id: instance_id,
                sequence_id: seq.id,
                tenant_id: tenant,
                namespace: ns,
                state: InstanceState::Scheduled,
                next_fire_at: Some(now),
                priority: orch8_types::instance::Priority::Normal,
                timezone: "UTC".to_string(),
                metadata: serde_json::json!({}),
                context,
                concurrency_key: None,
                max_concurrency: None,
                idempotency_key: dedup_key.clone(),
                session_id: None,
                parent_instance_id: None,
                created_at: now,
                updated_at: now,
            };

            self.storage.create_instance(&instance).await?;

            let id_str = instance_id.to_string();

            if let Some(key) = dedup_key {
                self.dedup_keys
                    .lock()
                    .await
                    .insert(key.clone(), id_str.clone());
                let _ = self.mobile_storage.set_dedup(&key, &id_str).await;
            }

            info!(instance_id = %id_str, sequence = %sequence_name, "started mobile instance");
            Ok(id_str)
        })
    }

    /// Cancel a running instance.
    pub fn cancel_instance(&self, instance_id: String) -> Result<(), MobileError> {
        self.runtime.block_on(async {
            let id = parse_instance_id(&instance_id)?;
            self.storage
                .update_instance_state(id, InstanceState::Cancelled, None)
                .await?;

            let mut dedup = self.dedup_keys.lock().await;
            dedup.retain(|_, v| v != &instance_id);
            drop(dedup);
            let _ = self.mobile_storage.remove_dedup(&instance_id).await;

            info!(instance_id = %instance_id, "cancelled mobile instance");
            Ok(())
        })
    }

    /// Get the state of a specific instance.
    pub fn get_instance(&self, instance_id: String) -> Result<InstanceState_, MobileError> {
        self.runtime.block_on(async {
            let id = parse_instance_id(&instance_id)?;
            let inst =
                self.storage
                    .get_instance(id)
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

            Ok(InstanceState_ {
                instance_id: inst.id.to_string(),
                sequence_name: seq,
                state: format!("{:?}", inst.state),
                context: serde_json::to_string(&inst.context.data).unwrap_or_default(),
                created_at: inst.created_at.to_rfc3339(),
                updated_at: inst.updated_at.to_rfc3339(),
            })
        })
    }

    /// List all non-terminal instances.
    pub fn active_instances(&self) -> Result<Vec<InstanceSummary>, MobileError> {
        self.runtime.block_on(async {
            let filter = orch8_types::filter::InstanceFilter {
                states: Some(vec![
                    InstanceState::Scheduled,
                    InstanceState::Running,
                    InstanceState::Waiting,
                    InstanceState::Paused,
                ]),
                ..Default::default()
            };
            let pagination = orch8_types::filter::Pagination {
                offset: 0,
                limit: 100,
                sort_ascending: false,
            };

            let instances = self.storage.list_instances(&filter, &pagination).await?;
            let mut summaries = Vec::with_capacity(instances.len());

            for inst in instances {
                let seq_name = self
                    .sequence_cache
                    .get_by_id(self.storage.as_ref(), inst.sequence_id)
                    .await
                    .ok()
                    .map(|s| s.name.clone())
                    .unwrap_or_default();

                summaries.push(InstanceSummary {
                    instance_id: inst.id.to_string(),
                    sequence_name: seq_name,
                    state: format!("{:?}", inst.state),
                    created_at: inst.created_at.to_rfc3339(),
                });
            }

            Ok(summaries)
        })
    }

    /// Complete a step that transitioned to Waiting state.
    pub fn complete_step(
        &self,
        instance_id: String,
        _step_name: String,
        output: String,
    ) -> Result<(), MobileError> {
        self.runtime.block_on(async {
            let id = parse_instance_id(&instance_id)?;

            let inst =
                self.storage
                    .get_instance(id)
                    .await?
                    .ok_or_else(|| MobileError::NotFound {
                        message: format!("instance '{instance_id}' not found"),
                    })?;

            if inst.state != InstanceState::Waiting {
                return Err(MobileError::InvalidInput {
                    message: format!("instance is in {:?} state, expected Waiting", inst.state),
                });
            }

            let output_value: serde_json::Value = serde_json::from_str(&output)?;

            let mut ctx = inst.context.clone();
            if let serde_json::Value::Object(map) = output_value {
                if let serde_json::Value::Object(ref mut data) = ctx.data {
                    data.extend(map);
                }
            }
            self.storage.update_instance_context(id, &ctx).await?;

            self.storage
                .update_instance_state(id, InstanceState::Scheduled, Some(chrono::Utc::now()))
                .await?;

            debug!(instance_id = %instance_id, "completed waiting step");
            Ok(())
        })
    }

    /// Load a sequence directly from a JSON string, bypassing sync.
    pub fn load_sequence_from_json(&self, json: String) -> Result<(), MobileError> {
        self.runtime.block_on(async {
            if json.len() as u64 > self.config.max_sequence_size_bytes {
                return Err(MobileError::ResourceLimit {
                    message: format!(
                        "sequence size ({} bytes) exceeds limit ({} bytes)",
                        json.len(),
                        self.config.max_sequence_size_bytes
                    ),
                });
            }

            let seq: SequenceDefinition = serde_json::from_str(&json)?;

            {
                let tenant = TenantId::new("mobile").expect("valid tenant");
                let ns = Namespace::new("default");
                let existing = self
                    .storage
                    .list_sequences(
                        Some(&tenant),
                        Some(&ns),
                        self.config.max_stored_sequences.saturating_add(1),
                        0,
                    )
                    .await?;
                let already_present = existing.iter().any(|s| s.name == seq.name);
                if !already_present {
                    #[allow(clippy::cast_possible_truncation)]
                    let count = existing.len() as u32;
                    if count >= self.config.max_stored_sequences {
                        return Err(MobileError::ResourceLimit {
                            message: format!(
                                "stored sequence count ({}) would exceed limit ({})",
                                count, self.config.max_stored_sequences
                            ),
                        });
                    }
                }
            }

            self.storage.create_sequence(&seq).await?;
            info!(name = %seq.name, version = seq.version, "loaded sequence from JSON");
            Ok(())
        })
    }

    /// List all sequences stored locally.
    pub fn loaded_sequences(&self) -> Result<Vec<SequenceInfo>, MobileError> {
        self.runtime.block_on(async {
            let tenant = TenantId::new("mobile").expect("valid tenant");
            let ns = Namespace::new("default");
            let seqs = self
                .storage
                .list_sequences(Some(&tenant), Some(&ns), 100, 0)
                .await?;
            Ok(seqs
                .into_iter()
                .map(|s| SequenceInfo {
                    name: s.name,
                    version: s.version,
                })
                .collect())
        })
    }

    /// Sync sequences from the remote manifest.
    pub fn sync(
        &self,
        manifest_url: String,
        token_provider: Option<Arc<dyn TokenProvider>>,
    ) -> Result<SyncResult, MobileError> {
        let handler_names: HashSet<String> = {
            let guard = self
                .handlers
                .read()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            guard
                .handler_names()
                .into_iter()
                .map(String::from)
                .collect()
        };
        self.runtime.block_on(async {
            let orch_guard = self.sync_orchestrator.lock().await;
            let orch = orch_guard.as_ref().ok_or_else(|| MobileError::Engine {
                message: "sync not configured — set root_public_key in config".to_string(),
            })?;

            let auth = match token_provider {
                Some(provider) => sync::SyncAuth::Bearer(provider.current_token()),
                None => sync::SyncAuth::UrlToken,
            };

            let result = orch.sync(&manifest_url, &auth, &handler_names).await;

            // Piggyback telemetry flush on successful sync.
            if result.is_ok() && self.config.telemetry_enabled {
                let _ = self
                    .telemetry
                    .flush(&format!(
                        "{}/telemetry/mobile",
                        manifest_url.trim_end_matches("/manifest.json")
                    ))
                    .await;
            }

            result
        })
    }

    /// Flush buffered telemetry to the remote endpoint.
    pub fn flush_telemetry(&self, endpoint_url: String) -> Result<FlushResult, MobileError> {
        self.runtime
            .block_on(async { self.telemetry.flush(&endpoint_url).await })
    }

    /// Set device context for telemetry.
    pub fn set_device_context(&self, ctx: DeviceContext) {
        self.telemetry.set_device_context(ctx);
    }

    /// Shut down the engine.
    pub fn shutdown(&self) {
        info!("mobile engine shutting down");
        self.cancel.cancel();
        self.tick_loop_cancel
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .cancel();
    }
}

impl MobileEngine {
    async fn count_active_instances(&self) -> Result<u64, MobileError> {
        let filter = orch8_types::filter::InstanceFilter {
            states: Some(vec![
                InstanceState::Scheduled,
                InstanceState::Running,
                InstanceState::Waiting,
            ]),
            ..Default::default()
        };
        let count = self.storage.count_instances(&filter).await?;
        Ok(count)
    }

    async fn fire_terminal_events(&self) {
        fire_terminal_events_inner(
            &self.storage,
            &self.listener,
            &self.dedup_keys,
            &self.notified_terminals,
        )
        .await;
    }

    async fn fire_step_pending_events(&self) {
        fire_step_pending_events_inner(
            &self.storage,
            &self.listener,
            &self.sequence_cache,
            &self.notified_waiting,
        )
        .await;
    }

    async fn gc_expired_instances(&self) {
        gc_expired_instances_inner(&self.storage, self.config.max_instance_lifetime_secs).await;
    }
}

async fn fire_terminal_events_inner(
    storage: &Arc<dyn StorageBackend>,
    listener: &Arc<Mutex<Option<Arc<dyn EngineListener>>>>,
    dedup_keys: &Arc<Mutex<HashMap<String, String>>>,
    notified: &Arc<Mutex<HashSet<String>>>,
) {
    let listener = {
        let guard = listener.lock().await;
        match guard.clone() {
            Some(l) => l,
            None => return,
        }
    };

    let filter = orch8_types::filter::InstanceFilter {
        states: Some(vec![
            InstanceState::Completed,
            InstanceState::Failed,
            InstanceState::Cancelled,
        ]),
        ..Default::default()
    };
    let pagination = orch8_types::filter::Pagination {
        offset: 0,
        limit: 100,
        sort_ascending: false,
    };

    let Ok(instances) = storage.list_instances(&filter, &pagination).await else {
        return;
    };

    let mut notified_set = notified.lock().await;
    if notified_set.len() > 500 {
        notified_set.clear();
    }

    for inst in instances {
        let id_str = inst.id.to_string();
        if !notified_set.insert(id_str.clone()) {
            continue;
        }

        match inst.state {
            InstanceState::Completed => {
                let output = serde_json::to_string(&inst.context.data).unwrap_or_default();
                listener.on_instance_completed(id_str.clone(), output);
            }
            InstanceState::Failed => {
                let error = serde_json::to_string(&inst.context.data).unwrap_or_default();
                listener.on_instance_failed(id_str.clone(), error);
            }
            _ => {}
        }

        dedup_keys.lock().await.retain(|_, v| v != &id_str);
    }
}

async fn fire_step_pending_events_inner(
    storage: &Arc<dyn StorageBackend>,
    listener: &Arc<Mutex<Option<Arc<dyn EngineListener>>>>,
    sequence_cache: &Arc<SequenceCache>,
    notified: &Arc<Mutex<HashSet<String>>>,
) {
    let listener = {
        let guard = listener.lock().await;
        match guard.clone() {
            Some(l) => l,
            None => return,
        }
    };

    let filter = orch8_types::filter::InstanceFilter {
        states: Some(vec![InstanceState::Waiting]),
        ..Default::default()
    };
    let pagination = orch8_types::filter::Pagination {
        offset: 0,
        limit: 100,
        sort_ascending: false,
    };

    let Ok(instances) = storage.list_instances(&filter, &pagination).await else {
        return;
    };

    let mut notified_set = notified.lock().await;
    if notified_set.len() > 500 {
        notified_set.clear();
    }

    for inst in instances {
        let Some(ref step_id) = inst.context.runtime.current_step else {
            continue;
        };
        let dedup_key = format!("{}:{}", inst.id, step_id);
        if !notified_set.insert(dedup_key) {
            continue;
        }

        let handler = sequence_cache
            .get_by_id(storage.as_ref(), inst.sequence_id)
            .await
            .ok()
            .and_then(|seq| {
                seq.blocks.iter().find_map(|b| {
                    if let orch8_types::sequence::BlockDefinition::Step(s) = b {
                        if s.id == *step_id {
                            return Some(s.handler.clone());
                        }
                    }
                    None
                })
            })
            .unwrap_or_default();

        listener.on_step_pending(inst.id.to_string(), step_id.to_string(), handler);
    }
}

async fn gc_expired_instances_inner(storage: &Arc<dyn StorageBackend>, max_lifetime_secs: u64) {
    let max_lifetime = Duration::from_secs(max_lifetime_secs);
    let cutoff = chrono::Utc::now()
        - chrono::Duration::from_std(max_lifetime).unwrap_or(chrono::Duration::hours(24));

    let filter = orch8_types::filter::InstanceFilter {
        states: Some(vec![
            InstanceState::Scheduled,
            InstanceState::Running,
            InstanceState::Waiting,
            InstanceState::Paused,
        ]),
        ..Default::default()
    };
    let pagination = orch8_types::filter::Pagination {
        offset: 0,
        limit: 50,
        sort_ascending: true,
    };

    let Ok(instances) = storage.list_instances(&filter, &pagination).await else {
        return;
    };

    for inst in instances {
        if inst.created_at < cutoff {
            if let Err(e) = storage
                .update_instance_state(inst.id, InstanceState::Failed, None)
                .await
            {
                warn!(instance_id = %inst.id, error = %e, "failed to expire instance");
            } else {
                info!(instance_id = %inst.id, "expired instance (exceeded max lifetime)");
            }
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

    #[test]
    fn default_config_has_sane_values() {
        let config = MobileEngineConfig::default();
        assert_eq!(config.tick_interval_ms, 100);
        assert_eq!(config.max_concurrent_steps, 4);
        assert_eq!(config.max_concurrent_instances, 10);
        assert_eq!(config.max_steps_per_instance, 1000);
        assert_eq!(config.max_tick_duration_ms, 5000);
        assert_eq!(config.max_instance_lifetime_secs, 86_400);
        assert_eq!(config.max_stored_sequences, 50);
        assert_eq!(config.max_sequence_size_bytes, 1_048_576);
        assert!(config.telemetry_enabled);
        assert_eq!(config.environment, "production");
    }

    #[test]
    fn tick_result_converts_from_engine() {
        let engine_result = TickOnceResult {
            instances_advanced: 3,
            steps_executed: 5,
            has_pending_work: true,
        };
        let result: TickResult = engine_result.into();
        assert_eq!(result.instances_advanced, 3);
        assert_eq!(result.steps_executed, 5);
        assert!(result.has_pending_work);
    }

    #[test]
    fn parse_instance_id_valid() {
        let uuid = uuid::Uuid::new_v4();
        let id = parse_instance_id(&uuid.to_string()).unwrap();
        assert_eq!(*id.as_uuid(), uuid);
    }

    #[test]
    fn parse_instance_id_invalid() {
        assert!(parse_instance_id("not-a-uuid").is_err());
    }
}
