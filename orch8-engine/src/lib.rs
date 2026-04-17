pub mod circuit_breaker;
pub mod credentials;
pub mod cron;
pub mod error;
pub mod evaluator;
pub mod expression;
pub mod externalized;
pub mod handlers;
pub mod lifecycle;
pub mod metrics;
pub mod preload;
pub mod recovery;
pub mod required_fields;
pub mod scheduler;
pub mod scheduling;
pub mod signals;
pub mod template;
pub mod triggers;
pub mod webhooks;

use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::handlers::HandlerRegistry;

/// The core scheduling and orchestration engine.
pub struct Engine {
    storage: Arc<dyn StorageBackend>,
    config: SchedulerConfig,
    handlers: Arc<HandlerRegistry>,
    cancel: CancellationToken,
    /// Unique node ID for this engine instance in a multi-node cluster.
    node_id: Uuid,
    /// Human-readable node name.
    node_name: String,
}

impl Engine {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        config: SchedulerConfig,
        handlers: HandlerRegistry,
        cancel: CancellationToken,
    ) -> Self {
        let node_id = Uuid::new_v4();
        let node_name = hostname().unwrap_or_else(|| {
            // UUID hyphen-form is always ≥ 8 chars, so slicing is safe.
            let id_str = node_id.to_string();
            format!("node-{}", &id_str[..8])
        });
        Self {
            storage,
            config,
            handlers: Arc::new(handlers),
            cancel,
            node_id,
            node_name,
        }
    }

    pub fn storage(&self) -> &dyn StorageBackend {
        self.storage.as_ref()
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    pub fn node_id(&self) -> Uuid {
        self.node_id
    }

    /// Start the engine: runs recovery, cron loop, and the main tick loop until cancelled.
    pub async fn run(&self) -> Result<(), error::EngineError> {
        // Register this node in the cluster.
        self.register_node().await?;

        // Recover stale instances from previous crash
        recovery::recover_stale_instances(
            self.storage.as_ref(),
            self.config.stale_instance_threshold_secs,
        )
        .await?;

        self.spawn_background_tasks();

        // Run the main tick loop
        let result = scheduler::run_tick_loop(
            Arc::clone(&self.storage),
            Arc::clone(&self.handlers),
            &self.config,
            self.cancel.clone(),
        )
        .await;

        // Deregister this node on shutdown.
        self.deregister_node().await;

        result
    }

    /// Spawn all background tasks (heartbeat, reapers, cron, triggers).
    fn spawn_background_tasks(&self) {
        // Cluster heartbeat + drain check (every 10 seconds).
        let hb_storage = Arc::clone(&self.storage);
        let hb_cancel = self.cancel.clone();
        let hb_node_id = self.node_id;
        let drain_cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                tokio::select! {
                    () = hb_cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        if let Err(e) = hb_storage.heartbeat_node(hb_node_id).await {
                            tracing::error!(error = %e, "cluster heartbeat failed");
                        }
                        match hb_storage.should_drain(hb_node_id).await {
                            Ok(true) => {
                                tracing::info!(node_id = %hb_node_id, "drain signal received — initiating shutdown");
                                drain_cancel.cancel();
                            }
                            Ok(false) => {}
                            Err(e) => tracing::error!(error = %e, "drain check failed"),
                        }
                    }
                }
            }
        });

        // Stale node reaper (every 60 seconds).
        let node_reaper_storage = Arc::clone(&self.storage);
        let node_reaper_cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_mins(1));
            loop {
                tokio::select! {
                    () = node_reaper_cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        match node_reaper_storage
                            .reap_stale_nodes(std::time::Duration::from_mins(2))
                            .await
                        {
                            Ok(0) => {}
                            Ok(n) => tracing::info!(count = n, "reaped stale cluster nodes"),
                            Err(e) => tracing::error!(error = %e, "cluster node reaper error"),
                        }
                    }
                }
            }
        });

        // Worker task reaper (resets stale claimed tasks every 30 seconds).
        let reaper_storage = Arc::clone(&self.storage);
        let reaper_cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(std::time::Duration::from_secs(30));
            loop {
                tokio::select! {
                    () = reaper_cancel.cancelled() => break,
                    _ = ticker.tick() => {
                        match reaper_storage
                            .reap_stale_worker_tasks(std::time::Duration::from_mins(1))
                            .await
                        {
                            Ok(0) => {}
                            Ok(n) => tracing::info!(count = n, "reaped stale worker tasks"),
                            Err(e) => tracing::error!(error = %e, "worker task reaper error"),
                        }
                    }
                }
            }
        });

        // Cron loop (checks every 10 seconds for due cron schedules).
        let cron_storage = Arc::clone(&self.storage);
        let cron_cancel = self.cancel.clone();
        tokio::spawn(async move {
            if let Err(e) = cron::run_cron_loop(
                cron_storage,
                std::time::Duration::from_secs(10),
                cron_cancel,
            )
            .await
            {
                tracing::error!(error = %e, "cron loop exited with error");
            }
        });

        // Trigger processor loop (syncs trigger definitions every 15 seconds).
        let trigger_storage = Arc::clone(&self.storage);
        let trigger_cancel = self.cancel.clone();
        tokio::spawn(async move {
            triggers::run_trigger_loop(
                trigger_storage,
                std::time::Duration::from_secs(15),
                trigger_cancel,
            )
            .await;
            tracing::info!("trigger processor loop exited");
        });

        Self::spawn_credentials_refresh(Arc::clone(&self.storage), self.cancel.clone());
    }

    /// Spawn the `OAuth2` credential refresh loop.
    /// Polls every 60s and refreshes tokens expiring within the next 5 minutes.
    fn spawn_credentials_refresh(storage: Arc<dyn StorageBackend>, cancel: CancellationToken) {
        tokio::spawn(async move {
            credentials::run_refresh_loop(
                storage,
                std::time::Duration::from_mins(1),
                std::time::Duration::from_mins(5),
                cancel,
            )
            .await;
            tracing::info!("credentials refresh loop exited");
        });
    }

    /// Register this engine instance as a cluster node.
    async fn register_node(&self) -> Result<(), error::EngineError> {
        let now = chrono::Utc::now();
        let node = orch8_types::cluster::ClusterNode {
            id: self.node_id,
            name: self.node_name.clone(),
            status: orch8_types::cluster::NodeStatus::Active,
            registered_at: now,
            last_heartbeat_at: now,
            drain: false,
        };
        self.storage.register_node(&node).await?;
        tracing::info!(
            node_id = %self.node_id,
            node_name = %self.node_name,
            "cluster node registered"
        );
        Ok(())
    }

    /// Mark this node as stopped in the cluster registry.
    async fn deregister_node(&self) {
        if let Err(e) = self.storage.deregister_node(self.node_id).await {
            tracing::error!(error = %e, "failed to deregister cluster node");
        } else {
            tracing::info!(node_id = %self.node_id, "cluster node deregistered");
        }
    }
}

/// Best-effort hostname retrieval.
fn hostname() -> Option<String> {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("POD_NAME"))
        .ok()
}
