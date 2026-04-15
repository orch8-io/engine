pub mod error;
pub mod evaluator;
pub mod handlers;
pub mod lifecycle;
pub mod recovery;
pub mod scheduler;
pub mod scheduling;
pub mod signals;
pub mod template;

use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;
use tokio_util::sync::CancellationToken;

use crate::handlers::HandlerRegistry;

/// The core scheduling and orchestration engine.
pub struct Engine {
    storage: Arc<dyn StorageBackend>,
    config: SchedulerConfig,
    handlers: Arc<HandlerRegistry>,
    cancel: CancellationToken,
}

impl Engine {
    pub fn new(
        storage: Arc<dyn StorageBackend>,
        config: SchedulerConfig,
        handlers: HandlerRegistry,
        cancel: CancellationToken,
    ) -> Self {
        Self {
            storage,
            config,
            handlers: Arc::new(handlers),
            cancel,
        }
    }

    pub fn storage(&self) -> &dyn StorageBackend {
        self.storage.as_ref()
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }

    /// Start the engine: runs the tick loop until cancelled.
    pub async fn run(&self) -> Result<(), error::EngineError> {
        // Recover stale instances from previous crash
        recovery::recover_stale_instances(
            self.storage.as_ref(),
            self.config.stale_instance_threshold_secs,
        )
        .await?;

        // Run the tick loop
        scheduler::run_tick_loop(
            Arc::clone(&self.storage),
            Arc::clone(&self.handlers),
            &self.config,
            self.cancel.clone(),
        )
        .await
    }
}
