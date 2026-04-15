pub mod error;

use std::sync::Arc;

use orch8_storage::StorageBackend;
use orch8_types::config::SchedulerConfig;

/// The core scheduling and orchestration engine.
///
/// Holds references to storage and configuration.
/// In Stage 1, this will gain the tick loop, step execution, and crash recovery.
pub struct Engine {
    storage: Arc<dyn StorageBackend>,
    config: SchedulerConfig,
}

impl Engine {
    pub fn new(storage: Arc<dyn StorageBackend>, config: SchedulerConfig) -> Self {
        Self { storage, config }
    }

    pub fn storage(&self) -> &dyn StorageBackend {
        self.storage.as_ref()
    }

    pub fn config(&self) -> &SchedulerConfig {
        &self.config
    }
}
