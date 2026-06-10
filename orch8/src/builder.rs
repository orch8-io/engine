use std::future::Future;
use std::time::Duration;

use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_engine::recovery;
use orch8_types::config::SchedulerConfig;
use orch8_types::error::StepError;
use orch8_types::ids::TenantId;

use crate::engine::Engine;
use crate::error::Error;
use crate::storage::Storage;

/// Builder for an embedded [`Engine`]. Obtain via [`Engine::builder`].
///
/// At minimum a [`Storage`] must be configured; everything else has
/// sensible defaults (100 ms tick interval, tenant `"default"`, the full
/// built-in handler set).
#[must_use = "call .build().await to construct the engine"]
pub struct EngineBuilder {
    storage: Option<Storage>,
    handlers: HandlerRegistry,
    tick_interval: Duration,
    tenant: String,
}

impl EngineBuilder {
    pub(crate) fn new() -> Self {
        let mut handlers = HandlerRegistry::new();
        // Same default registry the server wires up at startup: all built-in
        // handlers (noop, log, sleep, http_request, transform, ...).
        orch8_engine::handlers::builtin::register_builtins(&mut handlers);
        Self {
            storage: None,
            handlers,
            tick_interval: Duration::from_millis(SchedulerConfig::default().tick_interval_ms),
            tenant: "default".to_string(),
        }
    }

    /// Select the storage backend (required). See [`Storage`].
    pub fn storage(mut self, storage: Storage) -> Self {
        self.storage = Some(storage);
        self
    }

    /// Register a custom step handler under `name`.
    ///
    /// Handlers are plain async functions taking a [`StepContext`] and
    /// returning JSON output. Registering a name that collides with a
    /// built-in handler replaces the built-in.
    pub fn handler<F, Fut>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(StepContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, StepError>> + Send + 'static,
    {
        self.handlers.register(name, handler);
        self
    }

    /// Scheduler tick interval for the background loop started by
    /// [`Engine::start`]. Default: 100 ms.
    pub fn tick_interval(mut self, interval: Duration) -> Self {
        self.tick_interval = interval;
        self
    }

    /// Default tenant used by [`Engine::create_instance`] for instance
    /// scoping. Default: `"default"`.
    pub fn tenant(mut self, tenant: impl Into<String>) -> Self {
        self.tenant = tenant.into();
        self
    }

    /// Open the storage backend (applying schema/migrations), recover any
    /// instances left `Running` by a previous crash, and return the engine.
    ///
    /// Must be called from within a tokio runtime.
    pub async fn build(self) -> Result<Engine, Error> {
        let storage_cfg = self.storage.ok_or_else(|| {
            Error::Config(
                "no storage configured — call .storage(Storage::sqlite(..)) on the builder"
                    .to_string(),
            )
        })?;

        let tenant = TenantId::new(self.tenant).map_err(Error::Config)?;

        let storage = storage_cfg.connect().await?;

        let config = SchedulerConfig {
            tick_interval_ms: u64::try_from(self.tick_interval.as_millis())
                .unwrap_or(u64::MAX)
                .max(1),
            ..SchedulerConfig::default()
        };

        // Crash recovery, mirroring server startup: instances stuck in
        // `Running` longer than the stale threshold go back to `Scheduled`.
        recovery::recover_stale_instances(storage.as_ref(), config.stale_instance_threshold_secs)
            .await?;

        Ok(Engine::from_parts(storage, self.handlers, config, tenant))
    }
}
