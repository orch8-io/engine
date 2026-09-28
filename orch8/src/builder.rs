use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use orch8_engine::handlers::{HandlerRegistry, StepContext};
use orch8_engine::recovery;
use orch8_types::clock::SharedClock;
use orch8_types::config::SchedulerConfig;
use orch8_types::error::StepError;
use orch8_types::ids::TenantId;

use crate::effect::EffectContext;
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
    clock: SharedClock,
    startup_recovery_threshold: Option<Duration>,
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
            clock: SharedClock::default(),
            startup_recovery_threshold: None,
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

    /// Register an externally visible effect handler with durable dispatch
    /// evidence and a provider idempotency key.
    ///
    /// Before the handler runs, Orch8 persists an effect receipt. Return the
    /// provider's receipt in the output as `provider_receipt_id`; Orch8 copies
    /// it into the committed durable receipt. During a dry run,
    /// [`EffectContext::dispatch_idempotency_key`] is `None` and the handler
    /// must skip the external call.
    pub fn effect_handler<F, Fut>(mut self, name: &str, handler: F) -> Self
    where
        F: Fn(EffectContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<serde_json::Value, StepError>> + Send + 'static,
    {
        let handler = Arc::new(handler);
        self.handlers.register(name, move |step| {
            let handler = Arc::clone(&handler);
            async move {
                let context = EffectContext::load(step).await?;
                handler(context).await
            }
        });
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

    /// Time source for all scheduling decisions (claiming due instances,
    /// delay / send-window deferrals, retry backoff, cron evaluation).
    /// Default: the real system clock.
    ///
    /// Inject a [`crate::ManualClock`] (wrapped via
    /// [`crate::SharedClock::from_arc`]) to control virtual time — e.g. a
    /// test or dev loop that fast-forwards over a 3-day delay:
    ///
    /// ```no_run
    /// # async fn run() -> Result<(), Box<dyn std::error::Error>> {
    /// use std::sync::Arc;
    ///
    /// let manual = Arc::new(orch8::ManualClock::new(chrono::Utc::now()));
    /// let engine = orch8::Engine::builder()
    ///     .storage(orch8::Storage::sqlite_in_memory())
    ///     .clock(orch8::SharedClock::from_arc(
    ///         Arc::clone(&manual) as Arc<dyn orch8::Clock>
    ///     ))
    ///     .build()
    ///     .await?;
    /// // ... later: manual.advance(chrono::Duration::days(3));
    /// # Ok(())
    /// # }
    /// ```
    pub fn clock(mut self, clock: SharedClock) -> Self {
        self.clock = clock;
        self
    }

    /// How long an instance must have been `Running` without a heartbeat
    /// before [`EngineBuilder::build`] treats it as orphaned by a crash and
    /// reschedules it. Default: the scheduler's stale-instance threshold
    /// (300 s), which is safe when several processes share one database.
    ///
    /// A host that is the **sole owner** of its storage (e.g. an embedded
    /// `SQLite` file opened by exactly one process) can pass
    /// [`Duration::ZERO`]: nothing else can be executing its instances, so
    /// every `Running` row at startup was interrupted and is resumed
    /// immediately instead of after the threshold elapses.
    ///
    /// Only the one-shot startup recovery is affected; the scheduler's
    /// heartbeat cadence and in-flight lease keep their defaults.
    pub fn stale_instance_threshold(mut self, threshold: Duration) -> Self {
        self.startup_recovery_threshold = Some(threshold);
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
            clock: self.clock,
            ..SchedulerConfig::default()
        };

        // Crash recovery, mirroring server startup: instances stuck in
        // `Running` longer than the stale threshold go back to `Scheduled`.
        let recovery_threshold_secs = self
            .startup_recovery_threshold
            .map_or(config.stale_instance_threshold_secs, |threshold| {
                threshold.as_secs()
            });
        recovery::recover_stale_instances(storage.as_ref(), recovery_threshold_secs).await?;

        Ok(Engine::from_parts(storage, self.handlers, config, tenant))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use orch8_types::instance::InstanceState;

    const SEQ_JSON: &str = r#"{
        "id": "0195fdc0-0000-7000-8000-00000000b001",
        "tenant_id": "default",
        "namespace": "default",
        "name": "recovery",
        "version": 1,
        "blocks": [{ "type": "step", "id": "a", "handler": "noop", "params": {} }],
        "created_at": "2026-01-01T00:00:00Z"
    }"#;

    /// Simulates a crash: an instance is left `Running` in a file-backed
    /// database. With the default threshold a freshly reopened engine leaves
    /// it alone (another process might still own it); a sole owner passing
    /// `Duration::ZERO` reschedules it immediately.
    #[tokio::test]
    async fn zero_stale_threshold_recovers_running_instances_on_build() {
        let dir = std::env::temp_dir().join(format!("orch8-recovery-{}", uuid::Uuid::now_v7()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("engine.db");
        let path = path.to_str().unwrap();

        let engine = crate::Engine::builder()
            .storage(Storage::sqlite(path))
            .build()
            .await
            .unwrap();
        let seq = engine
            .upsert_sequence(serde_json::from_str(SEQ_JSON).unwrap())
            .await
            .unwrap();
        let id = engine
            .create_instance(seq, crate::CreateInstanceOptions::default())
            .await
            .unwrap();
        engine
            .storage_backend()
            .update_instance_state(id, InstanceState::Running, None)
            .await
            .unwrap();
        drop(engine);
        // `updated_at < now - threshold` is strict; make sure "now" moves on.
        tokio::time::sleep(Duration::from_millis(5)).await;

        let default_threshold = crate::Engine::builder()
            .storage(Storage::sqlite(path))
            .build()
            .await
            .unwrap();
        assert_eq!(
            default_threshold.get_instance(id).await.unwrap().state,
            InstanceState::Running,
            "default threshold must not steal a recently active instance"
        );
        drop(default_threshold);

        let sole_owner = crate::Engine::builder()
            .storage(Storage::sqlite(path))
            .stale_instance_threshold(Duration::ZERO)
            .build()
            .await
            .unwrap();
        assert_eq!(
            sole_owner.get_instance(id).await.unwrap().state,
            InstanceState::Scheduled
        );
        drop(sole_owner);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
