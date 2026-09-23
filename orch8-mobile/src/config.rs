/// Configuration for the mobile engine, exposed to host apps via `UniFFI`.
///
/// `Debug` is implemented by hand: `sync_api_key` is a credential and URLs
/// may carry signed-URL tokens in their query strings, so neither may reach
/// logs or crash reports verbatim.
#[derive(Clone, uniffi::Record)]
pub struct MobileEngineConfig {
    /// Tick interval in milliseconds for the foreground loop (default: 500).
    pub tick_interval_ms: u64,
    /// Maximum concurrent step executions; must be positive (default: 4).
    pub max_concurrent_steps: u32,
    /// Maximum steps per instance before forced failure (default: 1000).
    pub max_steps_per_instance: u32,
    /// Maximum concurrent running instances; must be positive (default: 10).
    pub max_concurrent_instances: u32,
    /// Maximum tick duration in milliseconds before yielding (default: 5000).
    pub max_tick_duration_ms: u64,
    /// Maximum instance lifetime in seconds before auto-cancel (default: 86400 = 24h).
    pub max_instance_lifetime_secs: u64,
    /// Maximum stored sequences in the local database (default: 50).
    pub max_stored_sequences: u32,
    /// Maximum sequence JSON size in bytes (default: 1MB).
    pub max_sequence_size_bytes: u64,
    /// Handler timeout in milliseconds — after this, step transitions to Waiting (default: 30000).
    pub handler_timeout_ms: u64,
    /// Operation timeout in milliseconds for synchronous API calls like start/cancel/get (default: 10000).
    pub operation_timeout_ms: u64,
    /// Enable telemetry collection (default: true).
    pub telemetry_enabled: bool,
    /// HTTPS endpoint where telemetry batches are sent. Must use port 443 and
    /// target a public host. If empty, telemetry flushing is disabled.
    pub telemetry_url: String,
    /// Target environment: "production" or "staging" (default: "production").
    pub environment: String,
    /// Base64-encoded Ed25519 root public key for manifest verification.
    /// If empty, sync is disabled.
    pub root_public_key: String,
    /// Mobile SDK version string, used for `min_sdk_version` checks during sync.
    pub sdk_version: String,
    /// Maximum memory budget in bytes (0 = unlimited). When process RSS exceeds
    /// this limit, tick execution is skipped until memory drops below the threshold.
    /// Default: 0 (unlimited).
    pub memory_budget_bytes: u64,
    /// URL to fetch sequence definitions from. The endpoint must return a JSON
    /// array of sequence objects. If empty, no remote loading is performed.
    /// Example: `https://api.orch8.io/api/mobile/apps/{id}/sequences`
    pub sequences_url: String,
    /// Server sync endpoint URL. If non-empty, the engine will periodically
    /// POST status updates / approval requests and receive commands.
    /// Example: `https://api.orch8.io/api/v1/mobile/sync`
    pub sync_url: String,
    /// Unique device identifier sent with each sync request.
    pub device_id: String,
    /// API key for authenticating sync requests.
    pub sync_api_key: String,
}

impl std::fmt::Debug for MobileEngineConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let url = |u: &str| {
            if u.is_empty() {
                String::new()
            } else {
                orch8_engine::outbound::redact_url(u)
            }
        };
        let secret = |s: &str| if s.is_empty() { "" } else { "[REDACTED]" };
        f.debug_struct("MobileEngineConfig")
            .field("tick_interval_ms", &self.tick_interval_ms)
            .field("max_concurrent_steps", &self.max_concurrent_steps)
            .field("max_steps_per_instance", &self.max_steps_per_instance)
            .field("max_concurrent_instances", &self.max_concurrent_instances)
            .field("max_tick_duration_ms", &self.max_tick_duration_ms)
            .field(
                "max_instance_lifetime_secs",
                &self.max_instance_lifetime_secs,
            )
            .field("max_stored_sequences", &self.max_stored_sequences)
            .field("max_sequence_size_bytes", &self.max_sequence_size_bytes)
            .field("handler_timeout_ms", &self.handler_timeout_ms)
            .field("operation_timeout_ms", &self.operation_timeout_ms)
            .field("telemetry_enabled", &self.telemetry_enabled)
            .field("telemetry_url", &url(&self.telemetry_url))
            .field("environment", &self.environment)
            .field("root_public_key", &self.root_public_key)
            .field("sdk_version", &self.sdk_version)
            .field("memory_budget_bytes", &self.memory_budget_bytes)
            .field("sequences_url", &url(&self.sequences_url))
            .field("sync_url", &url(&self.sync_url))
            .field("device_id", &self.device_id)
            .field("sync_api_key", &secret(&self.sync_api_key))
            .finish()
    }
}

impl Default for MobileEngineConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: 500,
            max_concurrent_steps: 4,
            max_steps_per_instance: 1000,
            max_concurrent_instances: 10,
            max_tick_duration_ms: 5000,
            max_instance_lifetime_secs: 86_400,
            max_stored_sequences: 50,
            max_sequence_size_bytes: 1_048_576,
            handler_timeout_ms: 30_000,
            operation_timeout_ms: 10_000,
            telemetry_enabled: true,
            telemetry_url: String::new(),
            environment: "production".to_string(),
            root_public_key: String::new(),
            sdk_version: env!("CARGO_PKG_VERSION").to_string(),
            memory_budget_bytes: 0,
            sequences_url: String::new(),
            sync_url: String::new(),
            device_id: String::new(),
            sync_api_key: String::new(),
        }
    }
}

impl MobileEngineConfig {
    pub(crate) fn validate(&self) -> Result<(), crate::MobileError> {
        if self.max_concurrent_steps == 0
            || u64::from(self.max_concurrent_steps) > tokio::sync::Semaphore::MAX_PERMITS as u64
        {
            return Err(crate::MobileError::InvalidInput {
                message:
                    "max_concurrent_steps must be positive and fit the platform semaphore limit"
                        .into(),
            });
        }
        if self.max_concurrent_instances == 0 {
            return Err(crate::MobileError::InvalidInput {
                message: "max_concurrent_instances must be greater than zero".into(),
            });
        }
        Ok(())
    }

    pub(crate) fn to_scheduler_config(&self) -> orch8_types::config::SchedulerConfig {
        orch8_types::config::SchedulerConfig {
            tick_interval_ms: self.tick_interval_ms,
            batch_size: self.max_concurrent_instances,
            max_concurrent_steps: self.max_concurrent_steps,
            shutdown_grace_period_secs: 5,
            stale_instance_threshold_secs: 60,
            max_instances_per_tenant: 0,
            max_steps_per_instance: self.max_steps_per_instance,
            ..orch8_types::config::SchedulerConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_has_sane_values() {
        let config = MobileEngineConfig::default();
        assert_eq!(config.tick_interval_ms, 500);
        assert_eq!(config.max_concurrent_steps, 4);
        assert_eq!(config.max_steps_per_instance, 1000);
        assert_eq!(config.max_concurrent_instances, 10);
        assert_eq!(config.max_tick_duration_ms, 5000);
        assert_eq!(config.max_instance_lifetime_secs, 86_400);
        assert_eq!(config.max_stored_sequences, 50);
        assert_eq!(config.max_sequence_size_bytes, 1_048_576);
        assert_eq!(config.handler_timeout_ms, 30_000);
        assert_eq!(config.operation_timeout_ms, 10_000);
        assert!(config.telemetry_enabled);
        assert_eq!(config.environment, "production");
        assert!(config.root_public_key.is_empty());
        assert_eq!(config.memory_budget_bytes, 0);
    }

    #[test]
    fn concurrency_limits_are_validated() {
        assert!(MobileEngineConfig::default().validate().is_ok());
        for (steps, instances) in [(0, 1), (1, 0)] {
            let config = MobileEngineConfig {
                max_concurrent_steps: steps,
                max_concurrent_instances: instances,
                ..Default::default()
            };
            assert!(matches!(
                config.validate(),
                Err(crate::MobileError::InvalidInput { .. })
            ));
        }
        let config = MobileEngineConfig {
            max_concurrent_steps: u32::MAX,
            ..Default::default()
        };
        assert_eq!(
            config.validate().is_ok(),
            u64::from(u32::MAX) <= tokio::sync::Semaphore::MAX_PERMITS as u64
        );
    }

    #[test]
    fn debug_redacts_api_key_and_url_tokens() {
        let config = MobileEngineConfig {
            sync_api_key: "sk_live_SECRET".into(),
            sequences_url: "https://cdn.example/seq.json?token=SECRET".into(),
            ..MobileEngineConfig::default()
        };
        let shown = format!("{config:?}");
        assert!(!shown.contains("SECRET"), "{shown}");
        assert!(shown.contains("[REDACTED]"));
        assert!(shown.contains("https://cdn.example/seq.json"));
    }

    #[test]
    fn scheduler_config_derived_correctly() {
        let config = MobileEngineConfig::default();
        let sched = config.to_scheduler_config();
        assert_eq!(sched.tick_interval_ms, 500);
        assert_eq!(sched.batch_size, 10);
        assert_eq!(sched.max_concurrent_steps, 4);
        assert_eq!(sched.max_steps_per_instance, 1000);
    }
}
