/// Configuration for the mobile engine, exposed to host apps via `UniFFI`.
#[derive(Debug, Clone, uniffi::Record)]
pub struct MobileEngineConfig {
    /// Tick interval in milliseconds for the foreground loop (default: 100).
    pub tick_interval_ms: u64,
    /// Maximum concurrent step executions (default: 4).
    pub max_concurrent_steps: u32,
    /// Maximum steps per instance before forced failure (default: 1000).
    pub max_steps_per_instance: u32,
    /// Maximum concurrent running instances (default: 10).
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
    /// Enable telemetry collection (default: true).
    pub telemetry_enabled: bool,
    /// Target environment: "production" or "staging" (default: "production").
    pub environment: String,
    /// Base64-encoded Ed25519 root public key for manifest verification.
    /// If empty, sync is disabled.
    pub root_public_key: String,
    /// Mobile SDK version string, used for `min_sdk_version` checks during sync.
    pub sdk_version: String,
}

impl Default for MobileEngineConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: 100,
            max_concurrent_steps: 4,
            max_steps_per_instance: 1000,
            max_concurrent_instances: 10,
            max_tick_duration_ms: 5000,
            max_instance_lifetime_secs: 86_400,
            max_stored_sequences: 50,
            max_sequence_size_bytes: 1_048_576,
            handler_timeout_ms: 30_000,
            telemetry_enabled: true,
            environment: "production".to_string(),
            root_public_key: String::new(),
            sdk_version: env!("CARGO_PKG_VERSION").to_string(),
        }
    }
}

impl MobileEngineConfig {
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
        assert_eq!(config.tick_interval_ms, 100);
        assert_eq!(config.max_concurrent_steps, 4);
        assert_eq!(config.max_steps_per_instance, 1000);
        assert_eq!(config.max_concurrent_instances, 10);
        assert_eq!(config.max_tick_duration_ms, 5000);
        assert_eq!(config.max_instance_lifetime_secs, 86_400);
        assert_eq!(config.max_stored_sequences, 50);
        assert_eq!(config.max_sequence_size_bytes, 1_048_576);
        assert_eq!(config.handler_timeout_ms, 30_000);
        assert!(config.telemetry_enabled);
        assert_eq!(config.environment, "production");
        assert!(config.root_public_key.is_empty());
    }

    #[test]
    fn scheduler_config_derived_correctly() {
        let config = MobileEngineConfig::default();
        let sched = config.to_scheduler_config();
        assert_eq!(sched.tick_interval_ms, 100);
        assert_eq!(sched.batch_size, 10);
        assert_eq!(sched.max_concurrent_steps, 4);
        assert_eq!(sched.max_steps_per_instance, 1000);
    }
}
