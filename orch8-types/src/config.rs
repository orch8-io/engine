use serde::{Deserialize, Serialize};
use std::fmt;

/// A wrapper for sensitive strings that redacts the value in Debug and Serialize output.
/// Use `.expose()` to access the inner value when you actually need it.
#[derive(Clone, Default, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

/// Placeholder emitted by `Debug`, `Serialize`, and `redact()` for non-empty secrets.
pub const REDACTED_PLACEHOLDER: &str = "[REDACTED]";

impl SecretString {
    pub fn new(s: String) -> Self {
        Self(s)
    }

    /// Access the actual secret value. Call sites should keep the returned `&str`
    /// out of logs, telemetry, and serialized output — use [`Self::redact`] instead.
    #[must_use = "secret values must not be dropped silently; call .redact() for display"]
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Return a safe-to-log placeholder (`""` when empty, `[REDACTED]` otherwise).
    #[must_use]
    pub fn redact(&self) -> &'static str {
        if self.0.is_empty() {
            ""
        } else {
            REDACTED_PLACEHOLDER
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl fmt::Debug for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("SecretString(\"\")")
        } else {
            f.write_str("SecretString(\"[REDACTED]\")")
        }
    }
}

impl Serialize for SecretString {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.0.is_empty() {
            s.serialize_str("")
        } else {
            s.serialize_str(REDACTED_PLACEHOLDER)
        }
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self(s)
    }
}

impl From<&str> for SecretString {
    fn from(s: &str) -> Self {
        Self(s.to_owned())
    }
}

impl fmt::Display for SecretString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.redact())
    }
}

impl PartialEq for SecretString {
    /// Constant-time comparison to mitigate timing side channels on secrets.
    fn eq(&self, other: &Self) -> bool {
        use subtle::ConstantTimeEq;
        self.0.as_bytes().ct_eq(other.0.as_bytes()).into()
    }
}

impl Eq for SecretString {}

/// Top-level configuration. Layered: TOML file -> env vars -> CLI flags.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EngineConfig {
    #[serde(default)]
    pub database: DatabaseConfig,
    #[serde(default)]
    pub engine: SchedulerConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Storage backend: "postgres" (default) or "sqlite".
    #[serde(default = "default_backend")]
    pub backend: String,
    /// Connection URL (may contain credentials — redacted in logs).
    #[serde(default = "default_database_url")]
    pub url: SecretString,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_true")]
    pub run_migrations: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            url: default_database_url(),
            max_connections: default_max_connections(),
            run_migrations: true,
        }
    }
}

fn default_backend() -> String {
    "postgres".to_string()
}

fn default_database_url() -> SecretString {
    // Safe default: empty. Operators must provide a real URL via config/env.
    // Avoids shipping hardcoded credentials (even dev ones) in the binary.
    SecretString::default()
}

fn default_max_connections() -> u32 {
    64
}

fn default_true() -> bool {
    true
}

/// How the engine decides which payloads leave the inline context and
/// land in `externalized_state`. Ships as `Threshold { bytes: 65536 }`
/// — oversized fields go external, everything smaller stays inline for
/// zero-RTT reads.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ExternalizationMode {
    /// Never externalize — every payload stays inline. Small deployments,
    /// tests, and benchmarks that want to measure the in-memory hot path.
    Never,
    /// Externalize top-level `context.data` keys whose serialized size
    /// exceeds `bytes`. Also externalizes block outputs exceeding the
    /// same threshold. This is the default.
    Threshold { bytes: u32 },
    /// Always externalize block outputs regardless of size, but keep
    /// `context.data` inline unless a caller sets an additional per-field
    /// override. Useful when outputs dominate state volume.
    AlwaysOutputs,
}

impl Default for ExternalizationMode {
    fn default() -> Self {
        Self::Threshold { bytes: 64 * 1024 }
    }
}

impl ExternalizationMode {
    /// The size threshold for `context.data` fields (in bytes), or `None`
    /// if this mode does not externalize context data.
    #[must_use]
    pub fn context_threshold(&self) -> Option<u32> {
        match self {
            Self::Never | Self::AlwaysOutputs => None,
            Self::Threshold { bytes } => Some(*bytes),
        }
    }

    /// Returns `true` iff block outputs should always be externalized.
    #[must_use]
    pub fn always_externalize_outputs(&self) -> bool {
        matches!(self, Self::AlwaysOutputs)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulerConfig {
    #[serde(default = "default_tick_interval_ms")]
    pub tick_interval_ms: u64,
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_steps: u32,
    #[serde(default = "default_grace_period")]
    pub shutdown_grace_period_secs: u64,
    #[serde(default = "default_stale_threshold")]
    pub stale_instance_threshold_secs: u64,
    /// Max instances a single tenant can claim per tick (noisy-neighbor protection).
    /// 0 means no per-tenant limit (default).
    #[serde(default)]
    pub max_instances_per_tenant: u32,
    #[serde(default)]
    pub webhooks: WebhookConfig,
    /// Output size threshold in bytes. Outputs larger than this are externalized
    /// to `externalized_state` and replaced with a reference key in `block_outputs`.
    /// 0 means no externalization (default).
    #[serde(default)]
    pub externalize_output_threshold: u32,
    /// AES-256-GCM encryption key (64 hex chars) for encrypting sensitive context
    /// fields at rest. If empty, no encryption is applied.
    /// Can also be set via `ORCH8_ENCRYPTION_KEY` env var.
    #[serde(default)]
    pub encryption_key: SecretString,
    /// Maximum serialized size of a single instance's `ExecutionContext`
    /// in bytes. Writes exceeding this limit are rejected with 413. The
    /// whole context travels on every scheduler claim, so keeping it small
    /// matters for tick latency.
    ///
    /// Default: `DEFAULT_MAX_CONTEXT_BYTES` (256 KiB). `0` disables the check.
    #[serde(default = "default_max_context_bytes")]
    pub max_context_bytes: u32,
    /// How the engine decides which payloads leave the inline context and
    /// land in `externalized_state`. Default: `Threshold { bytes: 65536 }`.
    #[serde(default)]
    pub externalization_mode: ExternalizationMode,
    /// How often the worker-task reaper ticks, in seconds. Controls the
    /// scan cadence for resetting stale claimed tasks back to Pending so
    /// another worker can pick them up.
    ///
    /// Exposing this matters for retry-backoff e2e tests: the default of
    /// 30s is too long for tight unit loops. Set to a smaller value in
    /// tests to tighten the observable reap latency.
    #[serde(default = "default_worker_reaper_tick_secs")]
    pub worker_reaper_tick_secs: u64,
    /// How old a worker task's heartbeat can be before the reaper resets
    /// it, in seconds. Must be greater than the expected handler latency
    /// to avoid yanking still-active workers. Default: 60s.
    #[serde(default = "default_worker_reaper_stale_secs")]
    pub worker_reaper_stale_secs: u64,
    /// How often the cluster-node reaper ticks, in seconds. Default: 60s.
    #[serde(default = "default_node_reaper_tick_secs")]
    pub node_reaper_tick_secs: u64,
    /// How old a cluster node's heartbeat can be before the reaper marks
    /// it dead, in seconds. Default: 120s.
    #[serde(default = "default_node_reaper_stale_secs")]
    pub node_reaper_stale_secs: u64,
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: default_tick_interval_ms(),
            batch_size: default_batch_size(),
            max_concurrent_steps: default_max_concurrent(),
            shutdown_grace_period_secs: default_grace_period(),
            stale_instance_threshold_secs: default_stale_threshold(),
            max_instances_per_tenant: 0,
            webhooks: WebhookConfig::default(),
            externalize_output_threshold: 0,
            encryption_key: SecretString::default(),
            max_context_bytes: default_max_context_bytes(),
            externalization_mode: ExternalizationMode::default(),
            worker_reaper_tick_secs: default_worker_reaper_tick_secs(),
            worker_reaper_stale_secs: default_worker_reaper_stale_secs(),
            node_reaper_tick_secs: default_node_reaper_tick_secs(),
            node_reaper_stale_secs: default_node_reaper_stale_secs(),
        }
    }
}

fn default_worker_reaper_tick_secs() -> u64 {
    30
}

fn default_worker_reaper_stale_secs() -> u64 {
    60
}

fn default_node_reaper_tick_secs() -> u64 {
    60
}

fn default_node_reaper_stale_secs() -> u64 {
    120
}

fn default_max_context_bytes() -> u32 {
    crate::context::DEFAULT_MAX_CONTEXT_BYTES
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhookConfig {
    #[serde(default)]
    pub urls: Vec<String>,
    #[serde(default = "default_webhook_timeout_secs")]
    pub timeout_secs: u64,
    #[serde(default = "default_webhook_max_retries")]
    pub max_retries: u32,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            timeout_secs: default_webhook_timeout_secs(),
            max_retries: default_webhook_max_retries(),
        }
    }
}

fn default_webhook_timeout_secs() -> u64 {
    10
}

fn default_webhook_max_retries() -> u32 {
    3
}

fn default_tick_interval_ms() -> u64 {
    100
}

fn default_batch_size() -> u32 {
    256
}

fn default_max_concurrent() -> u32 {
    128
}

fn default_grace_period() -> u64 {
    30
}

fn default_stale_threshold() -> u64 {
    300
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_grpc_addr")]
    pub grpc_addr: String,
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    /// Comma-separated allowed origins for CORS. Use `*` to allow all.
    #[serde(default = "default_cors_origins")]
    pub cors_origins: String,
    /// Optional API key for authenticating requests. Empty means no auth.
    #[serde(default)]
    pub api_key: SecretString,
    /// If true, require `X-Tenant-Id` header on all requests and enforce
    /// tenant isolation. Requests without the header get `400 Bad Request`.
    #[serde(default)]
    pub require_tenant_header: bool,
    /// Maximum API requests per second (global). 0 means no limit (default).
    #[serde(default)]
    pub rate_limit_rps: u64,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            http_addr: default_http_addr(),
            cors_origins: default_cors_origins(),
            api_key: SecretString::default(),
            require_tenant_header: false,
            rate_limit_rps: 0,
        }
    }
}

fn default_cors_origins() -> String {
    "*".to_string()
}

fn default_grpc_addr() -> String {
    "0.0.0.0:50051".to_string()
}

fn default_http_addr() -> String {
    "0.0.0.0:8080".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    #[serde(default = "default_log_level")]
    pub level: String,
    #[serde(default)]
    pub json: bool,
}

impl Default for LoggingConfig {
    fn default() -> Self {
        Self {
            level: default_log_level(),
            json: false,
        }
    }
}

fn default_log_level() -> String {
    "info".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn secret_string_debug_never_reveals_value() {
        let s = SecretString::new("super-secret-token-123".into());
        let debug_out = format!("{s:?}");
        assert!(debug_out.contains("[REDACTED]"));
        assert!(!debug_out.contains("super-secret-token-123"));
    }

    #[test]
    fn secret_string_debug_shows_empty_distinctly() {
        let s = SecretString::default();
        let debug_out = format!("{s:?}");
        assert_eq!(debug_out, "SecretString(\"\")");
        assert!(!debug_out.contains("[REDACTED]"));
    }

    #[test]
    fn secret_string_display_matches_redact() {
        let empty = SecretString::default();
        assert_eq!(format!("{empty}"), "");

        let non_empty = SecretString::from("hunter2");
        assert_eq!(format!("{non_empty}"), "[REDACTED]");
    }

    #[test]
    fn secret_string_serialize_redacts_non_empty() {
        let s = SecretString::from("api-key-xyz");
        let json = serde_json::to_string(&s).unwrap();
        assert_eq!(json, "\"[REDACTED]\"");
    }

    #[test]
    fn secret_string_serialize_preserves_empty() {
        // Empty secrets must serialize as empty strings so round-tripping
        // config with no api_key set doesn't accidentally promote "[REDACTED]"
        // into the key slot.
        let empty = SecretString::default();
        assert_eq!(serde_json::to_string(&empty).unwrap(), "\"\"");
    }

    #[test]
    fn secret_string_deserialize_transparent() {
        // Transparent deserialize reads the raw string back into the secret —
        // this is how env/TOML values become SecretString at load time.
        let s: SecretString = serde_json::from_str("\"abc123\"").unwrap();
        assert_eq!(s.expose(), "abc123");
    }

    #[test]
    fn secret_string_expose_returns_actual_value() {
        let s = SecretString::from("real-value");
        assert_eq!(s.expose(), "real-value");
    }

    #[test]
    fn secret_string_eq_is_constant_time_equiv() {
        let a = SecretString::from("hunter2");
        let b = SecretString::from("hunter2");
        let c = SecretString::from("hunter3");

        assert_eq!(a, b);
        assert_ne!(a, c);
        // Different lengths must still return false, not panic.
        assert_ne!(a, SecretString::from("hunter2-extra"));
    }

    #[test]
    fn secret_string_redact_distinguishes_empty() {
        assert_eq!(SecretString::default().redact(), "");
        assert_eq!(SecretString::from("x").redact(), "[REDACTED]");
    }

    #[test]
    fn secret_string_is_empty() {
        assert!(SecretString::default().is_empty());
        assert!(SecretString::from("").is_empty());
        assert!(!SecretString::from("a").is_empty());
    }

    #[test]
    fn default_database_url_is_empty_not_leaky() {
        // Must not ship a default postgres URL with embedded credentials in the binary.
        let url = default_database_url();
        assert!(
            url.is_empty(),
            "default database URL must be empty — operators must supply a real value"
        );
    }

    #[test]
    fn engine_config_default_has_safe_values() {
        let cfg = EngineConfig::default();
        assert!(cfg.database.url.is_empty());
        assert!(cfg.api.api_key.is_empty());
        assert!(cfg.engine.encryption_key.is_empty());
        // No tenant enforcement by default — opt-in.
        assert!(!cfg.api.require_tenant_header);
    }

    #[test]
    fn database_config_parses_from_json() {
        // JSON shape mirrors the TOML layout at the field level (serde applies the
        // same Deserialize impls) — good enough to verify the defaults-kick-in path
        // without pulling `toml` into the types crate as a dev dep.
        let json = r#"{
            "database": {
                "backend": "sqlite",
                "url": "sqlite::memory:",
                "max_connections": 4
            }
        }"#;
        let cfg: EngineConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.database.backend, "sqlite");
        assert_eq!(cfg.database.url.expose(), "sqlite::memory:");
        assert_eq!(cfg.database.max_connections, 4);
        // run_migrations omitted — falls back to the default_true() default.
        assert!(cfg.database.run_migrations);
    }

    #[test]
    fn externalization_mode_default_is_threshold_64k() {
        let cfg = SchedulerConfig::default();
        assert!(matches!(
            cfg.externalization_mode,
            ExternalizationMode::Threshold { bytes: 65536 }
        ));
    }

    #[test]
    fn externalization_mode_parses_tagged_json() {
        let mode: ExternalizationMode =
            serde_json::from_str(r#"{"type":"threshold","bytes":32768}"#).unwrap();
        assert!(matches!(
            mode,
            ExternalizationMode::Threshold { bytes: 32768 }
        ));

        let mode: ExternalizationMode = serde_json::from_str(r#"{"type":"never"}"#).unwrap();
        assert!(matches!(mode, ExternalizationMode::Never));

        let mode: ExternalizationMode =
            serde_json::from_str(r#"{"type":"always_outputs"}"#).unwrap();
        assert!(matches!(mode, ExternalizationMode::AlwaysOutputs));
    }

    #[test]
    fn externalization_mode_context_threshold_accessor() {
        assert_eq!(
            ExternalizationMode::Threshold { bytes: 4096 }.context_threshold(),
            Some(4096)
        );
        assert_eq!(ExternalizationMode::Never.context_threshold(), None);
        assert_eq!(ExternalizationMode::AlwaysOutputs.context_threshold(), None);
    }

    #[test]
    fn externalization_mode_always_externalize_outputs_accessor() {
        assert!(ExternalizationMode::AlwaysOutputs.always_externalize_outputs());
        assert!(!ExternalizationMode::Never.always_externalize_outputs());
        assert!(!ExternalizationMode::Threshold { bytes: 1024 }.always_externalize_outputs());
    }
}
