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
    #[must_use]
    pub const fn new(s: String) -> Self {
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
    pub const fn redact(&self) -> &'static str {
        if self.0.is_empty() {
            ""
        } else {
            REDACTED_PLACEHOLDER
        }
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
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

impl Drop for SecretString {
    #[allow(unsafe_code)]
    fn drop(&mut self) {
        // SAFETY: zero the backing buffer so secrets don't linger in freed memory.
        // `write_volatile` prevents the compiler from eliding the writes.
        let v = unsafe { self.0.as_mut_vec() };
        for b in v.iter_mut() {
            unsafe { std::ptr::write_volatile(std::ptr::from_mut::<u8>(b), 0) };
        }
    }
}

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
    #[serde(default)]
    pub artifacts: ArtifactConfig,
    #[serde(default)]
    pub telemetry: TelemetryConfig,
}

/// Selectable durable artifact backends. In-memory is intentionally absent —
/// losing artifacts on restart would break the engine's durability contract.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactBackend {
    /// Disabled — artifact ops fail loudly (`Unsupported`).
    None,
    /// Local filesystem. **Only durable on a persistent volume** — on ephemeral
    /// container filesystems (Cloud Run, k8s without a PVC) artifacts are lost
    /// on restart/redeploy. Prefer S3/R2 for cloud deployments.
    Local,
    /// S3-compatible object storage (AWS S3 / Cloudflare R2 / `MinIO`).
    S3,
}

/// Durable artifact storage (binary blobs produced/consumed by steps).
///
/// `backend`:
/// - `"none"` (default) — artifacts disabled; artifact ops fail loudly.
/// - `"local"` — local filesystem at `path`. Durable **only on a persistent
///   volume**; on ephemeral container FS it is lost on restart. See
///   [`ArtifactBackend::Local`].
/// - `"s3"` — S3-compatible object storage (AWS S3 / Cloudflare R2 / `MinIO`).
///
/// In-memory storage is deliberately not a configurable backend: losing
/// artifacts on restart would silently break the engine's durability contract.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArtifactConfig {
    #[serde(default = "default_artifact_backend")]
    pub backend: String,
    /// Filesystem directory for `backend = "local"`.
    #[serde(default = "default_artifact_path")]
    pub path: String,
    #[serde(default)]
    pub s3_bucket: String,
    #[serde(default)]
    pub s3_region: String,
    /// Custom endpoint for non-AWS S3 (R2/`MinIO`). Empty → AWS default.
    #[serde(default)]
    pub s3_endpoint: String,
    #[serde(default)]
    pub s3_access_key_id: SecretString,
    #[serde(default)]
    pub s3_secret_access_key: SecretString,
    /// Allow plain HTTP for the S3 endpoint (e.g. local `MinIO`).
    #[serde(default)]
    pub s3_allow_http: bool,
}

impl Default for ArtifactConfig {
    fn default() -> Self {
        Self {
            backend: default_artifact_backend(),
            path: default_artifact_path(),
            s3_bucket: String::new(),
            s3_region: String::new(),
            s3_endpoint: String::new(),
            s3_access_key_id: SecretString::default(),
            s3_secret_access_key: SecretString::default(),
            s3_allow_http: false,
        }
    }
}

impl ArtifactConfig {
    /// Parse the string `backend` into a typed [`ArtifactBackend`], rejecting
    /// unknown values up-front so a typo fails at startup with a clear message
    /// rather than silently disabling artifacts.
    ///
    /// # Errors
    /// Returns a message naming the offending value when it isn't recognised.
    pub fn backend_kind(&self) -> Result<ArtifactBackend, String> {
        match self.backend.as_str() {
            "none" | "" => Ok(ArtifactBackend::None),
            "local" => Ok(ArtifactBackend::Local),
            "s3" => Ok(ArtifactBackend::S3),
            other => Err(format!(
                "unknown artifacts.backend: {other:?} (expected none|local|s3)"
            )),
        }
    }
}

fn default_artifact_backend() -> String {
    "none".to_string()
}

fn default_artifact_path() -> String {
    "orch8-artifacts".to_string()
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
    #[serde(default)]
    pub run_migrations: bool,
    /// Postgres schema to use for this instance (schema-per-instance isolation).
    #[serde(default)]
    pub search_path: Option<String>,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            backend: default_backend(),
            url: default_database_url(),
            max_connections: default_max_connections(),
            run_migrations: false,
            search_path: None,
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

const fn default_max_connections() -> u32 {
    64
}

/// How the engine decides which payloads leave the inline context and
/// land in `externalized_state`.
///
/// Ships as `Threshold { bytes: 65536 }` — oversized fields go external,
/// everything smaller stays inline for zero-RTT reads.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    pub const fn context_threshold(&self) -> Option<u32> {
        match self {
            Self::Never | Self::AlwaysOutputs => None,
            Self::Threshold { bytes } => Some(*bytes),
        }
    }

    /// Returns `true` iff block outputs should always be externalized.
    #[must_use]
    pub const fn always_externalize_outputs(&self) -> bool {
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
    /// How often the cron loop ticks, in seconds. Default: 10s.
    /// Set to a smaller value in tests to reduce cron fire latency.
    #[serde(default = "default_cron_tick_secs")]
    pub cron_tick_secs: u64,
    /// Maximum total step executions (including retries) for a single instance.
    /// When exceeded the instance is failed. `0` means unlimited (default).
    /// Primarily used by the mobile SDK to prevent runaway workflows on-device.
    #[serde(default)]
    pub max_steps_per_instance: u32,
    /// Time source for scheduling decisions (claiming due instances, delay /
    /// send-window / rate-limit deferrals, retry backoff, deadline and
    /// human-input timeout checks, cron evaluation). Defaults to
    /// [`crate::clock::SystemClock`] — production behavior is unchanged.
    /// Tests inject a [`crate::clock::ManualClock`] here to advance time
    /// manually. Not configurable via files/env; skipped by serde.
    #[serde(skip)]
    pub clock: crate::clock::SharedClock,
    /// Artifact retention, in seconds. When `> 0`, the background GC sweeper
    /// deletes the durable artifacts of instances that have been in a terminal
    /// state for longer than this window. `0` (default) disables the sweep —
    /// artifacts are kept until removed out-of-band (e.g. an S3 lifecycle
    /// policy or `delete_instance_artifacts`). Set via
    /// `ORCH8_ARTIFACT_RETENTION_SECS`.
    #[serde(default)]
    pub artifact_retention_secs: u64,
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
            cron_tick_secs: default_cron_tick_secs(),
            max_steps_per_instance: 0,
            clock: crate::clock::SharedClock::default(),
            artifact_retention_secs: 0,
        }
    }
}

const fn default_worker_reaper_tick_secs() -> u64 {
    30
}

const fn default_worker_reaper_stale_secs() -> u64 {
    60
}

const fn default_node_reaper_tick_secs() -> u64 {
    60
}

const fn default_node_reaper_stale_secs() -> u64 {
    120
}

const fn default_cron_tick_secs() -> u64 {
    10
}

const fn default_max_context_bytes() -> u32 {
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
    /// Optional shared secret. When set, each outbound delivery is signed:
    /// the body is HMAC-SHA256'd over `"{timestamp}.{body}"` and sent as
    /// `X-Orch8-Signature: sha256=<hex>` alongside `X-Orch8-Timestamp`, so the
    /// receiver can verify authenticity and reject replays. Mirrors the
    /// inbound trigger-secret model, in the outbound direction.
    /// `SecretString` so the signing key is redacted in `Debug`/serialized
    /// output (consistent with the other secrets in this config) — leaking the
    /// signing secret would defeat the purpose of signing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub secret: Option<SecretString>,
}

impl Default for WebhookConfig {
    fn default() -> Self {
        Self {
            urls: Vec::new(),
            timeout_secs: default_webhook_timeout_secs(),
            max_retries: default_webhook_max_retries(),
            secret: None,
        }
    }
}

const fn default_webhook_timeout_secs() -> u64 {
    10
}

const fn default_webhook_max_retries() -> u32 {
    3
}

const fn default_tick_interval_ms() -> u64 {
    100
}

const fn default_batch_size() -> u32 {
    256
}

const fn default_max_concurrent() -> u32 {
    128
}

const fn default_grace_period() -> u64 {
    30
}

const fn default_stale_threshold() -> u64 {
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
    ///
    /// Defaults to `true` (secure by default): without the header, the
    /// per-resource tenant checks fall open and any API-key holder can read
    /// across tenants. Single-tenant deployments may explicitly set this to
    /// `false` (an informed opt-out, surfaced loudly at startup).
    #[serde(default = "default_require_tenant_header")]
    pub require_tenant_header: bool,
    /// Maximum in-flight HTTP requests (global concurrency cap). 0 disables the cap.
    ///
    /// Perf#10: this is a concurrency limit (tower `ConcurrencyLimitLayer`),
    /// not an RPS rate limiter. The `rate_limit_rps` alias is accepted for
    /// backward compatibility with older configs and the
    /// `ORCH8_RATE_LIMIT_RPS` env var.
    #[serde(default, alias = "rate_limit_rps")]
    pub max_concurrent_requests: u64,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            http_addr: default_http_addr(),
            cors_origins: default_cors_origins(),
            api_key: SecretString::default(),
            require_tenant_header: default_require_tenant_header(),
            max_concurrent_requests: 0,
        }
    }
}

const fn default_cors_origins() -> String {
    String::new()
}

/// Secure by default: tenant isolation is enforced unless explicitly disabled.
const fn default_require_tenant_header() -> bool {
    true
}

fn default_grpc_addr() -> String {
    "127.0.0.1:50051".to_string()
}

fn default_http_addr() -> String {
    "127.0.0.1:8080".to_string()
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

/// OpenTelemetry trace export (OTLP). Disabled unless `otlp_endpoint` is set —
/// when it is empty (the default) the server behaves exactly as before: no
/// OpenTelemetry layer is installed and there is no runtime cost.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelemetryConfig {
    /// OTLP collector endpoint, e.g. `"http://localhost:4317"` (Langfuse,
    /// Datadog Agent, Grafana Alloy, otel-collector…). Empty = export disabled.
    /// Env override: `ORCH8_OTLP_ENDPOINT`.
    #[serde(default)]
    pub otlp_endpoint: String,
    /// OTLP transport protocol. Only `"grpc"` is currently supported.
    /// Env override: `ORCH8_OTLP_PROTOCOL`.
    #[serde(default = "default_otlp_protocol")]
    pub otlp_protocol: String,
}

impl Default for TelemetryConfig {
    fn default() -> Self {
        Self {
            otlp_endpoint: String::new(),
            otlp_protocol: default_otlp_protocol(),
        }
    }
}

impl TelemetryConfig {
    /// Trace export is enabled iff an endpoint is configured.
    pub fn otlp_enabled(&self) -> bool {
        !self.otlp_endpoint.is_empty()
    }
}

fn default_otlp_protocol() -> String {
    "grpc".to_string()
}

impl EngineConfig {
    /// Validate configuration values, returning all errors found.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Database
        match self.database.backend.as_str() {
            "postgres" | "sqlite" => {}
            other => errors.push(format!(
                "database.backend: unknown backend \"{other}\" (expected \"postgres\" or \"sqlite\")"
            )),
        }
        if self.database.max_connections == 0 {
            errors.push("database.max_connections must be > 0".into());
        }

        // Engine / scheduler
        if self.engine.tick_interval_ms == 0 {
            errors.push("engine.tick_interval_ms must be > 0".into());
        }
        if self.engine.batch_size == 0 {
            errors.push("engine.batch_size must be > 0".into());
        }
        if self.engine.max_concurrent_steps == 0 {
            errors.push("engine.max_concurrent_steps must be > 0".into());
        }
        if self.engine.stale_instance_threshold_secs > 0
            && self.engine.tick_interval_ms > 0
            && self.engine.stale_instance_threshold_secs * 1000 <= self.engine.tick_interval_ms
        {
            errors.push(
                "engine.stale_instance_threshold_secs must be greater than tick_interval_ms".into(),
            );
        }
        if !self.engine.encryption_key.is_empty() && self.engine.encryption_key.expose().len() != 64
        {
            errors.push("engine.encryption_key must be exactly 64 hex characters".into());
        }

        // Logging
        match self.logging.level.as_str() {
            "trace" | "debug" | "info" | "warn" | "error" => {}
            other => errors.push(format!(
                "logging.level: unknown level \"{other}\" (expected trace/debug/info/warn/error)"
            )),
        }

        // Telemetry — only validated when export is actually enabled, so an
        // unset endpoint keeps zero-config startup byte-identical to before.
        if self.telemetry.otlp_enabled() {
            match self.telemetry.otlp_protocol.as_str() {
                "grpc" | "" => {}
                other => errors.push(format!(
                    "telemetry.otlp_protocol: unsupported protocol \"{other}\" (only \"grpc\" is supported)"
                )),
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }
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
        // Tenant enforcement is ON by default (secure by default); single-tenant
        // deployments must opt out explicitly.
        assert!(cfg.api.require_tenant_header);
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
        // run_migrations omitted — falls back to the serde default (false).
        assert!(!cfg.database.run_migrations);
    }

    #[test]
    fn api_config_accepts_legacy_rate_limit_rps_alias() {
        // Perf#10: the field was renamed to `max_concurrent_requests` but the
        // old `rate_limit_rps` spelling must still deserialize to the new
        // field so existing orch8.toml files keep working.
        let json = r#"{ "rate_limit_rps": 500 }"#;
        let cfg: ApiConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_concurrent_requests, 500);
    }

    #[test]
    fn api_config_prefers_canonical_name() {
        let json = r#"{ "max_concurrent_requests": 750 }"#;
        let cfg: ApiConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.max_concurrent_requests, 750);
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

    #[test]
    fn validate_default_config_passes() {
        let cfg = EngineConfig::default();
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn validate_catches_zero_tick_interval() {
        let mut cfg = EngineConfig::default();
        cfg.engine.tick_interval_ms = 0;
        let errs = cfg.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("tick_interval_ms")));
    }

    #[test]
    fn validate_catches_zero_batch_size() {
        let mut cfg = EngineConfig::default();
        cfg.engine.batch_size = 0;
        let errs = cfg.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("batch_size")));
    }

    #[test]
    fn validate_catches_unknown_backend() {
        let mut cfg = EngineConfig::default();
        cfg.database.backend = "mysql".into();
        let errs = cfg.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("mysql")));
    }

    #[test]
    fn validate_catches_bad_log_level() {
        let mut cfg = EngineConfig::default();
        cfg.logging.level = "verbose".into();
        let errs = cfg.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("verbose")));
    }

    #[test]
    fn validate_catches_bad_encryption_key_length() {
        let mut cfg = EngineConfig::default();
        cfg.engine.encryption_key = SecretString::from("tooshort");
        let errs = cfg.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("encryption_key")));
    }

    #[test]
    fn validate_catches_stale_less_than_tick() {
        let mut cfg = EngineConfig::default();
        cfg.engine.tick_interval_ms = 5000;
        cfg.engine.stale_instance_threshold_secs = 4; // 4s = 4000ms < 5000ms tick
        let errs = cfg.validate().unwrap_err();
        assert!(errs.iter().any(|e| e.contains("stale_instance_threshold")));
    }

    #[test]
    fn validate_collects_multiple_errors() {
        let mut cfg = EngineConfig::default();
        cfg.engine.tick_interval_ms = 0;
        cfg.engine.batch_size = 0;
        cfg.database.backend = "mysql".into();
        let errs = cfg.validate().unwrap_err();
        assert!(
            errs.len() >= 3,
            "expected at least 3 errors, got {}",
            errs.len()
        );
    }

    #[test]
    fn secret_string_from_string_and_str() {
        let from_string = SecretString::from("from-string".to_string());
        assert_eq!(from_string.expose(), "from-string");

        let from_str = SecretString::from("from-str");
        assert_eq!(from_str.expose(), "from-str");

        let new = SecretString::new("new-method".to_string());
        assert_eq!(new.expose(), "new-method");
    }

    #[test]
    fn secret_string_display_shows_redacted() {
        let secret = SecretString::from("my-secret");
        assert_eq!(format!("{secret}"), "[REDACTED]");

        let empty = SecretString::default();
        assert_eq!(format!("{empty}"), "");
    }

    #[test]
    fn secret_string_creation_and_redaction() {
        let secret = SecretString::new("top-secret".to_string());
        assert_eq!(secret.expose(), "top-secret");
        assert_eq!(format!("{secret}"), "[REDACTED]");
        assert_eq!(secret.redact(), "[REDACTED]");
    }

    #[test]
    fn externalization_mode_json_roundtrip_never() {
        let original = ExternalizationMode::Never;
        let json = serde_json::to_string(&original).unwrap();
        let roundtripped: ExternalizationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(original, roundtripped);
    }

    #[test]
    fn externalization_mode_json_roundtrip_threshold() {
        let original = ExternalizationMode::Threshold { bytes: 8192 };
        let json = serde_json::to_string(&original).unwrap();
        let roundtripped: ExternalizationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(original, roundtripped);
    }

    #[test]
    fn externalization_mode_json_roundtrip_always_outputs() {
        let original = ExternalizationMode::AlwaysOutputs;
        let json = serde_json::to_string(&original).unwrap();
        let roundtripped: ExternalizationMode = serde_json::from_str(&json).unwrap();
        assert_eq!(original, roundtripped);
    }

    #[test]
    fn scheduler_config_defaults() {
        let cfg = SchedulerConfig::default();
        assert_eq!(cfg.tick_interval_ms, 100);
        assert_eq!(cfg.batch_size, 256);
        assert_eq!(cfg.max_concurrent_steps, 128);
        assert_eq!(cfg.shutdown_grace_period_secs, 30);
        assert_eq!(cfg.stale_instance_threshold_secs, 300);
        assert_eq!(cfg.max_instances_per_tenant, 0);
        assert_eq!(cfg.externalize_output_threshold, 0);
        assert!(cfg.encryption_key.is_empty());
        assert_eq!(
            cfg.max_context_bytes,
            crate::context::DEFAULT_MAX_CONTEXT_BYTES
        );
        assert!(matches!(
            cfg.externalization_mode,
            ExternalizationMode::Threshold { bytes: 65536 }
        ));
        assert_eq!(cfg.worker_reaper_tick_secs, 30);
        assert_eq!(cfg.worker_reaper_stale_secs, 60);
        assert_eq!(cfg.node_reaper_tick_secs, 60);
        assert_eq!(cfg.node_reaper_stale_secs, 120);
        assert_eq!(cfg.cron_tick_secs, 10);
        assert_eq!(cfg.max_steps_per_instance, 0);
    }

    #[test]
    fn scheduler_config_deserializes_from_json() {
        let json = r#"{
            "tick_interval_ms": 50,
            "batch_size": 128
        }"#;
        let cfg: SchedulerConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.tick_interval_ms, 50);
        assert_eq!(cfg.batch_size, 128);
        // Defaults applied for missing fields.
        assert_eq!(cfg.max_concurrent_steps, 128);
        assert_eq!(cfg.shutdown_grace_period_secs, 30);
    }

    #[test]
    fn api_config_defaults() {
        let cfg = ApiConfig::default();
        assert_eq!(cfg.grpc_addr, "127.0.0.1:50051");
        assert_eq!(cfg.http_addr, "127.0.0.1:8080");
        assert_eq!(cfg.cors_origins, "");
        assert!(cfg.api_key.is_empty());
        assert!(cfg.require_tenant_header);
        assert_eq!(cfg.max_concurrent_requests, 0);
    }

    #[test]
    fn api_config_deserializes_from_json() {
        let json = r#"{
            "grpc_addr": "0.0.0.0:50051",
            "http_addr": "0.0.0.0:8080",
            "cors_origins": "*"
        }"#;
        let cfg: ApiConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.grpc_addr, "0.0.0.0:50051");
        assert_eq!(cfg.http_addr, "0.0.0.0:8080");
        assert_eq!(cfg.cors_origins, "*");
        assert!(cfg.api_key.is_empty());
    }

    #[test]
    fn webhook_config_defaults() {
        let cfg = WebhookConfig::default();
        assert!(cfg.urls.is_empty());
        assert_eq!(cfg.timeout_secs, 10);
        assert_eq!(cfg.max_retries, 3);
    }

    #[test]
    fn webhook_config_deserializes_from_json() {
        let json = r#"{
            "urls": ["https://example.com/hook"],
            "timeout_secs": 5
        }"#;
        let cfg: WebhookConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.urls, vec!["https://example.com/hook"]);
        assert_eq!(cfg.timeout_secs, 5);
        assert_eq!(cfg.max_retries, 3);
    }

    #[test]
    fn engine_config_defaults_exhaustive() {
        let cfg = EngineConfig::default();
        assert_eq!(cfg.database.backend, "postgres");
        assert!(cfg.database.url.is_empty());
        assert_eq!(cfg.database.max_connections, 64);
        assert!(!cfg.database.run_migrations);
        assert_eq!(cfg.database.search_path, None);
        assert_eq!(cfg.engine.tick_interval_ms, 100);
        assert_eq!(cfg.api.grpc_addr, "127.0.0.1:50051");
        assert_eq!(cfg.logging.level, "info");
        assert!(!cfg.logging.json);
    }

    #[test]
    fn engine_config_deserializes_from_json() {
        let json = r#"{
            "database": { "backend": "sqlite", "max_connections": 8 },
            "engine": { "tick_interval_ms": 200, "batch_size": 512 },
            "api": { "http_addr": "0.0.0.0:9000" },
            "logging": { "level": "debug", "json": true }
        }"#;
        let cfg: EngineConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.database.backend, "sqlite");
        assert_eq!(cfg.database.max_connections, 8);
        assert_eq!(cfg.engine.tick_interval_ms, 200);
        assert_eq!(cfg.engine.batch_size, 512);
        assert_eq!(cfg.api.http_addr, "0.0.0.0:9000");
        assert_eq!(cfg.logging.level, "debug");
        assert!(cfg.logging.json);
    }

    #[test]
    fn database_config_defaults() {
        let cfg = DatabaseConfig::default();
        assert_eq!(cfg.backend, "postgres");
        assert!(cfg.url.is_empty());
        assert_eq!(cfg.max_connections, 64);
        assert!(!cfg.run_migrations);
        assert_eq!(cfg.search_path, None);
    }

    #[test]
    fn database_config_deserializes_from_json() {
        let json = r#"{
            "backend": "sqlite",
            "url": "sqlite::memory:",
            "max_connections": 4,
            "run_migrations": false,
            "search_path": "public"
        }"#;
        let cfg: DatabaseConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.backend, "sqlite");
        assert_eq!(cfg.url.expose(), "sqlite::memory:");
        assert_eq!(cfg.max_connections, 4);
        assert!(!cfg.run_migrations);
        assert_eq!(cfg.search_path, Some("public".to_string()));
    }

    #[test]
    fn logging_config_defaults() {
        let cfg = LoggingConfig::default();
        assert_eq!(cfg.level, "info");
        assert!(!cfg.json);
    }

    #[test]
    fn logging_config_deserializes_from_json() {
        let json = r#"{"level": "warn", "json": true}"#;
        let cfg: LoggingConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.level, "warn");
        assert!(cfg.json);
    }

    #[test]
    fn telemetry_config_defaults_to_disabled() {
        let cfg = TelemetryConfig::default();
        assert!(cfg.otlp_endpoint.is_empty());
        assert_eq!(cfg.otlp_protocol, "grpc");
        assert!(!cfg.otlp_enabled());
        // EngineConfig embeds the same default — and a default config must
        // validate cleanly (no telemetry errors when export is off).
        let engine_cfg = EngineConfig::default();
        assert!(!engine_cfg.telemetry.otlp_enabled());
        assert!(engine_cfg.validate().is_ok());
    }

    #[test]
    fn telemetry_config_parses_endpoint_from_toml_section() {
        let toml_src = r#"
            [telemetry]
            otlp_endpoint = "http://localhost:4317"
        "#;
        let cfg: EngineConfig = toml::from_str(toml_src).unwrap();
        assert_eq!(cfg.telemetry.otlp_endpoint, "http://localhost:4317");
        assert_eq!(cfg.telemetry.otlp_protocol, "grpc"); // default kicks in
        assert!(cfg.telemetry.otlp_enabled());
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn telemetry_config_missing_section_means_disabled() {
        let cfg: EngineConfig = toml::from_str("[logging]\nlevel = \"info\"\n").unwrap();
        assert!(!cfg.telemetry.otlp_enabled());
    }

    #[test]
    fn telemetry_validation_rejects_unknown_protocol_only_when_enabled() {
        let mut cfg = EngineConfig::default();
        // Unknown protocol with NO endpoint: export disabled, must not error.
        cfg.telemetry.otlp_protocol = "carrier-pigeon".into();
        assert!(cfg.validate().is_ok());
        // Same protocol with an endpoint set: now it must be rejected.
        cfg.telemetry.otlp_endpoint = "http://localhost:4317".into();
        let errors = cfg.validate().unwrap_err();
        assert!(
            errors.iter().any(|e| e.contains("otlp_protocol")),
            "expected otlp_protocol error, got: {errors:?}"
        );
    }

    #[test]
    fn config_deserialization_from_json_string_for_key_fields() {
        let json = r#"{
            "database": { "url": "postgres://user:pass@localhost/db" },
            "engine": { "encryption_key": "aabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd" },
            "api": { "api_key": "secret-api-key" }
        }"#;
        let cfg: EngineConfig = serde_json::from_str(json).unwrap();
        assert_eq!(
            cfg.database.url.expose(),
            "postgres://user:pass@localhost/db"
        );
        assert_eq!(
            cfg.engine.encryption_key.expose(),
            "aabbccdd11223344556677889900aabbccdd11223344556677889900aabbccdd"
        );
        assert_eq!(cfg.api.api_key.expose(), "secret-api-key");
    }
}
