use serde::{Deserialize, Serialize};
use std::fmt;

/// A wrapper for sensitive strings that redacts the value in Debug and Serialize output.
/// Use `.expose()` to access the inner value when you actually need it.
#[derive(Clone, Default, Deserialize)]
#[serde(transparent)]
pub struct SecretString(String);

impl SecretString {
    pub fn new(s: String) -> Self {
        Self(s)
    }

    /// Access the actual secret value.
    pub fn expose(&self) -> &str {
        &self.0
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
            f.write_str("SecretString(\"***\")")
        }
    }
}

impl Serialize for SecretString {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        if self.0.is_empty() {
            s.serialize_str("")
        } else {
            s.serialize_str("***")
        }
    }
}

impl From<String> for SecretString {
    fn from(s: String) -> Self {
        Self(s)
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
    SecretString::new("postgres://orch8:orch8@localhost:5432/orch8".to_string())
}

fn default_max_connections() -> u32 {
    64
}

fn default_true() -> bool {
    true
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
        }
    }
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
