use serde::{Deserialize, Serialize};

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
    #[serde(default = "default_database_url")]
    pub url: String,
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,
    #[serde(default = "default_true")]
    pub run_migrations: bool,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            url: default_database_url(),
            max_connections: default_max_connections(),
            run_migrations: true,
        }
    }
}

fn default_database_url() -> String {
    "postgres://orch8:orch8@localhost:5432/orch8".to_string()
}

fn default_max_connections() -> u32 {
    20
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
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            tick_interval_ms: default_tick_interval_ms(),
            batch_size: default_batch_size(),
            max_concurrent_steps: default_max_concurrent(),
            shutdown_grace_period_secs: default_grace_period(),
            stale_instance_threshold_secs: default_stale_threshold(),
        }
    }
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
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            grpc_addr: default_grpc_addr(),
            http_addr: default_http_addr(),
        }
    }
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
