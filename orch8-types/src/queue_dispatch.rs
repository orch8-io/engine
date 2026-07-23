//! Per-queue dispatch mode.
//!
//! By default tasks land in a queue and external workers **poll** for them. A
//! queue can instead be configured for **push**: when a task is enqueued to it,
//! the engine POSTs a signed task envelope to a target URL (the durable
//! `worker_tasks` row is still written, so the worker reports completion the
//! same way). Keyed by `(tenant_id, queue_name)`.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

/// How tasks on a queue reach workers.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DispatchMode {
    /// Workers poll the queue (default; unchanged behavior).
    #[default]
    Poll,
    /// The engine POSTs a signed envelope to `push_url` at enqueue.
    Push,
}

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct QueueDispatchConfig {
    pub tenant_id: String,
    pub queue_name: String,
    pub mode: DispatchMode,
    /// Target URL for `push` mode (required when `mode = push`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_url: Option<String>,
    /// Optional HMAC secret used to sign the pushed envelope (same scheme as
    /// outbound webhooks). Never serialized back out; redacted in `Debug`.
    #[serde(default, skip_serializing)]
    pub secret: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

// Hand-written `Debug` (instead of derive) so the HMAC signing secret can
// never leak through a `tracing`/`log` statement that debug-formats the
// config — serialization is suppressed via `skip_serializing`, but a derived
// `Debug` would print the plaintext.
impl std::fmt::Debug for QueueDispatchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueDispatchConfig")
            .field("tenant_id", &self.tenant_id)
            .field("queue_name", &self.queue_name)
            .field("mode", &self.mode)
            .field("push_url", &self.push_url)
            .field("secret", &self.secret.as_ref().map(|_| "[REDACTED]"))
            .field("created_at", &self.created_at)
            .field("updated_at", &self.updated_at)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_redacts_secret() {
        let cfg = QueueDispatchConfig {
            tenant_id: "t1".into(),
            queue_name: "q".into(),
            mode: DispatchMode::Push,
            push_url: Some("https://example.com/hook".into()),
            secret: Some("super-secret-hmac-key".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let debug = format!("{cfg:?}");
        assert!(!debug.contains("super-secret-hmac-key"), "{debug}");
        assert!(debug.contains("[REDACTED]"), "{debug}");
    }

    #[test]
    fn secret_is_never_serialized() {
        let cfg = QueueDispatchConfig {
            tenant_id: "t1".into(),
            queue_name: "q".into(),
            mode: DispatchMode::Push,
            push_url: None,
            secret: Some("super-secret-hmac-key".into()),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        };
        let json = serde_json::to_string(&cfg).unwrap();
        assert!(!json.contains("super-secret-hmac-key"), "{json}");
        assert!(!json.contains("secret"), "{json}");
    }
}
