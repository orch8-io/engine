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

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct QueueDispatchConfig {
    pub tenant_id: String,
    pub queue_name: String,
    pub mode: DispatchMode,
    /// Target URL for `push` mode (required when `mode = push`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub push_url: Option<String>,
    /// Optional HMAC secret used to sign the pushed envelope (same scheme as
    /// outbound webhooks). Never serialized back out.
    #[serde(default, skip_serializing)]
    pub secret: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}
