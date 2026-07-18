//! Parked and in-flight outbound webhook deliveries.
//!
//! The outbox is the durability backbone of webhook delivery: terminal-state
//! events are written here in the same transaction as the instance state
//! change, then dispatched by the in-memory fast path or the engine's drain
//! loop. Rows whose retries are exhausted stay `parked` for operator
//! inspection and manual redelivery. One row per (url, event).

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Delivery lifecycle of an outbox row.
///
/// `Pending` rows are due for dispatch (the drain loop or the in-memory fast
/// path claims them); `InFlight` rows are claimed by a dispatcher right now
/// (a stale claim is reset to `Pending` if the dispatcher dies mid-dispatch);
/// `Parked` rows exhausted their retries and await manual redelivery — the
/// drain loop never touches them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum WebhookOutboxStatus {
    /// Due for dispatch at `next_attempt_at` (NULL = immediately).
    Pending,
    /// Claimed by a dispatcher; `claimed_at` bounds the claim for stale
    /// recovery.
    InFlight,
    /// Retries exhausted — manual redelivery only.
    Parked,
}

impl WebhookOutboxStatus {
    /// Stable label stored in the `status` column.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::InFlight => "in_flight",
            Self::Parked => "parked",
        }
    }

    /// Parse a stored label. Unknown values (written by a newer binary) read
    /// back as `Parked` — the safe, drain-loop-inert default.
    #[must_use]
    pub fn parse(s: &str) -> Self {
        match s {
            "pending" => Self::Pending,
            "in_flight" => Self::InFlight,
            _ => Self::Parked,
        }
    }
}

impl Default for WebhookOutboxStatus {
    /// Rows parked before the drain loop existed carry no status; they are
    /// semantically `Parked` (manual redelivery only).
    fn default() -> Self {
        Self::Parked
    }
}

/// One outbound webhook delivery. `Pending`/`InFlight` rows are being worked
/// by the engine; `Parked` rows failed after all retries and were kept for
/// manual inspection / redelivery.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct WebhookOutboxEntry {
    pub id: Uuid,
    /// The destination URL the delivery was targeting.
    pub url: String,
    /// The event type (e.g. `instance.failed`).
    pub event_type: String,
    /// The related instance id, if the event carried one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<Uuid>,
    /// The full serialized `WebhookEvent` payload, replayed verbatim on redelivery.
    pub payload: serde_json::Value,
    /// How many delivery attempts were made so far.
    pub attempts: i32,
    /// The last error (HTTP status or transport error) seen, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// When the delivery was first enqueued (or parked, for pre-drain rows).
    pub created_at: DateTime<Utc>,
    /// Links this entry to its attempt history (see
    /// `webhook_delivery::WebhookDeliveryAttempt`). `None` for rows parked
    /// before attempt tracking existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<Uuid>,
    /// Delivery lifecycle state (see [`WebhookOutboxStatus`]).
    #[serde(default)]
    pub status: WebhookOutboxStatus,
    /// When a `Pending` row becomes due for its next dispatch attempt.
    /// `None` means immediately due (fresh rows) or not scheduled (`Parked`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_attempt_at: Option<DateTime<Utc>>,
    /// When an `InFlight` row was claimed; stale claims older than the
    /// dispatcher's worst-case retry pass are recovered back to `Pending`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claimed_at: Option<DateTime<Utc>>,
}
