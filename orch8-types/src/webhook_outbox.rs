//! Parked webhook deliveries.
//!
//! When an outbound webhook exhausts its retries, the engine parks the
//! delivery here instead of dropping it, so an operator can inspect and
//! redeliver it later. One row per failed (url, event) attempt.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// A webhook delivery that failed after all retries and was parked for manual
/// inspection / redelivery.
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
    /// How many delivery attempts were made before parking.
    pub attempts: i32,
    /// The last error (HTTP status or transport error) seen before parking.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// When the delivery was parked.
    pub created_at: DateTime<Utc>,
    /// Links this parked entry to its attempt history (see
    /// `webhook_delivery::WebhookDeliveryAttempt`). `None` for rows parked
    /// before attempt tracking existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub delivery_id: Option<Uuid>,
}
