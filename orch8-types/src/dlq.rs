//! DLQ root-cause groups.
//!
//! A DLQ with two hundred failures rarely holds two hundred problems.
//! Failed instances are grouped by a stable, explainable
//! [`crate::failure::FailureFingerprint`] so operators fix causes, not
//! instances.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::failure::ErrorClass;

/// One root-cause group of failed instances.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct DlqGroup {
    /// Stable fingerprint hash (group key).
    pub fingerprint: String,
    /// The fingerprint's components — the grouping is explainable, never
    /// a black box.
    pub components: Vec<String>,
    pub error_class: ErrorClass,
    /// Stable error code shared by the group (`HTTP_STATUS`, `TIMEOUT`,
    /// `unknown`, ...).
    pub error_code: String,
    /// One representative (redacted) message.
    pub sample_message: String,
    /// Failed instances in this group.
    pub count: u64,
    pub first_occurrence: DateTime<Utc>,
    pub last_occurrence: DateTime<Utc>,
    /// Affected sequence names (bounded).
    pub sequences: Vec<String>,
    /// Affected block ids (bounded).
    pub blocks: Vec<String>,
    /// Affected handlers (bounded).
    pub handlers: Vec<String>,
    /// Up to ten newest instance ids for drill-down / sample retry.
    pub sample_instance_ids: Vec<Uuid>,
}

/// Request body for group retry.
#[derive(Debug, Clone, Deserialize, ToSchema)]
pub struct DlqGroupRetryRequest {
    pub mode: DlqRetryMode,
    /// Bulk only: an instance from this group whose *sample retry*
    /// succeeded (now `completed`). Verified server-side.
    #[serde(default)]
    pub sample_verified_instance_id: Option<Uuid>,
    /// Bulk only: explicit override when no successful sample exists.
    #[serde(default)]
    pub force: bool,
    /// Bulk only: cap on instances retried in this call (default 100,
    /// max 500).
    #[serde(default)]
    pub limit: Option<u32>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DlqRetryMode {
    /// Retry one representative instance and mark it so a later bulk
    /// call can verify its outcome.
    Sample,
    /// Retry every (still failed) instance in the group, bounded by
    /// `limit`.
    Bulk,
}

/// Response for group retry.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DlqGroupRetryResponse {
    pub fingerprint: String,
    pub mode: DlqRetryMode,
    /// Instances transitioned back to `scheduled`.
    pub retried: Vec<Uuid>,
    /// Instances skipped (no longer failed / no longer in the group).
    pub skipped: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_mode_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&DlqRetryMode::Sample).unwrap(),
            "\"sample\""
        );
        assert_eq!(
            serde_json::to_string(&DlqRetryMode::Bulk).unwrap(),
            "\"bulk\""
        );
    }

    #[test]
    fn retry_request_defaults() {
        let req: DlqGroupRetryRequest =
            serde_json::from_value(serde_json::json!({"mode": "sample"})).unwrap();
        assert_eq!(req.mode, DlqRetryMode::Sample);
        assert!(!req.force);
        assert!(req.sample_verified_instance_id.is_none());
        assert!(req.limit.is_none());
    }

    #[test]
    fn group_round_trips() {
        let g = DlqGroup {
            fingerprint: "deadbeefdeadbeef".into(),
            components: vec!["seq:x".into(), "code:HTTP_STATUS".into()],
            error_class: ErrorClass::ExternalDependency,
            error_code: "HTTP_STATUS".into(),
            sample_message: "http 503".into(),
            count: 200,
            first_occurrence: Utc::now(),
            last_occurrence: Utc::now(),
            sequences: vec!["checkout".into()],
            blocks: vec!["charge".into()],
            handlers: vec!["charge_card".into()],
            sample_instance_ids: vec![Uuid::now_v7()],
        };
        let back: DlqGroup = serde_json::from_str(&serde_json::to_string(&g).unwrap()).unwrap();
        assert_eq!(back, g);
    }
}
