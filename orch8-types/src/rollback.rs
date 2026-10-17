//! Error budget / auto-rollback policy types.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// A per-tenant, per-sequence rollback policy.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackPolicy {
    pub id: i64,
    pub tenant_id: String,
    pub sequence_name: String,
    pub error_rate_threshold: f64,
    pub time_window_secs: i32,
    pub enabled: bool,
    /// Minimum seconds between consecutive rollback triggers (prevents flapping).
    /// Default: 3600 (1 hour).
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: i32,
    /// Error rate must remain above threshold for this many seconds before triggering.
    /// Default: 60. Set to 0 to disable sustained-breach confirmation.
    #[serde(default = "default_confirmation_window")]
    pub confirmation_window_secs: i32,
    /// Optional webhook URL to POST alert payloads when rollback triggers.
    #[serde(default)]
    pub webhook_url: Option<String>,
    /// Minimum number of observed runs (successes + failures) inside the
    /// window before the error rate is allowed to trigger a rollback. Stops a
    /// single (possibly forged) error report from unpublishing a sequence.
    /// Default: 20.
    #[serde(default = "default_min_samples")]
    pub min_samples: i32,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

/// Default for [`RollbackPolicy::min_samples`].
pub const DEFAULT_MIN_SAMPLES: i32 = 20;

fn default_min_samples() -> i32 {
    DEFAULT_MIN_SAMPLES
}

fn default_cooldown() -> i32 {
    3600
}

fn default_confirmation_window() -> i32 {
    60
}

/// A record of an auto-rollback trigger event.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RollbackHistory {
    pub id: i64,
    pub tenant_id: String,
    pub sequence_name: String,
    pub triggered_at: DateTime<Utc>,
    pub error_rate: f64,
    pub threshold: f64,
    pub previous_manifest_version: Option<String>,
    pub reason: String,
    pub alert_sent: bool,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollback_policy_serde_roundtrip() {
        let now = Utc::now();
        let policy = RollbackPolicy {
            id: 42,
            tenant_id: "tenant-1".into(),
            sequence_name: "seq-a".into(),
            error_rate_threshold: 0.05,
            time_window_secs: 300,
            enabled: true,
            cooldown_secs: 3600,
            confirmation_window_secs: 60,
            webhook_url: None,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&policy).unwrap();
        let decoded: RollbackPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, 42);
        assert_eq!(decoded.tenant_id, "tenant-1");
        assert_eq!(decoded.sequence_name, "seq-a");
        assert!((decoded.error_rate_threshold - 0.05).abs() < f64::EPSILON);
        assert_eq!(decoded.time_window_secs, 300);
        assert!(decoded.enabled);
        assert_eq!(decoded.cooldown_secs, 3600);
        assert_eq!(decoded.confirmation_window_secs, 60);
    }

    #[test]
    fn rollback_history_serde_roundtrip() {
        let now = Utc::now();
        let hist = RollbackHistory {
            id: 7,
            tenant_id: "tenant-2".into(),
            sequence_name: "seq-b".into(),
            triggered_at: now,
            error_rate: 0.15,
            threshold: 0.1,
            previous_manifest_version: Some("v1.2.3".into()),
            reason: "threshold_breach".into(),
            alert_sent: false,
        };
        let json = serde_json::to_string(&hist).unwrap();
        let decoded: RollbackHistory = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id, 7);
        assert_eq!(decoded.previous_manifest_version, Some("v1.2.3".into()));
        assert!(!decoded.alert_sent);
    }

    #[test]
    fn rollback_policy_json_snake_case() {
        let json = r#"{
            "id": 1,
            "tenant_id": "t",
            "sequence_name": "s",
            "error_rate_threshold": 0.1,
            "time_window_secs": 60,
            "enabled": false,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }"#;
        let policy: RollbackPolicy = serde_json::from_str(json).unwrap();
        assert!(!policy.enabled);
        assert_eq!(policy.time_window_secs, 60);
        assert_eq!(policy.cooldown_secs, 3600);
        assert_eq!(policy.confirmation_window_secs, 60);
    }

    #[test]
    fn rollback_policy_custom_hysteresis() {
        let json = r#"{
            "id": 1,
            "tenant_id": "t",
            "sequence_name": "s",
            "error_rate_threshold": 0.1,
            "time_window_secs": 300,
            "enabled": true,
            "cooldown_secs": 7200,
            "confirmation_window_secs": 120,
            "created_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z"
        }"#;
        let policy: RollbackPolicy = serde_json::from_str(json).unwrap();
        assert_eq!(policy.cooldown_secs, 7200);
        assert_eq!(policy.confirmation_window_secs, 120);
    }
}
