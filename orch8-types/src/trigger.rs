use std::fmt;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::ids::TenantId;

/// The kind of event source that fires a trigger.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum TriggerType {
    #[default]
    Webhook,
    Nats,
    FileWatch,
    /// In-process event bus — fired by `POST /triggers/{slug}/fire` or
    /// internally by workflows via the `emit_event` built-in handler.
    /// Unlike webhooks, event triggers carry no HMAC validation — they're
    /// intended for trusted server-to-server or in-cluster integration.
    Event,
    /// Polling trigger executed via the `ActivePieces` Node sidecar. The
    /// engine periodically asks the sidecar to run a piece trigger's poll
    /// (config holds piece, trigger, auth, props, and a schedule) and
    /// creates one instance per returned item.
    ActivepiecesPoll,
}

impl fmt::Display for TriggerType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Webhook => f.write_str("webhook"),
            Self::Nats => f.write_str("nats"),
            Self::FileWatch => f.write_str("file_watch"),
            Self::Event => f.write_str("event"),
            Self::ActivepiecesPoll => f.write_str("activepieces_poll"),
        }
    }
}

impl TriggerType {
    #[must_use]
    pub fn from_str_loose(s: &str) -> Option<Self> {
        match s {
            "webhook" => Some(Self::Webhook),
            "nats" => Some(Self::Nats),
            "file_watch" => Some(Self::FileWatch),
            "event" => Some(Self::Event),
            "activepieces_poll" => Some(Self::ActivepiecesPoll),
            _ => None,
        }
    }
}

/// Runtime state for a polling trigger (`TriggerType::ActivepiecesPoll`).
///
/// Stored separately from [`TriggerDef`] so the poll loop can persist its
/// cursor and failure bookkeeping without rewriting the user-authored
/// trigger definition (avoiding lost-update races with API edits).
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TriggerPollState {
    /// Slug of the trigger this state belongs to.
    pub slug: String,
    /// Opaque cursor blob returned by the sidecar's last successful poll
    /// (`ActivePieces` store contents — `lastPoll` epoch, item ids, ...).
    /// Sent back verbatim on the next poll for deduplication.
    #[serde(default)]
    pub state: serde_json::Value,
    /// When the last poll attempt (success or failure) completed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_poll_at: Option<DateTime<Utc>>,
    /// Error message from the most recent failed poll. Cleared on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// Number of consecutive failed polls. Reset to 0 on success.
    #[serde(default)]
    pub consecutive_failures: i32,
    pub updated_at: DateTime<Utc>,
}

impl TriggerPollState {
    /// Fresh state for a trigger that has never polled.
    #[must_use]
    pub fn empty(slug: impl Into<String>) -> Self {
        Self {
            slug: slug.into(),
            state: serde_json::Value::Null,
            last_poll_at: None,
            last_error: None,
            consecutive_failures: 0,
            updated_at: Utc::now(),
        }
    }
}

/// A persisted trigger definition that maps an event source to a sequence.
#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct TriggerDef {
    /// Unique slug used in the trigger URL / identifier.
    pub slug: String,
    /// Sequence name to instantiate when fired.
    pub sequence_name: String,
    /// Optional specific version. If omitted, uses latest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<i32>,
    /// Tenant ID for created instances.
    pub tenant_id: TenantId,
    /// Namespace for created instances.
    #[serde(default = "crate::serde_defaults::default_namespace")]
    pub namespace: String,
    /// Whether this trigger is active.
    #[serde(default = "crate::serde_defaults::yes")]
    pub enabled: bool,
    /// Optional secret for HMAC validation of incoming requests.
    /// Redacted in Debug/Serialize output — use `.secret.as_ref().map(|s| s.expose())` to access.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<String>)]
    pub secret: Option<crate::config::SecretString>,
    /// Trigger type: `webhook`, `nats`, `file_watch`.
    #[serde(default)]
    pub trigger_type: TriggerType,
    /// Type-specific configuration (JSON).
    #[serde(default)]
    pub config: serde_json::Value,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trigger_type_display() {
        assert_eq!(TriggerType::Webhook.to_string(), "webhook");
        assert_eq!(TriggerType::Nats.to_string(), "nats");
        assert_eq!(TriggerType::FileWatch.to_string(), "file_watch");
        assert_eq!(TriggerType::Event.to_string(), "event");
        assert_eq!(
            TriggerType::ActivepiecesPoll.to_string(),
            "activepieces_poll"
        );
    }

    #[test]
    fn activepieces_poll_round_trips_display_and_serde() {
        // Display ↔ from_str_loose must agree (storage round-trips through text).
        assert_eq!(
            TriggerType::from_str_loose("activepieces_poll"),
            Some(TriggerType::ActivepiecesPoll)
        );
        let t: TriggerType = serde_json::from_str(r#""activepieces_poll""#).unwrap();
        assert_eq!(t, TriggerType::ActivepiecesPoll);
        assert_eq!(serde_json::to_string(&t).unwrap(), r#""activepieces_poll""#);
    }

    #[test]
    fn trigger_poll_state_empty_and_round_trip() {
        let s = TriggerPollState::empty("my-poll");
        assert_eq!(s.slug, "my-poll");
        assert!(s.state.is_null());
        assert!(s.last_poll_at.is_none());
        assert!(s.last_error.is_none());
        assert_eq!(s.consecutive_failures, 0);

        let json = serde_json::to_string(&s).unwrap();
        let back: TriggerPollState = serde_json::from_str(&json).unwrap();
        assert_eq!(back.slug, "my-poll");
        assert_eq!(back.consecutive_failures, 0);
        // Optional fields are skipped when None.
        assert!(!json.contains("last_error"));
        assert!(!json.contains("last_poll_at"));
    }

    #[test]
    fn trigger_type_default_is_webhook() {
        assert_eq!(TriggerType::default(), TriggerType::Webhook);
    }

    #[test]
    fn trigger_type_from_str_loose() {
        assert_eq!(
            TriggerType::from_str_loose("webhook"),
            Some(TriggerType::Webhook)
        );
        assert_eq!(TriggerType::from_str_loose("nats"), Some(TriggerType::Nats));
        assert_eq!(
            TriggerType::from_str_loose("file_watch"),
            Some(TriggerType::FileWatch)
        );
        assert_eq!(
            TriggerType::from_str_loose("event"),
            Some(TriggerType::Event)
        );
        assert_eq!(TriggerType::from_str_loose("unknown"), None);
        assert_eq!(TriggerType::from_str_loose(""), None);
        assert_eq!(TriggerType::from_str_loose("WEBHOOK"), None);
    }

    #[test]
    fn trigger_type_serde_round_trip() {
        let webhook: TriggerType = serde_json::from_str(r#""webhook""#).unwrap();
        assert_eq!(webhook, TriggerType::Webhook);
        assert_eq!(serde_json::to_string(&webhook).unwrap(), r#""webhook""#);

        let nats: TriggerType = serde_json::from_str(r#""nats""#).unwrap();
        assert_eq!(nats, TriggerType::Nats);

        let fw: TriggerType = serde_json::from_str(r#""file_watch""#).unwrap();
        assert_eq!(fw, TriggerType::FileWatch);
    }

    #[test]
    fn trigger_type_serde_rejects_invalid() {
        assert!(serde_json::from_str::<TriggerType>(r#""cron""#).is_err());
        assert!(serde_json::from_str::<TriggerType>(r#""Webhook""#).is_err());
    }

    #[test]
    fn trigger_def_defaults() {
        let json = r#"{
            "slug": "on-deploy",
            "sequence_name": "deploy-pipeline",
            "tenant_id": "t1",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z"
        }"#;
        let def: TriggerDef = serde_json::from_str(json).unwrap();
        assert!(def.enabled);
        assert_eq!(def.namespace, "default");
        assert_eq!(def.trigger_type, TriggerType::Webhook);
        assert_eq!(def.config, serde_json::Value::Null);
        assert!(def.version.is_none());
        assert!(def.secret.is_none());
    }

    #[test]
    fn trigger_def_round_trip() {
        let now = Utc::now();
        let def = TriggerDef {
            slug: "on-push".into(),
            sequence_name: "ci-pipeline".into(),
            version: Some(3),
            tenant_id: TenantId::unchecked("t1"),
            namespace: "prod".into(),
            enabled: false,
            secret: Some(crate::config::SecretString::new("s3cret".into())),
            trigger_type: TriggerType::Nats,
            config: serde_json::json!({"subject": "events.>"}),
            created_at: now,
            updated_at: now,
        };
        // Verify secret is accessible before serialization.
        assert_eq!(
            def.secret.as_ref().map(crate::config::SecretString::expose),
            Some("s3cret")
        );
        let json = serde_json::to_string(&def).unwrap();
        // SecretString serializes as "[REDACTED]" — round-trip produces the redacted value.
        assert!(json.contains("\"[REDACTED]\""));
        let back: TriggerDef = serde_json::from_str(&json).unwrap();
        assert_eq!(back.slug, "on-push");
        assert_eq!(back.trigger_type, TriggerType::Nats);
        assert!(!back.enabled);
        assert_eq!(back.version, Some(3));
        assert_eq!(
            back.secret
                .as_ref()
                .map(crate::config::SecretString::expose),
            Some("[REDACTED]")
        );
    }

    #[test]
    fn trigger_def_skip_serializing_none_fields() {
        let now = Utc::now();
        let def = TriggerDef {
            slug: "t".into(),
            sequence_name: "s".into(),
            version: None,
            tenant_id: TenantId::unchecked(""),
            namespace: "default".into(),
            enabled: true,
            secret: None,
            trigger_type: TriggerType::Webhook,
            config: serde_json::Value::Null,
            created_at: now,
            updated_at: now,
        };
        let json = serde_json::to_string(&def).unwrap();
        assert!(!json.contains("version"));
        assert!(!json.contains("secret"));
    }
}
