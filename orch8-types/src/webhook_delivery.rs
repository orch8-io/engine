//! Webhook delivery attempt history.
//!
//! The outbox stores only the final parked row; these types record the
//! whole delivery story — every attempt with timestamp, status, duration,
//! normalized error class, and a bounded redacted error excerpt — so an
//! operator can see whether Orch8 rejected, attempted, retried,
//! redirected, parked, or delivered an event.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

/// Longest error excerpt persisted per attempt.
pub const MAX_ERROR_EXCERPT_LEN: usize = 512;

/// Normalized failure class for a delivery attempt. Metrics are emitted
/// by class (bounded cardinality), never by endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum DeliveryErrorClass {
    /// DNS resolution failed.
    Dns,
    /// TCP connect failed / connection refused or reset.
    Connect,
    /// TLS handshake / certificate problem.
    Tls,
    /// The request timed out.
    Timeout,
    /// A redirect was rejected (private/internal target or too many hops).
    RedirectRejected,
    /// The receiver answered with an HTTP error status.
    HttpStatus,
    /// Signing was requested but misconfigured.
    SignatureConfiguration,
    /// The response exceeded the allowed size.
    ResponseTooLarge,
    /// Blocked by internal policy before sending.
    InternalPolicy,
    /// Payload could not be serialized.
    Serialization,
    /// Delivery aborted by shutdown.
    Aborted,
    /// Anything else.
    Other,
}

impl DeliveryErrorClass {
    /// Stable label for metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dns => "dns",
            Self::Connect => "connect",
            Self::Tls => "tls",
            Self::Timeout => "timeout",
            Self::RedirectRejected => "redirect_rejected",
            Self::HttpStatus => "http_status",
            Self::SignatureConfiguration => "signature_configuration",
            Self::ResponseTooLarge => "response_too_large",
            Self::InternalPolicy => "internal_policy",
            Self::Serialization => "serialization",
            Self::Aborted => "aborted",
            Self::Other => "other",
        }
    }
}

/// Normalize a transport error string / HTTP status into a class.
///
/// The transport strings come from reqwest's error `Display`; matching is
/// substring-based and deliberately conservative — anything unrecognized
/// lands in `Other` rather than a wrong class.
#[must_use]
pub fn classify_error(error: &str, status: Option<u16>) -> DeliveryErrorClass {
    if let Some(status) = status
        && status >= 400
    {
        return DeliveryErrorClass::HttpStatus;
    }
    let lower = error.to_ascii_lowercase();
    if lower.contains("aborted by shutdown") {
        DeliveryErrorClass::Aborted
    } else if lower.contains("serialize") {
        DeliveryErrorClass::Serialization
    } else if lower.contains("redirect") {
        DeliveryErrorClass::RedirectRejected
    } else if lower.contains("dns") || lower.contains("resolve") {
        DeliveryErrorClass::Dns
    } else if lower.contains("certificate")
        || lower.contains("tls")
        || lower.contains("ssl")
        || lower.contains("handshake")
    {
        DeliveryErrorClass::Tls
    } else if lower.contains("timed out") || lower.contains("timeout") {
        DeliveryErrorClass::Timeout
    } else if lower.contains("connect")
        || lower.contains("connection refused")
        || lower.contains("connection reset")
        || lower.contains("broken pipe")
    {
        DeliveryErrorClass::Connect
    } else if lower.contains("body") && lower.contains("large") {
        DeliveryErrorClass::ResponseTooLarge
    } else if lower.starts_with("http ") {
        DeliveryErrorClass::HttpStatus
    } else {
        DeliveryErrorClass::Other
    }
}

/// One delivery attempt (bounded, redacted metadata only — never the
/// payload or response body).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WebhookDeliveryAttempt {
    pub id: Uuid,
    /// Groups the attempts of one delivery pass (initial emit or one
    /// redelivery).
    pub delivery_id: Uuid,
    pub url: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<Uuid>,
    /// 1-based attempt number within the delivery.
    pub attempt_number: i32,
    pub attempted_at: DateTime<Utc>,
    pub duration_ms: i64,
    pub success: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status_code: Option<i32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_class: Option<DeliveryErrorClass>,
    /// Bounded, redacted excerpt of the transport error (never a response
    /// body).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error_excerpt: Option<String>,
    /// Whether the request was HMAC-signed.
    pub signed: bool,
}

impl WebhookDeliveryAttempt {
    /// Bound the error excerpt at construction so oversized transport
    /// errors can never bloat storage.
    pub fn set_error(&mut self, error: &str, status: Option<u16>) {
        self.error_class = Some(classify_error(error, status));
        let mut excerpt = error.to_string();
        if excerpt.len() > MAX_ERROR_EXCERPT_LEN {
            let mut end = MAX_ERROR_EXCERPT_LEN;
            while end > 0 && !excerpt.is_char_boundary(end) {
                end -= 1;
            }
            excerpt.truncate(end);
            excerpt.push('…');
        }
        self.error_excerpt = Some(excerpt);
    }
}

/// Aggregated view of one delivery (all attempts in a group).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct WebhookDeliverySummary {
    pub delivery_id: Uuid,
    pub url: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<Uuid>,
    pub attempts: i64,
    pub delivered: bool,
    pub first_attempt_at: DateTime<Utc>,
    pub last_attempt_at: DateTime<Utc>,
    /// Error class of the final attempt when the delivery failed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error_class: Option<DeliveryErrorClass>,
}

/// Filters for the delivery list endpoint.
#[derive(Debug, Clone, Default)]
pub struct DeliveryFilter {
    pub url: Option<String>,
    pub event_type: Option<String>,
    /// `Some(true)` = only delivered, `Some(false)` = only failed.
    pub delivered: Option<bool>,
    pub error_class: Option<DeliveryErrorClass>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_class_serializes_snake_case_and_matches_as_str() {
        for class in [
            DeliveryErrorClass::Dns,
            DeliveryErrorClass::Connect,
            DeliveryErrorClass::Tls,
            DeliveryErrorClass::Timeout,
            DeliveryErrorClass::RedirectRejected,
            DeliveryErrorClass::HttpStatus,
            DeliveryErrorClass::SignatureConfiguration,
            DeliveryErrorClass::ResponseTooLarge,
            DeliveryErrorClass::InternalPolicy,
            DeliveryErrorClass::Serialization,
            DeliveryErrorClass::Aborted,
            DeliveryErrorClass::Other,
        ] {
            assert_eq!(
                serde_json::to_string(&class).unwrap(),
                format!("\"{}\"", class.as_str())
            );
        }
    }

    #[test]
    fn classify_recognizes_transport_errors() {
        assert_eq!(
            classify_error("error sending request: dns error: failed to lookup", None),
            DeliveryErrorClass::Dns
        );
        assert_eq!(
            classify_error("connection refused", None),
            DeliveryErrorClass::Connect
        );
        assert_eq!(
            classify_error("invalid peer certificate: Expired", None),
            DeliveryErrorClass::Tls
        );
        assert_eq!(
            classify_error("operation timed out", None),
            DeliveryErrorClass::Timeout
        );
        assert_eq!(
            classify_error(
                "blocked: redirect targets a private/internal network address",
                None
            ),
            DeliveryErrorClass::RedirectRejected
        );
        assert_eq!(
            classify_error("aborted by shutdown", None),
            DeliveryErrorClass::Aborted
        );
        assert_eq!(
            classify_error("serialize: key must be a string", None),
            DeliveryErrorClass::Serialization
        );
        assert_eq!(classify_error("http 503", None), DeliveryErrorClass::HttpStatus);
        assert_eq!(
            classify_error("mysterious gremlins", None),
            DeliveryErrorClass::Other
        );
    }

    #[test]
    fn status_code_dominates_classification() {
        assert_eq!(
            classify_error("anything", Some(503)),
            DeliveryErrorClass::HttpStatus
        );
        // 2xx/3xx statuses never reach classification as failures, but a
        // low status must not force HttpStatus.
        assert_eq!(
            classify_error("connection refused", Some(0)),
            DeliveryErrorClass::Connect
        );
    }

    fn attempt() -> WebhookDeliveryAttempt {
        WebhookDeliveryAttempt {
            id: Uuid::now_v7(),
            delivery_id: Uuid::now_v7(),
            url: "https://example.com/hook".into(),
            event_type: "instance.failed".into(),
            instance_id: None,
            attempt_number: 1,
            attempted_at: Utc::now(),
            duration_ms: 12,
            success: false,
            status_code: None,
            error_class: None,
            error_excerpt: None,
            signed: true,
        }
    }

    #[test]
    fn set_error_bounds_excerpt_length() {
        let mut a = attempt();
        a.set_error(&"x".repeat(10_000), None);
        let excerpt = a.error_excerpt.as_ref().unwrap();
        assert!(excerpt.len() <= MAX_ERROR_EXCERPT_LEN + '…'.len_utf8());
        assert!(excerpt.ends_with('…'));
    }

    #[test]
    fn set_error_respects_utf8_boundaries() {
        let mut a = attempt();
        let long = "é".repeat(MAX_ERROR_EXCERPT_LEN); // 2 bytes each
        a.set_error(&long, None);
        // Must not panic and must stay valid UTF-8 (implicitly checked by
        // String), bounded.
        assert!(a.error_excerpt.as_ref().unwrap().len() <= MAX_ERROR_EXCERPT_LEN + 3);
    }

    #[test]
    fn set_error_classifies() {
        let mut a = attempt();
        a.set_error("http 503", Some(503));
        assert_eq!(a.error_class, Some(DeliveryErrorClass::HttpStatus));
        assert_eq!(a.error_excerpt.as_deref(), Some("http 503"));
    }

    #[test]
    fn attempt_round_trips() {
        let mut a = attempt();
        a.set_error("connection reset by peer", None);
        let back: WebhookDeliveryAttempt =
            serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
        assert_eq!(back, a);
    }
}
