//! Structured failure envelope.
//!
//! Human-readable error messages stay, but they are no longer the primary
//! machine interface. Every permanent failure gets a [`FailureEnvelope`]
//! with a stable code and class so DLQ grouping, diagnosis, release gates,
//! and delivery inspection can operate on structured data instead of
//! parsing prose.
//!
//! The envelope also produces the DLQ *root-cause fingerprint*: a stable,
//! explainable hash built from normalized structured inputs — never from
//! tenant secrets, full payloads, volatile timestamps, UUIDs, or stack
//! addresses.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::finding::ResourceRef;

/// Broad, stable classification of where a failure originates. Used to
/// separate "fix your workflow" from "fix your infrastructure" without
/// reading messages.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema,
)]
#[serde(rename_all = "snake_case")]
pub enum ErrorClass {
    /// The handler itself failed on valid infrastructure (business logic,
    /// bad data).
    Application,
    /// Missing or invalid configuration: unknown handler, bad template,
    /// invalid params.
    Configuration,
    /// Worker-side problems: no compatible worker, stale claim, version
    /// mismatch.
    Worker,
    /// Missing, expired, or rejected credential.
    Credential,
    /// Engine policy stopped execution: budget, rate limit, URL policy,
    /// circuit breaker.
    Policy,
    /// An external dependency failed: HTTP 5xx, DNS, connect, TLS,
    /// upstream timeout.
    ExternalDependency,
    /// The step or instance exceeded its own timeout or deadline.
    Timeout,
    /// Cancelled by an operator or by control flow.
    Cancelled,
    /// A bug or invariant violation inside the engine.
    Internal,
}

impl ErrorClass {
    /// Stable string used inside fingerprints. Must never change for an
    /// existing variant — fingerprints are persisted.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Application => "application",
            Self::Configuration => "configuration",
            Self::Worker => "worker",
            Self::Credential => "credential",
            Self::Policy => "policy",
            Self::ExternalDependency => "external_dependency",
            Self::Timeout => "timeout",
            Self::Cancelled => "cancelled",
            Self::Internal => "internal",
        }
    }
}

/// Structured description of one failure, preserved alongside the
/// human-readable message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FailureEnvelope {
    /// Stable machine code, SCREAMING_SNAKE_CASE, e.g. `HANDLER_TIMEOUT`,
    /// `HTTP_STATUS`, `NO_COMPATIBLE_WORKER`. Codes are producer-defined
    /// but must be stable across releases.
    pub error_code: String,
    pub error_class: ErrorClass,
    /// The human-readable message. Kept verbatim; never used as the
    /// primary machine interface.
    pub message: String,
    /// Block that failed, when the failure is block-scoped.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block_id: Option<String>,
    /// Handler that failed, when applicable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub handler: Option<String>,
    /// Normalized external status (HTTP status, DB SQLSTATE class, exit
    /// code), when the failure came from an external dependency.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub external_status: Option<String>,
    /// The credential / queue / worker / plugin / breaker involved, when
    /// applicable. This is an identifier, never a secret.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resource: Option<ResourceRef>,
    /// Whether the engine considered this retryable at the time.
    pub retryable: bool,
    /// Producer-supplied structured details (already redacted).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
    pub occurred_at: DateTime<Utc>,
}

impl FailureEnvelope {
    #[must_use]
    pub fn new(
        error_code: impl Into<String>,
        error_class: ErrorClass,
        message: impl Into<String>,
        retryable: bool,
        occurred_at: DateTime<Utc>,
    ) -> Self {
        Self {
            error_code: error_code.into(),
            error_class,
            message: message.into(),
            block_id: None,
            handler: None,
            external_status: None,
            resource: None,
            retryable,
            details: None,
            occurred_at,
        }
    }

    #[must_use]
    pub fn with_block(mut self, block_id: impl Into<String>) -> Self {
        self.block_id = Some(block_id.into());
        self
    }

    #[must_use]
    pub fn with_handler(mut self, handler: impl Into<String>) -> Self {
        self.handler = Some(handler.into());
        self
    }

    #[must_use]
    pub fn with_external_status(mut self, status: impl Into<String>) -> Self {
        self.external_status = Some(status.into());
        self
    }

    #[must_use]
    pub fn with_resource(mut self, resource: ResourceRef) -> Self {
        self.resource = Some(resource);
        self
    }

    #[must_use]
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

/// Inputs identifying *where* a failure happened, combined with the
/// envelope to build a fingerprint.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, ToSchema)]
pub struct FingerprintScope {
    /// Sequence identity (UUID string) — part of the fingerprint because
    /// the same error in two workflows is two incidents.
    pub sequence_id: String,
    /// Sequence version.
    pub sequence_version: i64,
}

/// A stable, explainable root-cause fingerprint for grouping DLQ entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, ToSchema)]
pub struct FailureFingerprint {
    /// Hex-encoded 64-bit FNV-1a hash of the normalized components.
    pub hash: String,
    /// The components that went into the hash, in order — kept so the
    /// grouping is explainable, never a black box.
    pub components: Vec<String>,
}

/// Build the root-cause fingerprint for a failure.
///
/// Components, in order: sequence id, sequence version, block id, handler,
/// error class, error code, external status, resource kind+id, and — only
/// when no error code more specific than the class is available — a
/// bounded normalized-message hash as fallback.
#[must_use]
pub fn fingerprint(scope: &FingerprintScope, envelope: &FailureEnvelope) -> FailureFingerprint {
    let mut components: Vec<String> = vec![
        format!("seq:{}", scope.sequence_id),
        format!("v:{}", scope.sequence_version),
    ];
    if let Some(block) = &envelope.block_id {
        components.push(format!("block:{block}"));
    }
    if let Some(handler) = &envelope.handler {
        components.push(format!("handler:{handler}"));
    }
    components.push(format!("class:{}", envelope.error_class.as_str()));
    components.push(format!("code:{}", envelope.error_code));
    if let Some(status) = &envelope.external_status {
        components.push(format!("status:{status}"));
    }
    if let Some(resource) = &envelope.resource {
        components.push(format!("resource:{}:{}", resource.kind, resource.id));
    }
    // Fallback: when the code carries no more information than the class,
    // add a normalized-message hash so distinct generic failures don't all
    // collapse into one group.
    if envelope.error_code.eq_ignore_ascii_case("unknown")
        || envelope.error_code.is_empty()
    {
        components.push(format!(
            "msg:{:016x}",
            fnv1a(normalize_message(&envelope.message).as_bytes())
        ));
    }

    let mut hasher_input = String::new();
    for c in &components {
        hasher_input.push_str(c);
        hasher_input.push('\u{1f}'); // unit separator so "a"+"bc" != "ab"+"c"
    }
    FailureFingerprint {
        hash: format!("{:016x}", fnv1a(hasher_input.as_bytes())),
        components,
    }
}

/// Normalize a human-readable message for fallback hashing: lowercase,
/// strip UUIDs, hex runs, numbers, and timestamps, collapse whitespace,
/// and bound the length. Volatile identifiers must not survive, or every
/// failure becomes its own group.
#[must_use]
pub fn normalize_message(message: &str) -> String {
    const MAX_LEN: usize = 256;

    let mut out = String::with_capacity(message.len().min(MAX_LEN));
    let mut chars = message.chars().peekable();
    let mut last_was_space = true; // suppress leading whitespace
    let mut last_was_placeholder = false;

    while let Some(c) = chars.next() {
        if out.len() >= MAX_LEN {
            break;
        }
        if c.is_whitespace() {
            if !last_was_space {
                out.push(' ');
                last_was_space = true;
                last_was_placeholder = false;
            }
            continue;
        }
        if c.is_ascii_alphanumeric() {
            // Collect the full alphanumeric+dash token to classify it.
            let mut token = String::new();
            token.push(c);
            while let Some(&next) = chars.peek() {
                if next.is_ascii_alphanumeric() || next == '-' {
                    token.push(next);
                    chars.next();
                } else {
                    break;
                }
            }
            if is_volatile_token(&token) {
                if !last_was_placeholder {
                    out.push('#');
                    last_was_placeholder = true;
                }
            } else {
                out.push_str(&token.to_ascii_lowercase());
                last_was_placeholder = false;
            }
            last_was_space = false;
            continue;
        }
        out.push(c);
        last_was_space = false;
        last_was_placeholder = false;
    }
    // Adjacent volatile tokens separated by whitespace each emitted a
    // placeholder ("# # #"); collapse them — they are one volatile region.
    while out.contains("# #") {
        out = out.replace("# #", "#");
    }
    // Trim a trailing space introduced by whitespace collapsing.
    while out.ends_with(' ') {
        out.pop();
    }
    out.truncate(MAX_LEN);
    out
}

/// A token is volatile when it is (mostly) digits, a UUID, or a long hex
/// run — identifiers that vary per occurrence of the same root cause.
fn is_volatile_token(token: &str) -> bool {
    if token.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }
    // UUID with dashes (8-4-4-4-12) or any dash-separated all-hex token.
    let hexish = token
        .chars()
        .all(|c| c.is_ascii_hexdigit() || c == '-');
    if hexish && token.len() >= 8 {
        return true;
    }
    // Mixed alphanumeric tokens that contain digits and are long are
    // usually generated IDs (request ids, ULIDs).
    let digits = token.chars().filter(char::is_ascii_digit).count();
    token.len() >= 16 && digits >= 4
}

/// Derive a structured envelope from a legacy human-readable error
/// message. Used to lazily backfill envelopes for failures recorded
/// before (or outside) structured error reporting; producers that know
/// their error should construct [`FailureEnvelope`] directly instead.
///
/// Heuristics are deliberately conservative: unrecognized messages get
/// `error_code = "unknown"`, which makes [`fingerprint`] fall back to the
/// bounded normalized-message hash.
#[must_use]
pub fn envelope_from_message(
    message: &str,
    retryable: bool,
    occurred_at: DateTime<Utc>,
) -> FailureEnvelope {
    let lower = message.to_ascii_lowercase();

    // HTTP status extraction: "http 503", "status 429", "returned 502".
    let status = extract_http_status(&lower);
    if let Some(status) = status {
        let class = match status {
            401 | 403 => ErrorClass::Credential,
            408 | 429 => ErrorClass::ExternalDependency,
            400..=499 => ErrorClass::Application,
            _ => ErrorClass::ExternalDependency,
        };
        return FailureEnvelope::new("HTTP_STATUS", class, message, retryable, occurred_at)
            .with_external_status(status.to_string());
    }

    let (code, class) = if lower.contains("timed out")
        || lower.contains("timeout")
        || lower.contains("deadline")
    {
        ("TIMEOUT", ErrorClass::Timeout)
    } else if lower.contains("credential") || lower.contains("unauthorized") {
        ("CREDENTIAL", ErrorClass::Credential)
    } else if lower.contains("no compatible worker") || lower.contains("no worker") {
        ("NO_WORKER", ErrorClass::Worker)
    } else if lower.contains("circuit breaker") || lower.contains("breaker open") {
        ("CIRCUIT_OPEN", ErrorClass::Policy)
    } else if lower.contains("budget") {
        ("BUDGET_EXCEEDED", ErrorClass::Policy)
    } else if lower.contains("rate limit") || lower.contains("rate-limit") {
        ("RATE_LIMITED", ErrorClass::Policy)
    } else if lower.contains("blocked:") || lower.contains("url policy") {
        ("URL_POLICY", ErrorClass::Policy)
    } else if lower.contains("unknown handler")
        || lower.contains("unknown template root")
        || lower.contains("template")
        || lower.contains("invalid params")
        || lower.contains("schema")
    {
        ("CONFIGURATION", ErrorClass::Configuration)
    } else if lower.contains("cancel") {
        ("CANCELLED", ErrorClass::Cancelled)
    } else if lower.contains("dns") || lower.contains("connect") || lower.contains("tls") {
        ("TRANSPORT", ErrorClass::ExternalDependency)
    } else {
        ("unknown", ErrorClass::Application)
    };
    FailureEnvelope::new(code, class, message, retryable, occurred_at)
}

/// Find an HTTP status code in a lowercased message ("http 503",
/// "status 429", "returned 404 not found").
fn extract_http_status(lower: &str) -> Option<u16> {
    for marker in ["http ", "status ", "returned "] {
        if let Some(pos) = lower.find(marker) {
            let rest = &lower[pos + marker.len()..];
            let digits: String = rest.chars().take_while(char::is_ascii_digit).collect();
            if digits.len() == 3
                && let Ok(n) = digits.parse::<u16>()
                && (100..=599).contains(&n)
            {
                return Some(n);
            }
        }
    }
    None
}

/// FNV-1a 64-bit — small, dependency-free, stable across platforms.
/// Suitable for grouping keys; not a security boundary.
#[must_use]
fn fnv1a(bytes: &[u8]) -> u64 {
    const OFFSET: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET;
    for &b in bytes {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn t0() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 7, 11, 12, 0, 0).unwrap()
    }

    fn scope() -> FingerprintScope {
        FingerprintScope {
            sequence_id: "0198f0a2-0000-7000-8000-000000000001".into(),
            sequence_version: 3,
        }
    }

    fn envelope(code: &str) -> FailureEnvelope {
        FailureEnvelope::new(code, ErrorClass::ExternalDependency, "boom", false, t0())
            .with_block("charge")
            .with_handler("http_request")
    }

    // --- ErrorClass ---

    #[test]
    fn error_class_serializes_snake_case() {
        assert_eq!(
            serde_json::to_string(&ErrorClass::ExternalDependency).unwrap(),
            "\"external_dependency\""
        );
        assert_eq!(serde_json::to_string(&ErrorClass::Worker).unwrap(), "\"worker\"");
    }

    #[test]
    fn error_class_as_str_matches_serde() {
        for class in [
            ErrorClass::Application,
            ErrorClass::Configuration,
            ErrorClass::Worker,
            ErrorClass::Credential,
            ErrorClass::Policy,
            ErrorClass::ExternalDependency,
            ErrorClass::Timeout,
            ErrorClass::Cancelled,
            ErrorClass::Internal,
        ] {
            let serde_repr = serde_json::to_string(&class).unwrap();
            assert_eq!(serde_repr, format!("\"{}\"", class.as_str()));
        }
    }

    // --- FailureEnvelope ---

    #[test]
    fn envelope_round_trips_through_json() {
        let e = envelope("HTTP_STATUS")
            .with_external_status("503")
            .with_resource(ResourceRef::new("credential", "billing_key"))
            .with_details(serde_json::json!({"attempt": 3}));
        let json = serde_json::to_string(&e).unwrap();
        let back: FailureEnvelope = serde_json::from_str(&json).unwrap();
        assert_eq!(back, e);
    }

    #[test]
    fn envelope_omits_absent_optional_fields() {
        let e = FailureEnvelope::new("X", ErrorClass::Internal, "m", true, t0());
        let v = serde_json::to_value(&e).unwrap();
        let obj = v.as_object().unwrap();
        for key in ["block_id", "handler", "external_status", "resource", "details"] {
            assert!(!obj.contains_key(key), "{key} should be omitted");
        }
    }

    // --- Fingerprinting ---

    #[test]
    fn identical_failures_share_a_fingerprint() {
        let a = fingerprint(&scope(), &envelope("HTTP_STATUS").with_external_status("503"));
        let b = fingerprint(&scope(), &envelope("HTTP_STATUS").with_external_status("503"));
        assert_eq!(a.hash, b.hash);
        assert_eq!(a.components, b.components);
    }

    #[test]
    fn fingerprint_ignores_message_when_code_is_specific() {
        let mut e1 = envelope("HTTP_STATUS");
        e1.message = "request req-8f2a9b17 to https://x failed".into();
        let mut e2 = envelope("HTTP_STATUS");
        e2.message = "request req-99999999 to https://x failed".into();
        assert_eq!(fingerprint(&scope(), &e1).hash, fingerprint(&scope(), &e2).hash);
    }

    #[test]
    fn fingerprint_differs_by_block() {
        // envelope() sets block "charge"; the same failure in "refund"
        // must group separately.
        let a = fingerprint(&scope(), &envelope("HTTP_STATUS"));
        let mut e = envelope("HTTP_STATUS");
        e.block_id = Some("refund".into());
        let b = fingerprint(&scope(), &e);
        assert_ne!(b.hash, a.hash);
    }

    #[test]
    fn fingerprint_differs_by_sequence_version() {
        let mut s2 = scope();
        s2.sequence_version = 4;
        let e = envelope("HTTP_STATUS");
        assert_ne!(fingerprint(&scope(), &e).hash, fingerprint(&s2, &e).hash);
    }

    #[test]
    fn fingerprint_differs_by_error_class() {
        let mut e1 = envelope("TIMEOUT");
        e1.error_class = ErrorClass::Timeout;
        let mut e2 = envelope("TIMEOUT");
        e2.error_class = ErrorClass::ExternalDependency;
        assert_ne!(fingerprint(&scope(), &e1).hash, fingerprint(&scope(), &e2).hash);
    }

    #[test]
    fn fingerprint_includes_resource_identity() {
        let a = fingerprint(
            &scope(),
            &envelope("CREDENTIAL_EXPIRED")
                .with_resource(ResourceRef::new("credential", "billing_key")),
        );
        let b = fingerprint(
            &scope(),
            &envelope("CREDENTIAL_EXPIRED")
                .with_resource(ResourceRef::new("credential", "other_key")),
        );
        assert_ne!(a.hash, b.hash);
    }

    #[test]
    fn unknown_code_falls_back_to_normalized_message() {
        let mut e1 = envelope("unknown");
        e1.message = "widget frobnication failed".into();
        let mut e2 = envelope("unknown");
        e2.message = "database exploded".into();
        assert_ne!(fingerprint(&scope(), &e1).hash, fingerprint(&scope(), &e2).hash);

        // But volatile parts of the message do not split groups.
        let mut e3 = envelope("unknown");
        e3.message = "widget frobnication failed".into();
        assert_eq!(fingerprint(&scope(), &e1).hash, fingerprint(&scope(), &e3).hash);
    }

    #[test]
    fn fingerprint_components_are_explainable() {
        let fp = fingerprint(
            &scope(),
            &envelope("HTTP_STATUS").with_external_status("503"),
        );
        assert!(fp.components.iter().any(|c| c == "block:charge"));
        assert!(fp.components.iter().any(|c| c == "handler:http_request"));
        assert!(fp.components.iter().any(|c| c == "class:external_dependency"));
        assert!(fp.components.iter().any(|c| c == "code:HTTP_STATUS"));
        assert!(fp.components.iter().any(|c| c == "status:503"));
    }

    #[test]
    fn fingerprint_component_boundaries_are_unambiguous() {
        // "block:ab" + "handler:c" must differ from "block:a" + "handler:bc"
        let mut e1 = envelope("X");
        e1.block_id = Some("ab".into());
        e1.handler = Some("c".into());
        let mut e2 = envelope("X");
        e2.block_id = Some("a".into());
        e2.handler = Some("bc".into());
        assert_ne!(fingerprint(&scope(), &e1).hash, fingerprint(&scope(), &e2).hash);
    }

    // --- normalize_message ---

    #[test]
    fn normalize_strips_uuids() {
        let n = normalize_message("instance 0198f0a2-1111-7000-8000-00000000abcd not found");
        assert_eq!(n, "instance # not found");
    }

    #[test]
    fn normalize_strips_numbers() {
        assert_eq!(
            normalize_message("retry 17 of 30 failed after 2500 ms"),
            "retry # of # failed after # ms"
        );
    }

    #[test]
    fn normalize_collapses_adjacent_volatile_tokens() {
        assert_eq!(normalize_message("id 123 456 789 bad"), "id # bad");
    }

    #[test]
    fn normalize_lowercases_and_collapses_whitespace() {
        assert_eq!(
            normalize_message("  Connection   REFUSED\n by  peer "),
            "connection refused by peer"
        );
    }

    #[test]
    fn normalize_strips_long_hex_runs() {
        assert_eq!(
            normalize_message("stack at deadbeefcafe1234 in frame"),
            "stack at # in frame"
        );
    }

    #[test]
    fn normalize_keeps_short_words_with_digits() {
        // "s3" and "utf8" style tokens must survive.
        assert_eq!(normalize_message("s3 upload failed utf8 decode"), "s3 upload failed utf8 decode");
    }

    #[test]
    fn normalize_bounds_length() {
        let long = "word ".repeat(200);
        assert!(normalize_message(&long).len() <= 256);
    }

    #[test]
    fn normalize_handles_empty_and_symbol_only() {
        assert_eq!(normalize_message(""), "");
        assert_eq!(normalize_message("!!!"), "!!!");
    }

    // --- envelope_from_message ---

    #[test]
    fn message_derivation_extracts_http_status() {
        let e = envelope_from_message("http 503", true, t0());
        assert_eq!(e.error_code, "HTTP_STATUS");
        assert_eq!(e.error_class, ErrorClass::ExternalDependency);
        assert_eq!(e.external_status.as_deref(), Some("503"));

        let e = envelope_from_message("upstream returned 404 not found", false, t0());
        assert_eq!(e.external_status.as_deref(), Some("404"));
        assert_eq!(e.error_class, ErrorClass::Application);

        let e = envelope_from_message("status 401 from api", false, t0());
        assert_eq!(e.error_class, ErrorClass::Credential);

        let e = envelope_from_message("status 429 too many requests", true, t0());
        assert_eq!(e.error_class, ErrorClass::ExternalDependency);
    }

    #[test]
    fn message_derivation_classifies_common_failures() {
        let cases = [
            ("request timed out after 30s", "TIMEOUT", ErrorClass::Timeout),
            ("credential 'billing' is disabled", "CREDENTIAL", ErrorClass::Credential),
            ("circuit breaker open for charge_card", "CIRCUIT_OPEN", ErrorClass::Policy),
            ("budget exceeded: max_steps", "BUDGET_EXCEEDED", ErrorClass::Policy),
            ("rate limit exhausted for mailbox:x", "RATE_LIMITED", ErrorClass::Policy),
            ("unknown template root: bogus", "CONFIGURATION", ErrorClass::Configuration),
            ("cancelled by operator", "CANCELLED", ErrorClass::Cancelled),
            ("dns error: failed to resolve", "TRANSPORT", ErrorClass::ExternalDependency),
        ];
        for (msg, code, class) in cases {
            let e = envelope_from_message(msg, false, t0());
            assert_eq!(e.error_code, code, "{msg}");
            assert_eq!(e.error_class, class, "{msg}");
        }
    }

    #[test]
    fn unrecognized_message_gets_unknown_code() {
        let e = envelope_from_message("widget frobnication failed", false, t0());
        assert_eq!(e.error_code, "unknown");
        assert_eq!(e.error_class, ErrorClass::Application);
        // Unknown code means fingerprint falls back to normalized message —
        // two different unknown messages must group separately.
        let e2 = envelope_from_message("database exploded", false, t0());
        assert_ne!(
            fingerprint(&scope(), &e).hash,
            fingerprint(&scope(), &e2).hash
        );
    }

    #[test]
    fn http_status_extraction_ignores_non_status_numbers() {
        // "returned 12345 rows" — not a 3-digit status.
        let e = envelope_from_message("returned 12345 rows then failed", false, t0());
        assert_eq!(e.error_code, "unknown");
        // Volatile row counts must not split groups either.
        let e2 = envelope_from_message("returned 99999 rows then failed", false, t0());
        assert_eq!(
            fingerprint(&scope(), &e).hash,
            fingerprint(&scope(), &e2).hash
        );
    }

    // --- fnv1a stability ---

    #[test]
    fn fnv1a_is_stable() {
        // Known FNV-1a 64 test vectors — these must never change, the
        // hashes are persisted.
        assert_eq!(fnv1a(b""), 0xcbf2_9ce4_8422_2325);
        assert_eq!(fnv1a(b"a"), 0xaf63_dc4c_8601_ec8c);
    }
}
