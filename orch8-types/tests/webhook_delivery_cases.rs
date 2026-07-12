//! Extensive unit tests for the webhook delivery inspector types:
//! `classify_error`, `DeliveryErrorClass`, `WebhookDeliveryAttempt`
//! (including `set_error` excerpt bounding), `WebhookDeliverySummary`,
//! `DeliveryFilter`, and the `WebhookOutboxEntry::delivery_id` linkage.

use chrono::{TimeZone, Utc};
use orch8_types::webhook_delivery::{
    DeliveryErrorClass, DeliveryFilter, MAX_ERROR_EXCERPT_LEN, WebhookDeliveryAttempt,
    WebhookDeliverySummary, classify_error,
};
use orch8_types::webhook_outbox::WebhookOutboxEntry;
use uuid::Uuid;

const ALL_CLASSES: [DeliveryErrorClass; 12] = [
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
];

/// One `#[test]` per (error string, status, expected class) case.
macro_rules! classify_case {
    ($name:ident, $err:expr, $status:expr, $class:ident) => {
        #[test]
        fn $name() {
            let status: Option<u16> = $status;
            assert_eq!(
                classify_error($err, status),
                DeliveryErrorClass::$class,
                "classify_error({:?}, {:?})",
                $err,
                status
            );
        }
    };
}

// ---------------------------------------------------------------------------
// classify_error: DNS
// ---------------------------------------------------------------------------

classify_case!(
    classify_dns_reqwest_lookup,
    "error sending request for url (https://x.example/hook): dns error: failed to lookup address information: Name or service not known",
    None,
    Dns
);
classify_case!(
    classify_dns_no_record,
    "dns error: no record found for Query { name: Name(\"missing.example.\") }",
    None,
    Dns
);
classify_case!(classify_dns_resolve_host, "failed to resolve host", None, Dns);
classify_case!(
    classify_dns_curl_style_resolve,
    "curl: (6) could not resolve host: example.com",
    None,
    Dns
);
classify_case!(
    classify_dns_wins_over_connect,
    "error trying to connect: dns error: failed to lookup",
    None,
    Dns
);
classify_case!(classify_dns_uppercase, "DNS LOOKUP FAILED", None, Dns);
classify_case!(classify_dns_mixed_case_resolve, "cannot Resolve address", None, Dns);
classify_case!(classify_dns_dnssec_embedded, "dnssec validation failed", None, Dns);
// "resolve" embedded in a larger word still matches (substring matching).
classify_case!(classify_dns_unresolved_embedded, "unresolved symbol in handler", None, Dns);
classify_case!(classify_dns_resolved_embedded, "resolved the promise twice", None, Dns);
// "resolution" does NOT contain "resolve" (no 'v') — conservative fallthrough.
classify_case!(
    classify_dns_name_resolution_is_other,
    "temporary failure in name resolution",
    None,
    Other
);

// ---------------------------------------------------------------------------
// classify_error: Connect
// ---------------------------------------------------------------------------

classify_case!(classify_connect_refused, "connection refused", None, Connect);
classify_case!(
    classify_connect_reqwest_tcp_refused,
    "error trying to connect: tcp connect error: Connection refused (os error 61)",
    None,
    Connect
);
classify_case!(classify_connect_reset_by_peer, "connection reset by peer", None, Connect);
classify_case!(classify_connect_aborted_no_shutdown, "connection aborted", None, Connect);
classify_case!(classify_connect_broken_pipe, "broken pipe", None, Connect);
classify_case!(classify_connect_broken_pipe_os_error, "os error 32: Broken pipe", None, Connect);
classify_case!(classify_connect_uppercase, "CONNECTION RESET", None, Connect);
classify_case!(classify_connect_failed_to_connect, "failed to connect to host", None, Connect);
classify_case!(
    classify_connect_closed_before_complete,
    "connection closed before message completed",
    None,
    Connect
);
// "connect" embedded inside bigger words still matches.
classify_case!(classify_connect_reconnect_embedded, "unable to reconnect", None, Connect);
classify_case!(classify_connect_disconnected_embedded, "disconnected from server", None, Connect);
// "refused" alone (without "connect"/"connection refused") is NOT enough.
classify_case!(classify_connect_refused_alone_is_other, "https://example.com refused", None, Other);

// ---------------------------------------------------------------------------
// classify_error: TLS
// ---------------------------------------------------------------------------

classify_case!(classify_tls_cert_expired, "invalid peer certificate: Expired", None, Tls);
classify_case!(
    classify_tls_cert_unknown_issuer,
    "invalid peer certificate: UnknownIssuer",
    None,
    Tls
);
classify_case!(classify_tls_handshake_eof, "tls handshake eof", None, Tls);
classify_case!(
    classify_tls_openssl_alert,
    "error:0A000410:SSL routines:ssl3_read_bytes:sslv3 alert handshake failure",
    None,
    Tls
);
classify_case!(classify_tls_cert_verify_failed, "certificate verify failed", None, Tls);
classify_case!(classify_tls_uppercase, "TLS ERROR", None, Tls);
classify_case!(classify_tls_handshake_alone, "handshake failure", None, Tls);
classify_case!(classify_tls_self_signed_cert, "self-signed certificate in chain", None, Tls);
classify_case!(classify_tls_hostname_mismatch, "hostname mismatch in certificate", None, Tls);
classify_case!(
    classify_tls_close_notify_beats_connect,
    "peer closed connection without sending TLS close_notify",
    None,
    Tls
);
// "handshake timed out" — TLS branch is checked before Timeout.
classify_case!(classify_tls_handshake_timed_out, "handshake timed out", None, Tls);
classify_case!(classify_tls_timed_out, "tls timed out", None, Tls);
// "ssl" hidden inside ordinary words still matches (documenting actual behavior).
classify_case!(classify_tls_ssl_embedded_crossly, "peer answered crossly", None, Tls);
classify_case!(classify_tls_ssl_embedded_misslabeled, "misslabeled endpoint", None, Tls);

// ---------------------------------------------------------------------------
// classify_error: Timeout
// ---------------------------------------------------------------------------

classify_case!(classify_timeout_operation_timed_out, "operation timed out", None, Timeout);
classify_case!(
    classify_timeout_reqwest_sending,
    "error sending request for url (https://slow.example/hook): operation timed out",
    None,
    Timeout
);
classify_case!(classify_timeout_request_timeout, "request timeout", None, Timeout);
classify_case!(classify_timeout_waiting, "timeout waiting for response", None, Timeout);
classify_case!(classify_timeout_uppercase, "TIMED OUT", None, Timeout);
// Timeout branch precedes Connect: "connect timeout" is a Timeout.
classify_case!(classify_timeout_beats_connect, "connect timeout", None, Timeout);
classify_case!(classify_timeout_during_connect, "timeout during connect", None, Timeout);
// "deadline has elapsed" carries no recognized keyword — conservative Other.
classify_case!(classify_timeout_deadline_is_other, "deadline has elapsed", None, Other);

// ---------------------------------------------------------------------------
// classify_error: RedirectRejected
// ---------------------------------------------------------------------------

classify_case!(
    classify_redirect_private_target,
    "blocked: redirect targets a private/internal network address",
    None,
    RedirectRejected
);
classify_case!(classify_redirect_too_many, "too many redirects", None, RedirectRejected);
classify_case!(classify_redirect_loop, "redirect loop detected", None, RedirectRejected);
classify_case!(
    classify_redirect_uppercase,
    "REDIRECT to disallowed scheme",
    None,
    RedirectRejected
);
// Redirect branch precedes DNS and Timeout.
classify_case!(
    classify_redirect_beats_dns,
    "redirect to unresolvable host: dns error",
    None,
    RedirectRejected
);
classify_case!(classify_redirect_timed_out, "redirect timed out", None, RedirectRejected);

// ---------------------------------------------------------------------------
// classify_error: Serialization
// ---------------------------------------------------------------------------

classify_case!(
    classify_serialize_key_must_be_string,
    "serialize: key must be a string",
    None,
    Serialization
);
classify_case!(
    classify_serialize_failed_payload,
    "failed to serialize payload",
    None,
    Serialization
);
classify_case!(classify_serialize_uppercase, "could not SERIALIZE json", None, Serialization);
// "serialized" contains "serialize" — still matches.
classify_case!(
    classify_serialize_embedded_past_tense,
    "payload already serialized incorrectly",
    None,
    Serialization
);
// Serialize branch precedes Redirect and Timeout.
classify_case!(
    classify_serialize_beats_redirect,
    "serialize error after redirect",
    None,
    Serialization
);
classify_case!(
    classify_serialize_beats_timeout,
    "serialize failed due to timeout",
    None,
    Serialization
);

// ---------------------------------------------------------------------------
// classify_error: Aborted
// ---------------------------------------------------------------------------

classify_case!(classify_aborted_exact, "aborted by shutdown", None, Aborted);
classify_case!(
    classify_aborted_with_context,
    "delivery aborted by shutdown signal",
    None,
    Aborted
);
classify_case!(classify_aborted_uppercase, "ABORTED BY SHUTDOWN", None, Aborted);
// Aborted is the very first string branch — beats everything else.
classify_case!(
    classify_aborted_beats_connect,
    "aborted by shutdown: connection reset",
    None,
    Aborted
);
classify_case!(
    classify_aborted_beats_dns,
    "request aborted by shutdown while resolving dns",
    None,
    Aborted
);
// Bare "aborted" (without "by shutdown") is not enough for Aborted.
classify_case!(classify_aborted_bare_is_other, "aborted", None, Other);

// ---------------------------------------------------------------------------
// classify_error: "http NNN" strings
// ---------------------------------------------------------------------------

classify_case!(classify_http_503, "http 503", None, HttpStatus);
classify_case!(classify_http_404_text, "http 404 not found", None, HttpStatus);
classify_case!(classify_http_uppercase, "HTTP 500", None, HttpStatus);
classify_case!(classify_http_mixed_case, "HtTp 502", None, HttpStatus);
// Only a *prefix* "http " counts.
classify_case!(classify_http_leading_space_is_other, " http 503", None, Other);
classify_case!(classify_http_midstring_is_other, "got http 200 response", None, Other);
classify_case!(
    classify_http_error_prefix_is_other,
    "error: http 502 bad gateway",
    None,
    Other
);
// "https://" has no space after "http" — no match anywhere.
classify_case!(classify_https_url_is_other, "https://example.com unreachable", None, Other);

// ---------------------------------------------------------------------------
// classify_error: ResponseTooLarge
// ---------------------------------------------------------------------------

classify_case!(
    classify_too_large_response_body,
    "the response body was too large",
    None,
    ResponseTooLarge
);
classify_case!(
    classify_too_large_exceeded,
    "body exceeded max size: too large",
    None,
    ResponseTooLarge
);
classify_case!(classify_too_large_any_order, "large body received", None, ResponseTooLarge);
classify_case!(classify_too_large_uppercase, "BODY TOO LARGE", None, ResponseTooLarge);
// Needs BOTH "body" and "large".
classify_case!(classify_large_without_body_is_other, "payload too large", None, Other);
classify_case!(classify_body_without_large_is_other, "body exceeded limit", None, Other);

// ---------------------------------------------------------------------------
// classify_error: Other / never-classified strings
// ---------------------------------------------------------------------------

classify_case!(classify_empty_is_other, "", None, Other);
classify_case!(classify_gremlins_is_other, "mysterious gremlins", None, Other);
classify_case!(classify_channel_closed_is_other, "channel closed", None, Other);
classify_case!(classify_scheme_is_other, "invalid URL, scheme is not allowed", None, Other);
// `classify_error` never produces these classes — they are set by policy code.
classify_case!(
    classify_signature_words_are_other,
    "signature configuration invalid",
    None,
    Other
);
classify_case!(classify_policy_words_are_other, "internal policy blocked", None, Other);

// ---------------------------------------------------------------------------
// classify_error: status-code dominance
// ---------------------------------------------------------------------------

classify_case!(classify_status_400_dominates, "connection refused", Some(400), HttpStatus);
classify_case!(classify_status_404_dominates, "dns error", Some(404), HttpStatus);
classify_case!(classify_status_429_dominates, "timed out", Some(429), HttpStatus);
classify_case!(classify_status_499_dominates, "tls handshake eof", Some(499), HttpStatus);
classify_case!(classify_status_500_dominates, "redirect loop", Some(500), HttpStatus);
classify_case!(classify_status_503_dominates, "anything", Some(503), HttpStatus);
classify_case!(classify_status_599_dominates, "broken pipe", Some(599), HttpStatus);
classify_case!(classify_status_max_u16_dominates, "gremlins", Some(u16::MAX), HttpStatus);
classify_case!(classify_status_400_with_empty_error, "", Some(400), HttpStatus);
classify_case!(
    classify_status_dominates_aborted_by_shutdown,
    "aborted by shutdown",
    Some(500),
    HttpStatus
);
// Statuses below 400 do NOT force HttpStatus — the message decides.
classify_case!(classify_status_0_falls_through, "connection refused", Some(0), Connect);
classify_case!(classify_status_100_falls_through, "operation timed out", Some(100), Timeout);
classify_case!(classify_status_200_falls_through, "connection reset", Some(200), Connect);
classify_case!(classify_status_301_falls_through, "dns error", Some(301), Dns);
classify_case!(classify_status_399_falls_through, "connection refused", Some(399), Connect);
classify_case!(classify_status_200_unknown_error_is_other, "gremlins", Some(200), Other);

// ---------------------------------------------------------------------------
// DeliveryErrorClass: as_str / serde parity
// ---------------------------------------------------------------------------

macro_rules! class_parity {
    ($name:ident, $class:ident, $label:expr) => {
        #[test]
        fn $name() {
            let class = DeliveryErrorClass::$class;
            assert_eq!(class.as_str(), $label);
            // Serialize matches as_str.
            assert_eq!(serde_json::to_string(&class).unwrap(), format!("\"{}\"", $label));
            // Deserialize from the label round-trips.
            let back: DeliveryErrorClass =
                serde_json::from_str(&format!("\"{}\"", $label)).unwrap();
            assert_eq!(back, class);
        }
    };
}

class_parity!(parity_dns, Dns, "dns");
class_parity!(parity_connect, Connect, "connect");
class_parity!(parity_tls, Tls, "tls");
class_parity!(parity_timeout, Timeout, "timeout");
class_parity!(parity_redirect_rejected, RedirectRejected, "redirect_rejected");
class_parity!(parity_http_status, HttpStatus, "http_status");
class_parity!(
    parity_signature_configuration,
    SignatureConfiguration,
    "signature_configuration"
);
class_parity!(parity_response_too_large, ResponseTooLarge, "response_too_large");
class_parity!(parity_internal_policy, InternalPolicy, "internal_policy");
class_parity!(parity_serialization, Serialization, "serialization");
class_parity!(parity_aborted, Aborted, "aborted");
class_parity!(parity_other, Other, "other");

#[test]
fn class_labels_are_unique() {
    let mut labels: Vec<&str> = ALL_CLASSES.iter().map(|c| c.as_str()).collect();
    labels.sort_unstable();
    labels.dedup();
    assert_eq!(labels.len(), ALL_CLASSES.len());
}

#[test]
fn class_deserialize_rejects_unknown_label() {
    assert!(serde_json::from_str::<DeliveryErrorClass>("\"gremlins\"").is_err());
}

#[test]
fn class_deserialize_rejects_pascal_case() {
    assert!(serde_json::from_str::<DeliveryErrorClass>("\"Dns\"").is_err());
    assert!(serde_json::from_str::<DeliveryErrorClass>("\"HttpStatus\"").is_err());
}

#[test]
fn class_deserialize_rejects_upper_case() {
    assert!(serde_json::from_str::<DeliveryErrorClass>("\"DNS\"").is_err());
    assert!(serde_json::from_str::<DeliveryErrorClass>("\"TIMEOUT\"").is_err());
}

#[test]
fn class_deserialize_rejects_kebab_case() {
    assert!(serde_json::from_str::<DeliveryErrorClass>("\"redirect-rejected\"").is_err());
    assert!(serde_json::from_str::<DeliveryErrorClass>("\"response-too-large\"").is_err());
}

#[test]
fn class_round_trips_through_json_value() {
    for class in ALL_CLASSES {
        let value = serde_json::to_value(class).unwrap();
        assert_eq!(value, serde_json::Value::String(class.as_str().to_string()));
        let back: DeliveryErrorClass = serde_json::from_value(value).unwrap();
        assert_eq!(back, class);
    }
}

// ---------------------------------------------------------------------------
// set_error: excerpt bounding
// ---------------------------------------------------------------------------

fn base_attempt() -> WebhookDeliveryAttempt {
    WebhookDeliveryAttempt {
        id: Uuid::now_v7(),
        delivery_id: Uuid::now_v7(),
        url: "https://example.com/hook".into(),
        event_type: "instance.failed".into(),
        instance_id: None,
        attempt_number: 1,
        attempted_at: Utc.with_ymd_and_hms(2026, 7, 1, 12, 0, 0).unwrap(),
        duration_ms: 42,
        success: false,
        status_code: None,
        error_class: None,
        error_excerpt: None,
        signed: false,
    }
}

const ELLIPSIS: char = '…';

#[test]
fn max_error_excerpt_len_is_512() {
    assert_eq!(MAX_ERROR_EXCERPT_LEN, 512);
}

#[test]
fn set_error_keeps_exactly_max_len_untouched() {
    let mut a = base_attempt();
    let input = "x".repeat(MAX_ERROR_EXCERPT_LEN);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert_eq!(excerpt, input);
    assert_eq!(excerpt.len(), MAX_ERROR_EXCERPT_LEN);
    assert!(!excerpt.ends_with(ELLIPSIS));
}

#[test]
fn set_error_keeps_max_minus_one_untouched() {
    let mut a = base_attempt();
    let input = "y".repeat(MAX_ERROR_EXCERPT_LEN - 1);
    a.set_error(&input, None);
    assert_eq!(a.error_excerpt.as_deref(), Some(input.as_str()));
}

#[test]
fn set_error_truncates_max_plus_one() {
    let mut a = base_attempt();
    let input = "z".repeat(MAX_ERROR_EXCERPT_LEN + 1);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert!(excerpt.ends_with(ELLIPSIS));
    // 512 kept bytes + 3-byte ellipsis.
    assert_eq!(excerpt.len(), MAX_ERROR_EXCERPT_LEN + ELLIPSIS.len_utf8());
    assert_eq!(excerpt.strip_suffix(ELLIPSIS).unwrap(), &input[..MAX_ERROR_EXCERPT_LEN]);
}

#[test]
fn set_error_truncates_huge_input() {
    let mut a = base_attempt();
    a.set_error(&"e".repeat(100_000), None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert_eq!(excerpt.len(), MAX_ERROR_EXCERPT_LEN + ELLIPSIS.len_utf8());
    assert!(excerpt.ends_with(ELLIPSIS));
}

#[test]
fn set_error_empty_string() {
    let mut a = base_attempt();
    a.set_error("", None);
    assert_eq!(a.error_excerpt.as_deref(), Some(""));
    assert_eq!(a.error_class, Some(DeliveryErrorClass::Other));
}

#[test]
fn set_error_two_byte_chars_exactly_at_limit_untouched() {
    // 'é' is 2 bytes; 256 of them are exactly 512 bytes.
    let mut a = base_attempt();
    let input = "é".repeat(256);
    assert_eq!(input.len(), MAX_ERROR_EXCERPT_LEN);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert_eq!(excerpt, input);
    assert!(!excerpt.ends_with(ELLIPSIS));
}

#[test]
fn set_error_two_byte_chars_on_boundary_cut() {
    // 257 'é' = 514 bytes; byte 512 is a char boundary, so 256 chars survive.
    let mut a = base_attempt();
    let input = "é".repeat(257);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert_eq!(excerpt.len(), MAX_ERROR_EXCERPT_LEN + ELLIPSIS.len_utf8());
    assert!(excerpt.ends_with(ELLIPSIS));
    assert_eq!(excerpt.chars().filter(|c| *c == 'é').count(), 256);
}

#[test]
fn set_error_two_byte_chars_straddling_cut() {
    // "a" + 256 'é' = 513 bytes; byte 512 splits an 'é', so the cut backs
    // up to byte 511.
    let mut a = base_attempt();
    let input = format!("a{}", "é".repeat(256));
    assert_eq!(input.len(), MAX_ERROR_EXCERPT_LEN + 1);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert!(excerpt.ends_with(ELLIPSIS));
    assert_eq!(excerpt.len(), 511 + ELLIPSIS.len_utf8());
    assert_eq!(excerpt.strip_suffix(ELLIPSIS).unwrap(), &input[..511]);
    assert_eq!(excerpt.chars().filter(|c| *c == 'é').count(), 255);
}

#[test]
fn set_error_three_byte_chars_straddling_cut() {
    // '€' is 3 bytes; 171 of them = 513 bytes. Boundaries at multiples of
    // 3, so 512 → 511 → 510 (170 chars kept).
    let mut a = base_attempt();
    let input = "€".repeat(171);
    assert_eq!(input.len(), 513);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert!(excerpt.ends_with(ELLIPSIS));
    assert_eq!(excerpt.len(), 510 + ELLIPSIS.len_utf8());
    assert_eq!(excerpt.chars().filter(|c| *c == '€').count(), 170);
}

#[test]
fn set_error_four_byte_chars_on_boundary_cut() {
    // '🦀' is 4 bytes; 129 of them = 516 bytes. Byte 512 is a boundary, so
    // 128 crabs survive.
    let mut a = base_attempt();
    let input = "🦀".repeat(129);
    assert_eq!(input.len(), 516);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert!(excerpt.ends_with(ELLIPSIS));
    assert_eq!(excerpt.len(), MAX_ERROR_EXCERPT_LEN + ELLIPSIS.len_utf8());
    assert_eq!(excerpt.chars().filter(|c| *c == '🦀').count(), 128);
}

#[test]
fn set_error_four_byte_chars_straddling_cut() {
    // "ab" + 128 '🦀' = 514 bytes. Boundaries at 2 + 4k, so 512 → 510
    // (127 crabs kept).
    let mut a = base_attempt();
    let input = format!("ab{}", "🦀".repeat(128));
    assert_eq!(input.len(), 514);
    a.set_error(&input, None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert!(excerpt.ends_with(ELLIPSIS));
    assert_eq!(excerpt.len(), 510 + ELLIPSIS.len_utf8());
    assert!(excerpt.starts_with("ab"));
    assert_eq!(excerpt.chars().filter(|c| *c == '🦀').count(), 127);
}

#[test]
fn set_error_mixed_multibyte_ending_exactly_at_limit() {
    // 170 '€' (510 bytes) + "aa" = exactly 512 bytes — untouched.
    let mut a = base_attempt();
    let input = format!("{}aa", "€".repeat(170));
    assert_eq!(input.len(), MAX_ERROR_EXCERPT_LEN);
    a.set_error(&input, None);
    assert_eq!(a.error_excerpt.as_deref(), Some(input.as_str()));
}

#[test]
fn set_error_bound_invariant_over_lengths_around_max() {
    // Sweep lengths MAX-2 ..= MAX+8 with a 2-byte char to hit every
    // boundary alignment. Result must always be valid UTF-8 (guaranteed by
    // String) and within MAX + ellipsis bytes.
    for extra in 0..=10 {
        let bytes = MAX_ERROR_EXCERPT_LEN - 2 + extra;
        let input: String = "é".repeat(bytes / 2) + &"x".repeat(bytes % 2);
        assert_eq!(input.len(), bytes);
        let mut a = base_attempt();
        a.set_error(&input, None);
        let excerpt = a.error_excerpt.as_deref().unwrap();
        assert!(
            excerpt.len() <= MAX_ERROR_EXCERPT_LEN + ELLIPSIS.len_utf8(),
            "len {} for input of {bytes} bytes",
            excerpt.len()
        );
        if bytes <= MAX_ERROR_EXCERPT_LEN {
            assert_eq!(excerpt, input);
        } else {
            assert!(excerpt.ends_with(ELLIPSIS));
        }
    }
}

#[test]
fn set_error_classifies_like_classify_error() {
    for (msg, status) in [
        ("connection refused", None),
        ("dns error", None),
        ("tls handshake eof", None),
        ("operation timed out", None),
        ("anything", Some(503_u16)),
        ("gremlins", None),
    ] {
        let mut a = base_attempt();
        a.set_error(msg, status);
        assert_eq!(a.error_class, Some(classify_error(msg, status)));
    }
}

#[test]
fn set_error_with_status_sets_http_status_class() {
    let mut a = base_attempt();
    a.set_error("http 503", Some(503));
    assert_eq!(a.error_class, Some(DeliveryErrorClass::HttpStatus));
    assert_eq!(a.error_excerpt.as_deref(), Some("http 503"));
}

#[test]
fn set_error_overwrites_previous_error() {
    let mut a = base_attempt();
    a.set_error("connection refused", None);
    assert_eq!(a.error_class, Some(DeliveryErrorClass::Connect));
    a.set_error("operation timed out", None);
    assert_eq!(a.error_class, Some(DeliveryErrorClass::Timeout));
    assert_eq!(a.error_excerpt.as_deref(), Some("operation timed out"));
}

#[test]
fn set_error_ellipsis_is_last_char_only_when_truncated() {
    let mut a = base_attempt();
    a.set_error("short error", None);
    assert!(!a.error_excerpt.as_deref().unwrap().contains(ELLIPSIS));
    a.set_error(&"q".repeat(600), None);
    let excerpt = a.error_excerpt.as_deref().unwrap();
    assert_eq!(excerpt.chars().last(), Some(ELLIPSIS));
    assert_eq!(excerpt.chars().filter(|c| *c == ELLIPSIS).count(), 1);
}

#[test]
fn set_error_preserves_preexisting_ellipsis_in_short_input() {
    let mut a = base_attempt();
    a.set_error("already…elided", None);
    assert_eq!(a.error_excerpt.as_deref(), Some("already…elided"));
}

// ---------------------------------------------------------------------------
// WebhookDeliveryAttempt: serde
// ---------------------------------------------------------------------------

fn full_attempt() -> WebhookDeliveryAttempt {
    WebhookDeliveryAttempt {
        id: Uuid::now_v7(),
        delivery_id: Uuid::now_v7(),
        url: "https://hooks.example.com/full".into(),
        event_type: "instance.completed".into(),
        instance_id: Some(Uuid::now_v7()),
        attempt_number: 3,
        attempted_at: Utc.with_ymd_and_hms(2026, 7, 2, 8, 30, 45).unwrap(),
        duration_ms: 1234,
        success: false,
        status_code: Some(503),
        error_class: Some(DeliveryErrorClass::HttpStatus),
        error_excerpt: Some("http 503".into()),
        signed: true,
    }
}

#[test]
fn attempt_round_trips_with_all_optionals_absent() {
    let a = base_attempt();
    let json = serde_json::to_string(&a).unwrap();
    let back: WebhookDeliveryAttempt = serde_json::from_str(&json).unwrap();
    assert_eq!(back, a);
}

#[test]
fn attempt_round_trips_with_all_optionals_present() {
    let a = full_attempt();
    let json = serde_json::to_string(&a).unwrap();
    let back: WebhookDeliveryAttempt = serde_json::from_str(&json).unwrap();
    assert_eq!(back, a);
}

#[test]
fn attempt_omits_absent_optionals_in_json() {
    let value = serde_json::to_value(base_attempt()).unwrap();
    let obj = value.as_object().unwrap();
    for key in ["instance_id", "status_code", "error_class", "error_excerpt"] {
        assert!(!obj.contains_key(key), "{key} must be omitted when None");
    }
}

#[test]
fn attempt_includes_present_optionals_in_json() {
    let a = full_attempt();
    let value = serde_json::to_value(&a).unwrap();
    assert_eq!(value["instance_id"], a.instance_id.unwrap().to_string().as_str());
    assert_eq!(value["status_code"], 503);
    assert_eq!(value["error_class"], "http_status");
    assert_eq!(value["error_excerpt"], "http 503");
    assert_eq!(value["signed"], true);
    assert_eq!(value["attempt_number"], 3);
    assert_eq!(value["duration_ms"], 1234);
}

#[test]
fn attempt_deserializes_when_optional_fields_missing() {
    let json = serde_json::json!({
        "id": Uuid::now_v7(),
        "delivery_id": Uuid::now_v7(),
        "url": "https://x.example/hook",
        "event_type": "instance.failed",
        "attempt_number": 1,
        "attempted_at": "2026-07-01T00:00:00Z",
        "duration_ms": 5,
        "success": true,
        "signed": false
    });
    let a: WebhookDeliveryAttempt = serde_json::from_value(json).unwrap();
    assert_eq!(a.instance_id, None);
    assert_eq!(a.status_code, None);
    assert_eq!(a.error_class, None);
    assert_eq!(a.error_excerpt, None);
}

#[test]
fn attempt_deserialize_ignores_unknown_fields() {
    let mut value = serde_json::to_value(full_attempt()).unwrap();
    value["surprise"] = serde_json::json!("extra");
    assert!(serde_json::from_value::<WebhookDeliveryAttempt>(value).is_ok());
}

#[test]
fn attempt_deserialize_fails_without_url() {
    let mut value = serde_json::to_value(full_attempt()).unwrap();
    value.as_object_mut().unwrap().remove("url");
    assert!(serde_json::from_value::<WebhookDeliveryAttempt>(value).is_err());
}

#[test]
fn attempt_deserialize_fails_without_delivery_id() {
    let mut value = serde_json::to_value(full_attempt()).unwrap();
    value.as_object_mut().unwrap().remove("delivery_id");
    assert!(serde_json::from_value::<WebhookDeliveryAttempt>(value).is_err());
}

#[test]
fn attempt_deserialize_fails_with_bad_error_class() {
    let mut value = serde_json::to_value(full_attempt()).unwrap();
    value["error_class"] = serde_json::json!("gremlins");
    assert!(serde_json::from_value::<WebhookDeliveryAttempt>(value).is_err());
}

#[test]
fn attempt_round_trips_extreme_numbers() {
    let mut a = base_attempt();
    a.attempt_number = i32::MAX;
    a.duration_ms = i64::MAX;
    let back: WebhookDeliveryAttempt =
        serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
    assert_eq!(back, a);
}

#[test]
fn attempt_round_trips_zero_and_negative_numbers() {
    let mut a = base_attempt();
    a.attempt_number = 0;
    a.duration_ms = -1;
    let back: WebhookDeliveryAttempt =
        serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
    assert_eq!(back, a);
}

#[test]
fn attempt_timestamp_round_trips_rfc3339() {
    let a = full_attempt();
    let value = serde_json::to_value(&a).unwrap();
    let raw = value["attempted_at"].as_str().unwrap();
    assert!(raw.starts_with("2026-07-02T08:30:45"));
    let back: WebhookDeliveryAttempt = serde_json::from_value(value).unwrap();
    assert_eq!(back.attempted_at, a.attempted_at);
}

#[test]
fn attempt_round_trips_after_set_error_truncation() {
    let mut a = base_attempt();
    a.set_error(&format!("é{}", "x".repeat(1000)), None);
    let back: WebhookDeliveryAttempt =
        serde_json::from_str(&serde_json::to_string(&a).unwrap()).unwrap();
    assert_eq!(back, a);
}

#[test]
fn attempt_round_trips_every_error_class() {
    for class in ALL_CLASSES {
        let mut a = base_attempt();
        a.error_class = Some(class);
        let value = serde_json::to_value(&a).unwrap();
        assert_eq!(value["error_class"], class.as_str());
        let back: WebhookDeliveryAttempt = serde_json::from_value(value).unwrap();
        assert_eq!(back.error_class, Some(class));
    }
}

// ---------------------------------------------------------------------------
// WebhookDeliverySummary: serde
// ---------------------------------------------------------------------------

fn base_summary() -> WebhookDeliverySummary {
    WebhookDeliverySummary {
        delivery_id: Uuid::now_v7(),
        url: "https://hooks.example.com/sum".into(),
        event_type: "instance.failed".into(),
        instance_id: None,
        attempts: 4,
        delivered: false,
        first_attempt_at: Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap(),
        last_attempt_at: Utc.with_ymd_and_hms(2026, 7, 1, 0, 5, 0).unwrap(),
        last_error_class: Some(DeliveryErrorClass::Timeout),
    }
}

#[test]
fn summary_round_trips_with_optionals_present() {
    let mut s = base_summary();
    s.instance_id = Some(Uuid::now_v7());
    let back: WebhookDeliverySummary =
        serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
    assert_eq!(back, s);
}

#[test]
fn summary_round_trips_with_optionals_absent() {
    let mut s = base_summary();
    s.last_error_class = None;
    s.delivered = true;
    let back: WebhookDeliverySummary =
        serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
    assert_eq!(back, s);
}

#[test]
fn summary_omits_absent_optionals_in_json() {
    let mut s = base_summary();
    s.last_error_class = None;
    let value = serde_json::to_value(&s).unwrap();
    let obj = value.as_object().unwrap();
    assert!(!obj.contains_key("instance_id"));
    assert!(!obj.contains_key("last_error_class"));
}

#[test]
fn summary_serializes_expected_fields() {
    let s = base_summary();
    let value = serde_json::to_value(&s).unwrap();
    assert_eq!(value["delivery_id"], s.delivery_id.to_string().as_str());
    assert_eq!(value["url"], "https://hooks.example.com/sum");
    assert_eq!(value["event_type"], "instance.failed");
    assert_eq!(value["attempts"], 4);
    assert_eq!(value["delivered"], false);
    assert_eq!(value["last_error_class"], "timeout");
}

#[test]
fn summary_deserializes_when_optional_fields_missing() {
    let json = serde_json::json!({
        "delivery_id": Uuid::now_v7(),
        "url": "https://x.example/hook",
        "event_type": "instance.failed",
        "attempts": 1,
        "delivered": true,
        "first_attempt_at": "2026-07-01T00:00:00Z",
        "last_attempt_at": "2026-07-01T00:00:01Z"
    });
    let s: WebhookDeliverySummary = serde_json::from_value(json).unwrap();
    assert_eq!(s.instance_id, None);
    assert_eq!(s.last_error_class, None);
}

#[test]
fn summary_deserialize_fails_without_delivered() {
    let mut value = serde_json::to_value(base_summary()).unwrap();
    value.as_object_mut().unwrap().remove("delivered");
    assert!(serde_json::from_value::<WebhookDeliverySummary>(value).is_err());
}

#[test]
fn summary_round_trips_every_error_class() {
    for class in ALL_CLASSES {
        let mut s = base_summary();
        s.last_error_class = Some(class);
        let back: WebhookDeliverySummary =
            serde_json::from_str(&serde_json::to_string(&s).unwrap()).unwrap();
        assert_eq!(back.last_error_class, Some(class));
    }
}

#[test]
fn summary_has_no_payload_or_excerpt_fields() {
    let value = serde_json::to_value(base_summary()).unwrap();
    let obj = value.as_object().unwrap();
    assert!(!obj.contains_key("payload"));
    assert!(!obj.contains_key("error_excerpt"));
}

// ---------------------------------------------------------------------------
// DeliveryFilter
// ---------------------------------------------------------------------------

#[test]
fn filter_default_is_all_none() {
    let f = DeliveryFilter::default();
    assert_eq!(f.url, None);
    assert_eq!(f.event_type, None);
    assert_eq!(f.delivered, None);
    assert_eq!(f.error_class, None);
}

#[test]
fn filter_holds_values() {
    let f = DeliveryFilter {
        url: Some("https://x.example".into()),
        event_type: Some("instance.failed".into()),
        delivered: Some(false),
        error_class: Some(DeliveryErrorClass::Tls),
    };
    assert_eq!(f.url.as_deref(), Some("https://x.example"));
    assert_eq!(f.delivered, Some(false));
    assert_eq!(f.error_class, Some(DeliveryErrorClass::Tls));
}

#[test]
fn filter_clone_preserves_fields() {
    let f = DeliveryFilter {
        url: Some("u".into()),
        event_type: None,
        delivered: Some(true),
        error_class: Some(DeliveryErrorClass::Aborted),
    };
    let g = f.clone();
    assert_eq!(g.url, f.url);
    assert_eq!(g.event_type, f.event_type);
    assert_eq!(g.delivered, f.delivered);
    assert_eq!(g.error_class, f.error_class);
}

#[test]
fn filter_debug_is_nonempty() {
    assert!(!format!("{:?}", DeliveryFilter::default()).is_empty());
}

// ---------------------------------------------------------------------------
// WebhookOutboxEntry: delivery_id linkage serde
// ---------------------------------------------------------------------------

fn outbox_entry(delivery_id: Option<Uuid>) -> WebhookOutboxEntry {
    WebhookOutboxEntry {
        id: Uuid::now_v7(),
        url: "https://hooks.example.com/parked".into(),
        event_type: "instance.failed".into(),
        instance_id: Some(Uuid::now_v7()),
        payload: serde_json::json!({"event_type": "instance.failed", "data": {"k": "v"}}),
        attempts: 4,
        last_error: Some("http 503".into()),
        created_at: Utc.with_ymd_and_hms(2026, 7, 3, 9, 0, 0).unwrap(),
        delivery_id,
    }
}

#[test]
fn outbox_serializes_delivery_id_when_present() {
    let delivery = Uuid::now_v7();
    let value = serde_json::to_value(outbox_entry(Some(delivery))).unwrap();
    assert_eq!(value["delivery_id"], delivery.to_string().as_str());
}

#[test]
fn outbox_omits_delivery_id_when_none() {
    let value = serde_json::to_value(outbox_entry(None)).unwrap();
    assert!(!value.as_object().unwrap().contains_key("delivery_id"));
}

#[test]
fn outbox_deserializes_missing_delivery_id_to_none() {
    // Rows parked before attempt tracking existed have no delivery_id key.
    let json = serde_json::json!({
        "id": Uuid::now_v7(),
        "url": "https://old.example/hook",
        "event_type": "instance.failed",
        "payload": {"data": {}},
        "attempts": 2,
        "created_at": "2026-01-01T00:00:00Z"
    });
    let entry: WebhookOutboxEntry = serde_json::from_value(json).unwrap();
    assert_eq!(entry.delivery_id, None);
    assert_eq!(entry.instance_id, None);
    assert_eq!(entry.last_error, None);
}

#[test]
fn outbox_deserializes_explicit_null_delivery_id_to_none() {
    let mut value = serde_json::to_value(outbox_entry(None)).unwrap();
    value["delivery_id"] = serde_json::Value::Null;
    let entry: WebhookOutboxEntry = serde_json::from_value(value).unwrap();
    assert_eq!(entry.delivery_id, None);
}

#[test]
fn outbox_round_trips_with_delivery_id() {
    let entry = outbox_entry(Some(Uuid::now_v7()));
    let back: WebhookOutboxEntry =
        serde_json::from_str(&serde_json::to_string(&entry).unwrap()).unwrap();
    assert_eq!(back.id, entry.id);
    assert_eq!(back.url, entry.url);
    assert_eq!(back.event_type, entry.event_type);
    assert_eq!(back.instance_id, entry.instance_id);
    assert_eq!(back.payload, entry.payload);
    assert_eq!(back.attempts, entry.attempts);
    assert_eq!(back.last_error, entry.last_error);
    assert_eq!(back.created_at, entry.created_at);
    assert_eq!(back.delivery_id, entry.delivery_id);
}

#[test]
fn outbox_round_trips_without_delivery_id() {
    let entry = outbox_entry(None);
    let back: WebhookOutboxEntry =
        serde_json::from_str(&serde_json::to_string(&entry).unwrap()).unwrap();
    assert_eq!(back.delivery_id, None);
    assert_eq!(back.payload, entry.payload);
}

#[test]
fn outbox_deserialize_rejects_malformed_delivery_id() {
    let mut value = serde_json::to_value(outbox_entry(None)).unwrap();
    value["delivery_id"] = serde_json::json!("not-a-uuid");
    assert!(serde_json::from_value::<WebhookOutboxEntry>(value).is_err());
}

#[test]
fn outbox_payload_survives_verbatim() {
    let mut entry = outbox_entry(Some(Uuid::now_v7()));
    entry.payload = serde_json::json!({
        "nested": {"array": [1, 2, 3], "flag": true, "none": null},
        "unicode": "héllo 🦀"
    });
    let back: WebhookOutboxEntry =
        serde_json::from_str(&serde_json::to_string(&entry).unwrap()).unwrap();
    assert_eq!(back.payload, entry.payload);
}
