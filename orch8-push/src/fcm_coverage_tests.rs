//! FCM provider coverage: backoff schedule, error-body truncation, response
//! classification precedence, wake payload shape, and config gates.
//!
//! Count contract: 29 independently named unit tests.

use reqwest::StatusCode;

use super::*;

macro_rules! retry_backoff_case {
    ($name:ident, $attempt:expr, $millis:expr) => {
        #[test]
        fn $name() {
            assert_eq!(retry_backoff($attempt), Duration::from_millis($millis));
        }
    };
}

retry_backoff_case!(coverage_fcm_001_first_backoff_is_200ms, 0, 200);
retry_backoff_case!(coverage_fcm_002_second_backoff_is_400ms, 1, 400);
retry_backoff_case!(coverage_fcm_003_third_backoff_is_800ms, 2, 800);

#[test]
fn coverage_fcm_004_backoff_saturates_instead_of_overflowing() {
    assert_eq!(retry_backoff(63), Duration::from_millis(u64::MAX));
}

#[test]
fn coverage_fcm_005_error_preview_passes_empty_body_through() {
    assert_eq!(error_preview(""), "");
}

#[test]
fn coverage_fcm_006_error_preview_passes_short_body_through() {
    assert_eq!(error_preview("bad request"), "bad request");
}

#[test]
fn coverage_fcm_007_error_preview_keeps_body_at_exact_limit() {
    let body = "a".repeat(MAX_ERROR_BODY_LEN);
    assert_eq!(error_preview(&body), body);
}

#[test]
fn coverage_fcm_008_error_preview_truncates_oversized_body_with_marker() {
    let body = "a".repeat(MAX_ERROR_BODY_LEN + 1);
    let preview = error_preview(&body);
    assert!(preview.ends_with("… (truncated)"));
    assert!(preview.starts_with(&"a".repeat(MAX_ERROR_BODY_LEN)));
}

#[test]
fn coverage_fcm_009_error_preview_truncates_on_a_char_boundary() {
    // 'é' is two bytes, so a byte-level cut at the limit could split one.
    let body = "é".repeat(MAX_ERROR_BODY_LEN);
    let preview = error_preview(&body);
    assert!(preview.ends_with("… (truncated)"));
    let prefix = preview.strip_suffix("… (truncated)").unwrap();
    assert!(prefix.len() <= MAX_ERROR_BODY_LEN);
    assert!(prefix.chars().all(|c| c == 'é'));
}

#[test]
fn coverage_fcm_010_created_status_is_success() {
    assert_eq!(
        classify_fcm_response(StatusCode::CREATED, ""),
        FcmOutcome::Success
    );
}

#[test]
fn coverage_fcm_011_success_short_circuits_quota_looking_body() {
    // A 2xx must never be misread as retryable even if the body mentions quota.
    let body = r#"{"name":"projects/p/messages/1","status":"RESOURCE_EXHAUSTED"}"#;
    assert_eq!(
        classify_fcm_response(StatusCode::OK, body),
        FcmOutcome::Success
    );
}

#[test]
fn coverage_fcm_012_malformed_json_404_is_permanent() {
    assert_eq!(
        classify_fcm_response(StatusCode::NOT_FOUND, "not json at all"),
        FcmOutcome::Permanent
    );
}

#[test]
fn coverage_fcm_013_unregistered_in_message_string_only_is_permanent() {
    // Only the structured details[].errorCode marks a dead token; the word
    // "UNREGISTERED" elsewhere in the body must not wipe a device.
    let body = r#"{"error":{"code":404,"message":"UNREGISTERED"}}"#;
    assert_eq!(
        classify_fcm_response(StatusCode::NOT_FOUND, body),
        FcmOutcome::Permanent
    );
}

#[test]
fn coverage_fcm_014_unregistered_details_non_array_is_permanent() {
    let body = r#"{"error":{"details":{"errorCode":"UNREGISTERED"}}}"#;
    assert_eq!(
        classify_fcm_response(StatusCode::NOT_FOUND, body),
        FcmOutcome::Permanent
    );
}

#[test]
fn coverage_fcm_015_unregistered_detail_wins_over_retryable_status() {
    // The dead-token signal takes precedence over 429/5xx retry handling.
    let body = r#"{"error":{"code":429,"details":[{"errorCode":"UNREGISTERED"}]}}"#;
    assert_eq!(
        classify_fcm_response(StatusCode::TOO_MANY_REQUESTS, body),
        FcmOutcome::InvalidToken
    );
}

#[test]
fn coverage_fcm_016_bare_410_is_permanent_not_invalid_token() {
    assert_eq!(
        classify_fcm_response(StatusCode::GONE, ""),
        FcmOutcome::Permanent
    );
}

#[test]
fn coverage_fcm_017_bad_gateway_is_retryable() {
    assert_eq!(
        classify_fcm_response(StatusCode::BAD_GATEWAY, "{}"),
        FcmOutcome::Retryable
    );
}

fn signed_metadata() -> SignedWakeMetadata {
    let now = chrono::Utc::now();
    SignedWakeMetadata::sign(
        "tenant-a",
        "device-a",
        "command-a",
        "wake-key-1",
        &ed25519_dalek::SigningKey::from_bytes(&[7_u8; 32]),
        now,
        now + chrono::Duration::minutes(5),
    )
    .unwrap()
}

#[test]
fn coverage_fcm_018_payload_is_data_only_without_notification() {
    let payload = FcmProvider::wake_payload("token-a", None).unwrap();
    assert!(payload["message"].get("notification").is_none());
    assert!(payload["message"].get("data").is_some());
}

#[test]
fn coverage_fcm_019_payload_data_marks_sync_wake_type() {
    let payload = FcmProvider::wake_payload("token-a", None).unwrap();
    assert_eq!(payload["message"]["data"]["type"], "sync");
}

#[test]
fn coverage_fcm_020_payload_uses_normal_android_priority() {
    let payload = FcmProvider::wake_payload("token-a", None).unwrap();
    assert_eq!(payload["message"]["android"]["priority"], "normal");
}

#[test]
fn coverage_fcm_021_payload_embeds_the_device_token() {
    let payload = FcmProvider::wake_payload("token-a", None).unwrap();
    assert_eq!(payload["message"]["token"], "token-a");
}

#[test]
fn coverage_fcm_022_unsigned_payload_omits_signed_metadata() {
    let payload = FcmProvider::wake_payload("token-a", None).unwrap();
    assert!(payload["message"]["data"].get("orch8").is_none());
}

#[test]
fn coverage_fcm_023_signed_payload_embeds_metadata_as_data_string() {
    // FCM data values must be strings; the signed wake rides as JSON text,
    // never as a nested object and never as a notification payload.
    let metadata = signed_metadata();
    let payload = FcmProvider::wake_payload("token-a", Some(&metadata)).unwrap();
    let encoded = payload["message"]["data"]["orch8"]
        .as_str()
        .expect("signed wake must be a data string");
    let decoded: SignedWakeMetadata = serde_json::from_str(encoded).unwrap();
    assert_eq!(decoded, metadata);
    assert!(payload["message"].get("notification").is_none());
}

fn service_account_json(token_uri: &str) -> String {
    format!(
        r#"{{"client_email":"x@p.iam.gserviceaccount.com","private_key":"-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBALRiMLAHnoDX\n-----END RSA PRIVATE KEY-----","token_uri":"{token_uri}"}}"#
    )
}

#[test]
fn coverage_fcm_024_malformed_service_account_json_is_config_error() {
    let config = FcmConfig {
        project_id: "p".into(),
        service_account_json: "{not json".into(),
    };
    let result = FcmProvider::new(config);
    let Err(PushError::Config(message)) = result else {
        panic!("malformed service account JSON must be a config error");
    };
    assert!(message.contains("invalid FCM service account JSON"));
}

#[test]
fn coverage_fcm_025_missing_token_uri_field_is_config_error() {
    let config = FcmConfig {
        project_id: "p".into(),
        service_account_json: r#"{"client_email":"x@p.iam.gserviceaccount.com","private_key":"k"}"#
            .into(),
    };
    let result = FcmProvider::new(config);
    let Err(PushError::Config(message)) = result else {
        panic!("missing token_uri field must be a config error");
    };
    assert!(message.contains("invalid FCM service account JSON"));
    assert!(message.contains("token_uri"));
}

#[test]
fn coverage_fcm_026_well_formed_service_account_constructs_provider() {
    let config = FcmConfig {
        project_id: "p".into(),
        service_account_json: service_account_json(FCM_TOKEN_URI).into(),
    };
    assert!(FcmProvider::new(config).is_ok());
}

#[test]
fn coverage_fcm_027_unauthorized_is_auth_rejected() {
    // 401/403 reject the OAuth access token, not the device token: the cached
    // token must be invalidated and retried, never parked permanently.
    assert_eq!(
        classify_fcm_response(StatusCode::UNAUTHORIZED, "{}"),
        FcmOutcome::AuthRejected
    );
    assert_eq!(
        classify_fcm_response(StatusCode::FORBIDDEN, "{}"),
        FcmOutcome::AuthRejected
    );
}

#[test]
fn coverage_fcm_028_quota_body_on_non_auth_status_is_retryable() {
    // Quota exhaustion is transient at any real fan-out volume: even a 400
    // carrying RESOURCE_EXHAUSTED backs off instead of failing outright.
    let body = r#"{"error":{"code":400,"status":"RESOURCE_EXHAUSTED"}}"#;
    assert_eq!(
        classify_fcm_response(StatusCode::BAD_REQUEST, body),
        FcmOutcome::Retryable
    );
}

#[test]
fn coverage_fcm_029_plain_bad_request_is_permanent() {
    let body = r#"{"error":{"code":400,"message":"INVALID_ARGUMENT"}}"#;
    assert_eq!(
        classify_fcm_response(StatusCode::BAD_REQUEST, body),
        FcmOutcome::Permanent
    );
}
