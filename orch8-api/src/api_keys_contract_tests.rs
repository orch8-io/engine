//! Contract coverage for the API-key management surface: the admin gate,
//! serde shape of [`CreateApiKeyRequest`] (including capability parsing and
//! deny-unknown strings), secret hygiene of the response DTOs, and the
//! capability defaulting inside the create handler.
//!
//! Handler tests call the handlers directly with an in-memory `SQLite`
//! backend (the same backend the shared test harness uses); nothing touches
//! the network.
//!
//! Count contract: 41 independently named unit tests.

use super::*;

use std::sync::Arc;

use crate::auth::AdminContext;
use tokio_util::sync::CancellationToken;

// The direct-handler tests intentionally exercise the optional extractor type
// used by the public handler boundary.
#[allow(clippy::unnecessary_wraps)]
fn admin() -> OptionalAdmin {
    Some(axum::Extension(AdminContext))
}

async fn test_state() -> AppState {
    let storage = orch8_storage::sqlite::SqliteStorage::in_memory()
        .await
        .expect("in-memory sqlite storage must initialise for tests");
    AppState {
        storage: Arc::new(storage),
        shutdown: CancellationToken::new(),
        max_context_bytes: 0,
        externalization_mode: orch8_types::config::ExternalizationMode::default(),
        circuit_breakers: None,
        stream_limiter: Arc::new(tokio::sync::Semaphore::new(1)),
        publisher: None,
        push_provider: Arc::new(orch8_push::NoopPushProvider),
        mobile_sync_enabled: false,
        entitlements: crate::entitlements::unlimited_provider(),
        builtin_handlers: Arc::new(Vec::new()),
        engine_ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        continuity_crypto: None,
        federation_peers: Arc::new(Vec::new()),
        continuity_lab_enabled: false,
    }
}

async fn body_json(response: axum::response::Response) -> serde_json::Value {
    let bytes = axum::body::to_bytes(response.into_body(), 1 << 20)
        .await
        .expect("response body must collect");
    serde_json::from_slice(&bytes).expect("response body must be JSON")
}

fn request(json: &str) -> CreateApiKeyRequest {
    serde_json::from_str(json).expect("request fixture must parse")
}

// --- require_admin gate. ---

#[test]
fn coverage_api_keys_001_admin_marker_passes_the_gate() {
    assert!(require_admin(&admin()).is_ok());
}

#[test]
fn coverage_api_keys_002_missing_admin_marker_is_forbidden() {
    let err = require_admin(&None).expect_err("per-tenant callers must be rejected");
    assert!(matches!(err, ApiError::Forbidden(_)));
}

#[test]
fn coverage_api_keys_003_forbidden_message_names_the_root_key_requirement() {
    let err = require_admin(&None).expect_err("must be forbidden");
    let ApiError::Forbidden(message) = err else {
        panic!("wrong variant: {err:?}");
    };
    assert_eq!(message, "API key management requires the root API key");
}

// --- CreateApiKeyRequest serde shape. ---

#[test]
fn coverage_api_keys_004_minimal_request_defaults_name_to_empty() {
    let parsed = request(r#"{"tenant_id": "acme"}"#);
    assert_eq!(parsed.tenant_id, "acme");
    assert_eq!(parsed.name, "");
}

#[test]
fn coverage_api_keys_005_minimal_request_defaults_capabilities_to_empty() {
    let parsed = request(r#"{"tenant_id": "acme"}"#);
    assert!(parsed.capabilities.is_empty());
}

#[test]
fn coverage_api_keys_006_minimal_request_defaults_expiry_to_none() {
    let parsed = request(r#"{"tenant_id": "acme"}"#);
    assert!(parsed.expires_at.is_none());
}

#[test]
fn coverage_api_keys_007_missing_tenant_id_is_rejected() {
    assert!(serde_json::from_str::<CreateApiKeyRequest>(r#"{"name": "x"}"#).is_err());
}

macro_rules! capability_parse_case {
    ($name:ident, $literal:expr, $variant:expr) => {
        #[test]
        fn $name() {
            let json = format!(r#"{{"tenant_id": "acme", "capabilities": [{}]}}"#, $literal);
            let parsed = request(&json);
            assert_eq!(parsed.capabilities, vec![$variant]);
        }
    };
}

capability_parse_case!(
    coverage_api_keys_008_operator_parses_from_snake_case,
    r#""operator""#,
    ApiCapability::Operator
);
capability_parse_case!(
    coverage_api_keys_009_worker_parses_from_snake_case,
    r#""worker""#,
    ApiCapability::Worker
);
capability_parse_case!(
    coverage_api_keys_010_device_parses_from_snake_case,
    r#""device""#,
    ApiCapability::Device
);
capability_parse_case!(
    coverage_api_keys_011_publisher_parses_from_snake_case,
    r#""publisher""#,
    ApiCapability::Publisher
);
capability_parse_case!(
    coverage_api_keys_012_approver_parses_from_snake_case,
    r#""approver""#,
    ApiCapability::Approver
);
capability_parse_case!(
    coverage_api_keys_013_auditor_parses_from_snake_case,
    r#""auditor""#,
    ApiCapability::Auditor
);

#[test]
fn coverage_api_keys_014_unknown_capability_string_is_rejected() {
    // Deny-by-default at the parsing layer: a typo must never silently
    // mint a key with fewer (or surprising) grants.
    let json = r#"{"tenant_id": "acme", "capabilities": ["superuser"]}"#;
    assert!(serde_json::from_str::<CreateApiKeyRequest>(json).is_err());
}

#[test]
fn coverage_api_keys_015_capability_parsing_is_case_sensitive() {
    let json = r#"{"tenant_id": "acme", "capabilities": ["Operator"]}"#;
    assert!(serde_json::from_str::<CreateApiKeyRequest>(json).is_err());
}

#[test]
fn coverage_api_keys_016_capabilities_must_be_an_array() {
    let json = r#"{"tenant_id": "acme", "capabilities": "operator"}"#;
    assert!(serde_json::from_str::<CreateApiKeyRequest>(json).is_err());
}

#[test]
fn coverage_api_keys_017_rfc3339_expiry_parses() {
    let json = r#"{"tenant_id": "acme", "expires_at": "2027-01-01T00:00:00Z"}"#;
    let parsed = request(json);
    assert_eq!(
        parsed.expires_at.expect("expiry must parse").to_rfc3339(),
        "2027-01-01T00:00:00+00:00"
    );
}

#[test]
fn coverage_api_keys_018_malformed_expiry_is_rejected() {
    let json = r#"{"tenant_id": "acme", "expires_at": "next tuesday"}"#;
    assert!(serde_json::from_str::<CreateApiKeyRequest>(json).is_err());
}

#[test]
fn coverage_api_keys_019_null_expiry_means_no_expiry() {
    let parsed = request(r#"{"tenant_id": "acme", "expires_at": null}"#);
    assert!(parsed.expires_at.is_none());
}

#[test]
fn coverage_api_keys_020_null_capabilities_is_rejected() {
    let json = r#"{"tenant_id": "acme", "capabilities": null}"#;
    assert!(serde_json::from_str::<CreateApiKeyRequest>(json).is_err());
}

#[test]
fn coverage_api_keys_021_unknown_fields_are_ignored() {
    let parsed = request(r#"{"tenant_id": "acme", "legacy_role": "admin"}"#);
    assert_eq!(parsed.tenant_id, "acme");
}

// --- Response DTO secret hygiene. ---

#[test]
fn coverage_api_keys_022_info_from_record_maps_every_field() {
    let minted = orch8_types::api_key::mint_scoped("acme", "ci", None, vec![ApiCapability::Worker]);
    let info = ApiKeyInfo::from(minted.record.clone());
    assert_eq!(info.id, minted.record.id);
    assert_eq!(info.tenant_id, "acme");
    assert_eq!(info.name, "ci");
    assert_eq!(info.capabilities, vec![ApiCapability::Worker]);
    assert_eq!(info.created_at, minted.record.created_at);
    assert_eq!(info.last_used_at, None);
    assert_eq!(info.expires_at, None);
    assert!(!info.revoked);
}

#[test]
fn coverage_api_keys_023_info_json_never_contains_the_secret() {
    let minted = orch8_types::api_key::mint("acme", "ci", None);
    let value = serde_json::to_value(ApiKeyInfo::from(minted.record)).expect("serialize");
    assert!(value.get("secret").is_none());
    let rendered = value.to_string();
    assert!(!rendered.contains(&minted.secret));
}

#[test]
fn coverage_api_keys_024_info_json_never_contains_the_key_hash() {
    let minted = orch8_types::api_key::mint("acme", "ci", None);
    let value = serde_json::to_value(ApiKeyInfo::from(minted.record)).expect("serialize");
    assert!(value.get("key_hash").is_none());
}

#[test]
fn coverage_api_keys_025_info_serializes_capabilities_as_snake_case_strings() {
    let minted = orch8_types::api_key::mint_scoped(
        "acme",
        "ci",
        None,
        vec![ApiCapability::Worker, ApiCapability::Auditor],
    );
    let value = serde_json::to_value(ApiKeyInfo::from(minted.record)).expect("serialize");
    assert_eq!(
        value["capabilities"],
        serde_json::json!(["worker", "auditor"])
    );
}

#[test]
fn coverage_api_keys_026_info_from_record_preserves_revocation_and_usage() {
    let mut record = orch8_types::api_key::mint("acme", "ci", None).record;
    record.revoked = true;
    record.last_used_at = Some(record.created_at);
    let info = ApiKeyInfo::from(record.clone());
    assert!(info.revoked);
    assert_eq!(info.last_used_at, record.last_used_at);
}

#[test]
fn coverage_api_keys_027_created_key_is_the_only_dto_with_the_secret() {
    let created = CreatedApiKey {
        id: "ak_1".into(),
        tenant_id: "acme".into(),
        name: "ci".into(),
        capabilities: vec![ApiCapability::Operator],
        secret: "sk_plaintext".into(),
        created_at: chrono::Utc::now(),
        expires_at: None,
    };
    let value = serde_json::to_value(&created).expect("serialize");
    assert_eq!(value["secret"], "sk_plaintext");
    assert_eq!(value["capabilities"], serde_json::json!(["operator"]));
}

#[test]
fn coverage_api_keys_028_capability_all_is_a_six_entry_grant_without_duplicates() {
    let all = ApiCapability::all();
    assert_eq!(all.len(), 6);
    let mut deduped = all.clone();
    deduped.sort_by_key(|capability| format!("{capability:?}"));
    deduped.dedup();
    assert_eq!(deduped.len(), 6);
}

// --- Create handler: capability defaulting and validation. ---

async fn create(state: &AppState, body_json: &str) -> Result<axum::response::Response, ApiError> {
    let body = request(body_json);
    create_api_key(State(state.clone()), admin(), axum::Json(body))
        .await
        .map(IntoResponse::into_response)
}

#[tokio::test]
async fn coverage_api_keys_029_omitted_capabilities_default_to_operator() {
    let state = test_state().await;
    let response = create(&state, r#"{"tenant_id": "acme"}"#)
        .await
        .expect("create must succeed");
    let body = body_json(response).await;
    assert_eq!(body["capabilities"], serde_json::json!(["operator"]));
}

#[tokio::test]
async fn coverage_api_keys_030_explicit_capabilities_are_preserved_verbatim() {
    let state = test_state().await;
    let response = create(
        &state,
        r#"{"tenant_id": "acme", "capabilities": ["worker", "auditor"]}"#,
    )
    .await
    .expect("create must succeed");
    let body = body_json(response).await;
    assert_eq!(
        body["capabilities"],
        serde_json::json!(["worker", "auditor"])
    );
}

#[tokio::test]
async fn coverage_api_keys_031_empty_tenant_id_is_an_invalid_argument() {
    let state = test_state().await;
    let err = create(&state, r#"{"tenant_id": ""}"#)
        .await
        .expect_err("empty tenant must be rejected");
    assert!(matches!(err, ApiError::InvalidArgument(_)));
}

#[tokio::test]
async fn coverage_api_keys_032_whitespace_tenant_id_is_an_invalid_argument() {
    let state = test_state().await;
    let err = create(&state, r#"{"tenant_id": "   "}"#)
        .await
        .expect_err("whitespace tenant must be rejected");
    assert!(matches!(err, ApiError::InvalidArgument(_)));
}

#[tokio::test]
async fn coverage_api_keys_033_create_without_admin_marker_is_forbidden() {
    let state = test_state().await;
    let body = request(r#"{"tenant_id": "acme"}"#);
    let err = create_api_key(State(state), None, axum::Json(body))
        .await
        .err()
        .expect("per-tenant callers must not mint keys");
    assert!(matches!(err, ApiError::Forbidden(_)));
}

#[tokio::test]
async fn coverage_api_keys_034_create_returns_201_with_one_time_secret() {
    let state = test_state().await;
    let response = create(&state, r#"{"tenant_id": "acme", "name": "ci"}"#)
        .await
        .expect("create must succeed");
    assert_eq!(response.status(), StatusCode::CREATED);
    let body = body_json(response).await;
    let secret = body["secret"].as_str().expect("secret string");
    assert!(
        secret.starts_with("sk_"),
        "secret must carry the sk_ prefix"
    );
    let id = body["id"].as_str().expect("id string");
    assert!(id.starts_with("ak_"), "id must carry the ak_ prefix");
    assert_eq!(body["tenant_id"], "acme");
    assert_eq!(body["name"], "ci");
}

// --- List and revoke handlers over the same in-memory backend. ---

#[tokio::test]
async fn coverage_api_keys_035_list_without_admin_marker_is_forbidden() {
    let state = test_state().await;
    let err = list_api_keys(
        State(state),
        None,
        Query(ListApiKeysQuery {
            tenant_id: "acme".into(),
        }),
    )
    .await
    .err()
    .expect("per-tenant callers must not list keys");
    assert!(matches!(err, ApiError::Forbidden(_)));
}

#[tokio::test]
async fn coverage_api_keys_036_list_rejects_an_empty_tenant_filter() {
    let state = test_state().await;
    let err = list_api_keys(
        State(state),
        admin(),
        Query(ListApiKeysQuery {
            tenant_id: "  ".into(),
        }),
    )
    .await
    .err()
    .expect("empty tenant filter must be rejected");
    assert!(matches!(err, ApiError::InvalidArgument(_)));
}

#[tokio::test]
async fn coverage_api_keys_037_list_round_trips_the_minted_capabilities() {
    let state = test_state().await;
    create(
        &state,
        r#"{"tenant_id": "acme", "capabilities": ["worker"]}"#,
    )
    .await
    .expect("create must succeed");
    let response = list_api_keys(
        State(state),
        admin(),
        Query(ListApiKeysQuery {
            tenant_id: "acme".into(),
        }),
    )
    .await
    .expect("list must succeed")
    .into_response();
    let body = body_json(response).await;
    let keys = body.as_array().expect("list of keys");
    assert_eq!(keys.len(), 1);
    assert_eq!(keys[0]["capabilities"], serde_json::json!(["worker"]));
    assert!(keys[0].get("secret").is_none());
    assert!(keys[0].get("key_hash").is_none());
}

#[tokio::test]
async fn coverage_api_keys_038_revoking_an_unknown_key_is_not_found() {
    let state = test_state().await;
    let err = revoke_api_key(State(state), admin(), Path("ak_missing".into()))
        .await
        .err()
        .expect("unknown key must be 404");
    assert!(matches!(err, ApiError::NotFound(_)));
}

#[tokio::test]
async fn coverage_api_keys_039_revoking_an_existing_key_returns_no_content() {
    let state = test_state().await;
    let response = create(&state, r#"{"tenant_id": "acme"}"#)
        .await
        .expect("create must succeed");
    let body = body_json(response).await;
    let id = body["id"].as_str().expect("id string").to_string();
    let response = revoke_api_key(State(state), admin(), Path(id))
        .await
        .expect("revoke must succeed")
        .into_response();
    assert_eq!(response.status(), StatusCode::NO_CONTENT);
}

#[tokio::test]
async fn coverage_api_keys_040_revoke_without_admin_marker_is_forbidden() {
    let state = test_state().await;
    let err = revoke_api_key(State(state), None, Path("ak_any".into()))
        .await
        .err()
        .expect("per-tenant callers must not revoke keys");
    assert!(matches!(err, ApiError::Forbidden(_)));
}

#[tokio::test]
async fn coverage_api_keys_041_explicit_empty_capabilities_array_defaults_to_operator() {
    // The handler defaults on `capabilities.is_empty()`, so an explicit `[]`
    // must behave exactly like an omitted field (covered by 029).
    let state = test_state().await;
    let response = create(&state, r#"{"tenant_id": "acme", "capabilities": []}"#)
        .await
        .expect("create must succeed");
    let body = body_json(response).await;
    assert_eq!(body["capabilities"], serde_json::json!(["operator"]));
}
