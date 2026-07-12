//! End-to-end tests for the sequence preflight endpoints.
//!
//! Exercises `POST /sequences/preflight` (draft) and
//! `GET /sequences/{id}/preflight` (stored) against the full axum router
//! with in-memory SQLite, seeding runtime state (workers, pins,
//! credentials, plugins, queue dispatch, routing rules, sub-sequences)
//! directly through the storage traits.

use chrono::{DateTime, Duration, Utc};
use reqwest::StatusCode;
use serde_json::{Value, json};
use uuid::Uuid;

use orch8_api::test_harness::{TestServer, spawn_test_server};
use orch8_storage::{AdminStore, SequenceStore, WorkerStore};
use orch8_types::config::SecretString;
use orch8_types::credential::{CredentialDef, CredentialKind};
use orch8_types::ids::SequenceId;
use orch8_types::plugin::{PluginDef, PluginType};
use orch8_types::queue_dispatch::{DispatchMode, QueueDispatchConfig};
use orch8_types::queue_routing::QueueRoutingRule;
use orch8_types::worker::{WorkerRegistration, WorkerVersionPin};

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

fn draft(tenant: &str, name: &str, blocks: Value) -> Value {
    json!({
        "id": Uuid::now_v7(),
        "tenant_id": tenant,
        "namespace": "default",
        "name": name,
        "version": 1,
        "blocks": blocks,
        "created_at": Utc::now().to_rfc3339(),
    })
}

fn noop_step(id: &str) -> Value {
    json!({"type": "step", "id": id, "handler": "noop", "params": {}})
}

fn step(id: &str, handler: &str) -> Value {
    json!({"type": "step", "id": id, "handler": handler, "params": {}})
}

/// POST a draft to the preflight endpoint, asserting 200, returning the report.
async fn preflight(srv: &TestServer, tenant: &str, body: &Value) -> Value {
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", tenant)
        .json(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK, "{body}");
    resp.json().await.unwrap()
}

/// Create a stored sequence via the API; returns its id.
async fn create_sequence(srv: &TestServer, tenant: &str, body: &Value) -> Uuid {
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences", srv.v1_url()))
        .header("X-Tenant-Id", tenant)
        .json(body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::CREATED, "create failed: {body}");
    Uuid::parse_str(body["id"].as_str().unwrap()).unwrap()
}

async fn set_status(srv: &TestServer, id: Uuid, status: &str) {
    srv.storage
        .update_sequence_status(SequenceId::from_uuid(id), status)
        .await
        .unwrap();
}

fn check<'a>(report: &'a Value, id: &str) -> &'a Value {
    report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|c| c["id"] == id)
        .unwrap_or_else(|| panic!("missing check {id} in {report}"))
}

fn finding_codes(check: &Value) -> Vec<&str> {
    check["findings"]
        .as_array()
        .map(|fs| fs.iter().filter_map(|f| f["code"].as_str()).collect())
        .unwrap_or_default()
}

async fn seed_worker(
    srv: &TestServer,
    handler: &str,
    queue: Option<&str>,
    version: Option<&str>,
    tenant: Option<&str>,
    last_seen_at: DateTime<Utc>,
) {
    srv.storage
        .upsert_worker_registration(&WorkerRegistration {
            worker_id: format!("w-{handler}"),
            handler_name: handler.to_string(),
            queue_name: queue.map(ToString::to_string),
            version: version.map(ToString::to_string),
            tenant_id: tenant.map(ToString::to_string),
            last_seen_at,
        })
        .await
        .unwrap();
}

async fn seed_pin(srv: &TestServer, tenant: &str, handler: &str, min_version: &str) {
    srv.storage
        .upsert_worker_version_pin(&WorkerVersionPin {
            tenant_id: tenant.to_string(),
            handler_name: handler.to_string(),
            min_version: min_version.to_string(),
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

async fn seed_credential(
    srv: &TestServer,
    tenant: &str,
    id: &str,
    enabled: bool,
    expires_at: Option<DateTime<Utc>>,
) {
    srv.storage
        .create_credential(&CredentialDef {
            id: id.to_string(),
            tenant_id: tenant.to_string(),
            name: format!("credential {id}"),
            kind: CredentialKind::ApiKey,
            value: SecretString::new(r#"{"token":"shhh"}"#.to_string()),
            expires_at,
            refresh_url: None,
            refresh_token: None,
            enabled,
            description: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

async fn seed_plugin(srv: &TestServer, tenant: &str, name: &str, enabled: bool) {
    srv.storage
        .create_plugin(&PluginDef {
            name: name.to_string(),
            plugin_type: PluginType::Wasm,
            source: "/opt/plugins/test.wasm".to_string(),
            tenant_id: tenant.to_string(),
            enabled,
            config: json!({}),
            description: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

async fn seed_queue_dispatch(srv: &TestServer, tenant: &str, queue: &str, mode: DispatchMode) {
    srv.storage
        .upsert_queue_dispatch(&QueueDispatchConfig {
            tenant_id: tenant.to_string(),
            queue_name: queue.to_string(),
            mode,
            push_url: match mode {
                DispatchMode::Push => Some("https://worker.example.com/tasks".to_string()),
                DispatchMode::Poll => None,
            },
            secret: None,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

async fn seed_routing_rule(srv: &TestServer, tenant: &str, match_queue: &str, enabled: bool) {
    srv.storage
        .create_queue_routing_rule(&QueueRoutingRule {
            id: Uuid::now_v7(),
            tenant_id: tenant.to_string(),
            handler_name: "any-handler".to_string(),
            match_queue: Some(match_queue.to_string()),
            queue_override: "somewhere-else".to_string(),
            priority: 0,
            enabled,
            created_at: Utc::now(),
            updated_at: Utc::now(),
        })
        .await
        .unwrap();
}

// ---------------------------------------------------------------------------
// draft preflight: basics and failure classes
// ---------------------------------------------------------------------------

#[tokio::test]
async fn builtin_only_draft_passes_overall() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-builtin", json!([noop_step("a")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(report["overall"], "pass", "{report}");
    for c in report["checks"].as_array().unwrap() {
        assert_eq!(c["status"], "pass", "{c}");
    }
}

#[tokio::test]
async fn draft_missing_worker_fails_handler_check() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-noworker", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(report["overall"], "fail");
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["NO_COMPATIBLE_WORKER"]);
}

#[tokio::test]
async fn missing_worker_finding_carries_resource_and_remediation() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-shape", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    let f = &check(&report, "handlers_have_workers")["findings"][0];
    assert_eq!(f["affected_resource"]["kind"], "handler");
    assert_eq!(f["affected_resource"]["id"], "charge_card");
    assert_eq!(f["severity"], "error");
    assert!(f["remediation"][0]["summary"].as_str().unwrap().contains("worker"));
}

#[tokio::test]
async fn draft_missing_credential_fails() {
    let srv = spawn_test_server().await;
    let body = draft(
        "t1",
        "pf-nocred",
        json!([
            {"type": "step", "id": "a", "handler": "noop",
             "params": {"key": "credentials://billing_key"}}
        ]),
    );
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "credentials_present");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["CREDENTIAL_MISSING"]);
    assert_eq!(c["findings"][0]["affected_resource"]["id"], "billing_key");
}

#[tokio::test]
async fn multiple_failure_classes_accumulate_in_one_report() {
    let srv = spawn_test_server().await;
    let body = draft(
        "t1",
        "pf-multi",
        json!([
            {"type": "step", "id": "a", "handler": "custom_x",
             "params": {"key": "credentials://absent"}},
            {"type": "sub_sequence", "id": "c", "sequence_name": "ghost", "input": {}}
        ]),
    );
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(report["overall"], "fail");
    assert_eq!(check(&report, "handlers_have_workers")["status"], "fail");
    assert_eq!(check(&report, "credentials_present")["status"], "fail");
    assert_eq!(check(&report, "sub_sequences_available")["status"], "fail");
}

#[tokio::test]
async fn lint_warning_surfaces_for_http_request_without_url() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-lint", json!([step("fetch", "http_request")]));
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "lint_clean");
    assert_eq!(c["status"], "warning");
    assert_eq!(finding_codes(c), vec!["LINT_WARNING"]);
    assert_eq!(c["findings"][0]["affected_resource"]["kind"], "block");
    assert_eq!(c["findings"][0]["affected_resource"]["id"], "fetch");
}

#[tokio::test]
async fn duplicate_block_ids_fail_definition_check() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-dup", json!([noop_step("dup"), noop_step("dup")]));
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "definition_valid");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["INVALID_DEFINITION"]);
    assert_eq!(report["overall"], "fail");
}

#[tokio::test]
async fn empty_blocks_fail_definition_check() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-empty", json!([]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "definition_valid")["status"], "fail");
    assert_eq!(report["overall"], "fail");
}

#[tokio::test]
async fn orphan_queue_warns_but_stays_overall_warning() {
    let srv = spawn_test_server().await;
    let body = draft(
        "t1",
        "pf-orphanq",
        json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}, "queue_name": "orphan"}
        ]),
    );
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "queues_consumable");
    assert_eq!(c["status"], "warning");
    assert_eq!(finding_codes(c), vec!["QUEUE_HAS_NO_CONSUMER"]);
    assert_eq!(report["overall"], "warning");
}

#[tokio::test]
async fn report_contains_all_eight_check_ids() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-ids", json!([noop_step("a")]));
    let report = preflight(&srv, "t1", &body).await;
    let ids: Vec<&str> = report["checks"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["id"].as_str().unwrap())
        .collect();
    for expected in [
        "definition_valid",
        "lint_clean",
        "input_schema_valid",
        "handlers_have_workers",
        "plugins_enabled",
        "credentials_present",
        "queues_consumable",
        "sub_sequences_available",
    ] {
        assert!(ids.contains(&expected), "missing {expected} in {ids:?}");
    }
}

#[tokio::test]
async fn report_carries_name_version_and_timestamp() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-meta", json!([noop_step("a")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(report["sequence_name"], "pf-meta");
    assert_eq!(report["sequence_version"], 1);
    assert!(report["generated_at"].as_str().is_some());
}

// ---------------------------------------------------------------------------
// input schema: structural vs deep compilation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn deep_compilation_rejects_unknown_type_keyword() {
    let srv = spawn_test_server().await;
    let mut body = draft("t1", "pf-schema1", json!([noop_step("a")]));
    body["input_schema"] = json!({"type": "definitely_not_a_type"});
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "input_schema_valid");
    assert_eq!(c["status"], "fail", "{report}");
    assert_eq!(finding_codes(c), vec!["INVALID_INPUT_SCHEMA"]);
    assert_eq!(report["overall"], "fail");
}

#[tokio::test]
async fn deep_compilation_rejects_numeric_type_keyword() {
    let srv = spawn_test_server().await;
    let mut body = draft("t1", "pf-schema2", json!([noop_step("a")]));
    body["input_schema"] = json!({"type": 42});
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "input_schema_valid")["status"], "fail");
}

#[tokio::test]
async fn structural_failure_for_non_object_schema() {
    let srv = spawn_test_server().await;
    let mut body = draft("t1", "pf-schema3", json!([noop_step("a")]));
    body["input_schema"] = json!("not an object");
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "input_schema_valid");
    assert_eq!(c["status"], "fail");
    // The engine's structural check catches it (before deep compilation).
    assert!(
        c["findings"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("string")
    );
}

#[tokio::test]
async fn valid_object_schema_passes_deep_compilation() {
    let srv = spawn_test_server().await;
    let mut body = draft("t1", "pf-schema4", json!([noop_step("a")]));
    body["input_schema"] = json!({
        "type": "object",
        "properties": {"count": {"type": "integer", "minimum": 0}},
        "required": ["count"]
    });
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "input_schema_valid")["status"], "pass");
    assert_eq!(report["overall"], "pass");
}

#[tokio::test]
async fn absent_schema_passes() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-schema5", json!([noop_step("a")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "input_schema_valid")["status"], "pass");
}

// ---------------------------------------------------------------------------
// workers: registrations, liveness, tenancy, pins
// ---------------------------------------------------------------------------

#[tokio::test]
async fn registered_worker_flips_handler_check_to_pass() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, Some("1.0.0"), None, Utc::now()).await;
    let body = draft("t1", "pf-w1", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
    assert_eq!(report["overall"], "pass", "{report}");
}

#[tokio::test]
async fn stale_worker_registration_still_fails() {
    // Liveness window is 120s; a worker last seen 10 minutes ago is dead.
    let srv = spawn_test_server().await;
    seed_worker(
        &srv,
        "charge_card",
        None,
        Some("1.0.0"),
        None,
        Utc::now() - Duration::minutes(10),
    )
    .await;
    let body = draft("t1", "pf-w2", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["NO_COMPATIBLE_WORKER"]);
}

#[tokio::test]
async fn worker_scoped_to_other_tenant_fails() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, None, Some("other-tenant"), Utc::now()).await;
    let body = draft("t1", "pf-w3", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "fail");
}

#[tokio::test]
async fn worker_scoped_to_matching_tenant_passes() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, None, Some("t1"), Utc::now()).await;
    let body = draft("t1", "pf-w4", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
}

#[tokio::test]
async fn worker_with_empty_tenant_serves_everyone() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, None, Some(""), Utc::now()).await;
    let body = draft("t1", "pf-w5", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
}

#[tokio::test]
async fn unsatisfied_version_pin_fails_with_evidence() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, Some("1.1.0"), None, Utc::now()).await;
    seed_pin(&srv, "t1", "charge_card", "1.2.0").await;
    let body = draft("t1", "pf-pin1", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["WORKER_BELOW_VERSION_PIN"]);
    assert!(
        c["findings"][0]["evidence"][0]["summary"]
            .as_str()
            .unwrap()
            .contains("1.1.0")
    );
}

#[tokio::test]
async fn satisfied_version_pin_passes() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, Some("1.2.0"), None, Utc::now()).await;
    seed_pin(&srv, "t1", "charge_card", "1.2.0").await;
    let body = draft("t1", "pf-pin2", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
}

#[tokio::test]
async fn version_pin_compares_numerically_not_lexically() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, Some("1.10.0"), None, Utc::now()).await;
    seed_pin(&srv, "t1", "charge_card", "1.9.0").await;
    let body = draft("t1", "pf-pin3", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
}

#[tokio::test]
async fn version_pin_for_other_tenant_is_ignored() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, Some("0.1.0"), None, Utc::now()).await;
    seed_pin(&srv, "someone-else", "charge_card", "9.9.9").await;
    let body = draft("t1", "pf-pin4", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
}

#[tokio::test]
async fn unversioned_worker_never_satisfies_a_pin() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "charge_card", None, None, None, Utc::now()).await;
    seed_pin(&srv, "t1", "charge_card", "1.0.0").await;
    let body = draft("t1", "pf-pin5", json!([step("x", "charge_card")]));
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "handlers_have_workers");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["WORKER_BELOW_VERSION_PIN"]);
}

// ---------------------------------------------------------------------------
// credentials
// ---------------------------------------------------------------------------

fn cred_draft(tenant: &str, name: &str, cred_id: &str) -> Value {
    draft(
        tenant,
        name,
        json!([
            {"type": "step", "id": "a", "handler": "noop",
             "params": {"key": format!("credentials://{cred_id}")}}
        ]),
    )
}

#[tokio::test]
async fn seeded_credential_flips_check_to_pass() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "t1", "stripe", true, None).await;
    let report = preflight(&srv, "t1", &cred_draft("t1", "pf-c1", "stripe")).await;
    assert_eq!(check(&report, "credentials_present")["status"], "pass");
    assert_eq!(report["overall"], "pass", "{report}");
}

#[tokio::test]
async fn disabled_credential_fails() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "t1", "stripe", false, None).await;
    let report = preflight(&srv, "t1", &cred_draft("t1", "pf-c2", "stripe")).await;
    let c = check(&report, "credentials_present");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["CREDENTIAL_DISABLED"]);
}

#[tokio::test]
async fn expired_credential_fails() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "t1", "stripe", true, Some(Utc::now() - Duration::hours(2))).await;
    let report = preflight(&srv, "t1", &cred_draft("t1", "pf-c3", "stripe")).await;
    let c = check(&report, "credentials_present");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["CREDENTIAL_EXPIRED"]);
}

#[tokio::test]
async fn credential_with_future_expiry_passes() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "t1", "stripe", true, Some(Utc::now() + Duration::days(30))).await;
    let report = preflight(&srv, "t1", &cred_draft("t1", "pf-c4", "stripe")).await;
    assert_eq!(check(&report, "credentials_present")["status"], "pass");
}

#[tokio::test]
async fn global_credential_is_visible_to_any_tenant() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "", "shared-key", true, None).await;
    let report = preflight(&srv, "t1", &cred_draft("t1", "pf-c5", "shared-key")).await;
    assert_eq!(check(&report, "credentials_present")["status"], "pass");
}

#[tokio::test]
async fn other_tenants_credential_is_invisible() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "other-tenant", "their-key", true, None).await;
    let report = preflight(&srv, "t1", &cred_draft("t1", "pf-c6", "their-key")).await;
    let c = check(&report, "credentials_present");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["CREDENTIAL_MISSING"]);
}

#[tokio::test]
async fn credential_field_suffix_resolves_to_bare_id() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "t1", "vault", true, None).await;
    let body = draft(
        "t1",
        "pf-c7",
        json!([
            {"type": "step", "id": "a", "handler": "noop",
             "params": {"key": "credentials://vault/token"}}
        ]),
    );
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "credentials_present")["status"], "pass");
}

#[tokio::test]
async fn credential_in_nested_composite_is_checked() {
    let srv = spawn_test_server().await;
    let body = draft(
        "t1",
        "pf-c8",
        json!([
            {"type": "parallel", "id": "p", "branches": [[
                {"type": "try_catch", "id": "tc",
                 "try_block": [
                    {"type": "step", "id": "b", "handler": "noop",
                     "params": {"key": "credentials://deep-cred"}}
                 ],
                 "catch_block": [noop_step("c")]}
            ]]}
        ]),
    );
    let report = preflight(&srv, "t1", &body).await;
    let c = check(&report, "credentials_present");
    assert_eq!(c["status"], "fail");
    assert_eq!(c["findings"][0]["affected_resource"]["id"], "deep-cred");
}

#[tokio::test]
async fn missing_credential_finding_has_remediation_command() {
    let srv = spawn_test_server().await;
    let report = preflight(&srv, "t1", &cred_draft("t1", "pf-c9", "ghost-cred")).await;
    let f = &check(&report, "credentials_present")["findings"][0];
    assert!(
        f["remediation"][0]["command"]
            .as_str()
            .unwrap()
            .contains("orch8 credential create ghost-cred")
    );
}

// ---------------------------------------------------------------------------
// plugins
// ---------------------------------------------------------------------------

#[tokio::test]
async fn enabled_plugin_covers_handler_without_worker() {
    let srv = spawn_test_server().await;
    seed_plugin(&srv, "t1", "sentiment", true).await;
    let body = draft("t1", "pf-p1", json!([step("x", "sentiment")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
    assert_eq!(check(&report, "plugins_enabled")["status"], "pass");
    assert_eq!(report["overall"], "pass", "{report}");
}

#[tokio::test]
async fn disabled_plugin_fails_both_plugin_and_handler_checks() {
    let srv = spawn_test_server().await;
    seed_plugin(&srv, "t1", "sentiment", false).await;
    let body = draft("t1", "pf-p2", json!([step("x", "sentiment")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "fail");
    let c = check(&report, "plugins_enabled");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["PLUGIN_DISABLED"]);
    assert_eq!(c["findings"][0]["affected_resource"]["kind"], "plugin");
}

#[tokio::test]
async fn other_tenants_plugin_is_invisible() {
    let srv = spawn_test_server().await;
    seed_plugin(&srv, "other-tenant", "sentiment", true).await;
    let body = draft("t1", "pf-p3", json!([step("x", "sentiment")]));
    let report = preflight(&srv, "t1", &body).await;
    // Not visible → cannot cover the handler; plugin check itself passes
    // (no disabled plugin referenced from this tenant's point of view).
    assert_eq!(check(&report, "handlers_have_workers")["status"], "fail");
    assert_eq!(check(&report, "plugins_enabled")["status"], "pass");
}

#[tokio::test]
async fn global_plugin_is_visible_to_any_tenant() {
    let srv = spawn_test_server().await;
    seed_plugin(&srv, "", "sentiment", true).await;
    let body = draft("t1", "pf-p4", json!([step("x", "sentiment")]));
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "pass");
}

// ---------------------------------------------------------------------------
// queues: polling workers, push dispatch, routing rules
// ---------------------------------------------------------------------------

fn queue_draft(tenant: &str, name: &str, queue: &str) -> Value {
    draft(
        tenant,
        name,
        json!([
            {"type": "step", "id": "a", "handler": "noop", "params": {}, "queue_name": queue}
        ]),
    )
}

#[tokio::test]
async fn polling_worker_on_queue_satisfies_consumer_check() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "whatever", Some("billing"), None, None, Utc::now()).await;
    let report = preflight(&srv, "t1", &queue_draft("t1", "pf-q1", "billing")).await;
    assert_eq!(check(&report, "queues_consumable")["status"], "pass");
}

#[tokio::test]
async fn push_dispatch_config_satisfies_consumer_check() {
    let srv = spawn_test_server().await;
    seed_queue_dispatch(&srv, "t1", "push-q", DispatchMode::Push).await;
    let report = preflight(&srv, "t1", &queue_draft("t1", "pf-q2", "push-q")).await;
    assert_eq!(check(&report, "queues_consumable")["status"], "pass");
}

#[tokio::test]
async fn poll_mode_dispatch_config_does_not_satisfy_consumer_check() {
    let srv = spawn_test_server().await;
    seed_queue_dispatch(&srv, "t1", "poll-q", DispatchMode::Poll).await;
    let report = preflight(&srv, "t1", &queue_draft("t1", "pf-q3", "poll-q")).await;
    let c = check(&report, "queues_consumable");
    assert_eq!(c["status"], "warning");
    assert_eq!(finding_codes(c), vec!["QUEUE_HAS_NO_CONSUMER"]);
}

#[tokio::test]
async fn enabled_routing_rule_redirect_satisfies_consumer_check() {
    let srv = spawn_test_server().await;
    seed_routing_rule(&srv, "t1", "routed-q", true).await;
    let report = preflight(&srv, "t1", &queue_draft("t1", "pf-q4", "routed-q")).await;
    assert_eq!(check(&report, "queues_consumable")["status"], "pass");
}

#[tokio::test]
async fn disabled_routing_rule_does_not_redirect() {
    let srv = spawn_test_server().await;
    seed_routing_rule(&srv, "t1", "routed-q", false).await;
    let report = preflight(&srv, "t1", &queue_draft("t1", "pf-q5", "routed-q")).await;
    assert_eq!(check(&report, "queues_consumable")["status"], "warning");
}

#[tokio::test]
async fn other_tenants_routing_rule_is_invisible() {
    let srv = spawn_test_server().await;
    seed_routing_rule(&srv, "other-tenant", "routed-q", true).await;
    let report = preflight(&srv, "t1", &queue_draft("t1", "pf-q6", "routed-q")).await;
    assert_eq!(check(&report, "queues_consumable")["status"], "warning");
}

#[tokio::test]
async fn stale_polling_worker_does_not_consume_queue() {
    let srv = spawn_test_server().await;
    seed_worker(
        &srv,
        "whatever",
        Some("billing"),
        None,
        None,
        Utc::now() - Duration::minutes(30),
    )
    .await;
    let report = preflight(&srv, "t1", &queue_draft("t1", "pf-q7", "billing")).await;
    assert_eq!(check(&report, "queues_consumable")["status"], "warning");
}

// ---------------------------------------------------------------------------
// sub-sequences
// ---------------------------------------------------------------------------

fn sub_ref_draft(tenant: &str, name: &str, target: &str, version: Option<i32>) -> Value {
    let block = match version {
        Some(v) => json!({"type": "sub_sequence", "id": "child", "sequence_name": target,
                           "version": v, "input": {}}),
        None => json!({"type": "sub_sequence", "id": "child", "sequence_name": target,
                        "input": {}}),
    };
    draft(tenant, name, json!([block]))
}

#[tokio::test]
async fn missing_sub_sequence_fails() {
    let srv = spawn_test_server().await;
    let report = preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s1", "ghost", None)).await;
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["SUB_SEQUENCE_MISSING"]);
}

#[tokio::test]
async fn production_sub_sequence_passes() {
    let srv = spawn_test_server().await;
    // Created sequences default to production status.
    let target = draft("t1", "child-prod", json!([noop_step("a")]));
    create_sequence(&srv, "t1", &target).await;
    let report = preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s2", "child-prod", None)).await;
    assert_eq!(check(&report, "sub_sequences_available")["status"], "pass");
}

#[tokio::test]
async fn draft_only_sub_sequence_warns() {
    let srv = spawn_test_server().await;
    let target = draft("t1", "child-draft", json!([noop_step("a")]));
    let id = create_sequence(&srv, "t1", &target).await;
    set_status(&srv, id, "draft").await;
    let report = preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s3", "child-draft", None)).await;
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c["status"], "warning");
    assert_eq!(finding_codes(c), vec!["SUB_SEQUENCE_DRAFT_ONLY"]);
    assert_eq!(report["overall"], "warning", "{report}");
}

#[tokio::test]
async fn unpublished_sub_sequence_fails() {
    let srv = spawn_test_server().await;
    let target = draft("t1", "child-unpub", json!([noop_step("a")]));
    let id = create_sequence(&srv, "t1", &target).await;
    set_status(&srv, id, "unpublished").await;
    let report = preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s4", "child-unpub", None)).await;
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["SUB_SEQUENCE_UNPUBLISHED"]);
}

#[tokio::test]
async fn staging_sub_sequence_passes() {
    let srv = spawn_test_server().await;
    let target = draft("t1", "child-staging", json!([noop_step("a")]));
    let id = create_sequence(&srv, "t1", &target).await;
    set_status(&srv, id, "staging").await;
    let report = preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s5", "child-staging", None)).await;
    assert_eq!(check(&report, "sub_sequences_available")["status"], "pass");
}

#[tokio::test]
async fn version_filter_mismatch_fails_as_missing() {
    let srv = spawn_test_server().await;
    let target = draft("t1", "child-v1", json!([noop_step("a")]));
    create_sequence(&srv, "t1", &target).await;
    let report = preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s6", "child-v1", Some(7))).await;
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["SUB_SEQUENCE_MISSING"]);
}

#[tokio::test]
async fn version_filter_match_passes() {
    let srv = spawn_test_server().await;
    let target = draft("t1", "child-vmatch", json!([noop_step("a")]));
    create_sequence(&srv, "t1", &target).await;
    let report =
        preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s7", "child-vmatch", Some(1))).await;
    assert_eq!(check(&report, "sub_sequences_available")["status"], "pass");
}

#[tokio::test]
async fn sub_sequence_in_other_namespace_is_invisible() {
    let srv = spawn_test_server().await;
    let mut target = draft("t1", "child-ns", json!([noop_step("a")]));
    target["namespace"] = json!("otherns");
    create_sequence(&srv, "t1", &target).await;
    // The parent lives in "default" so the reference must not resolve.
    let report = preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s8", "child-ns", None)).await;
    assert_eq!(check(&report, "sub_sequences_available")["status"], "fail");
}

#[tokio::test]
async fn other_tenants_sub_sequence_is_invisible() {
    let srv = spawn_test_server().await;
    let target = draft("other-tenant", "child-foreign", json!([noop_step("a")]));
    create_sequence(&srv, "other-tenant", &target).await;
    let report =
        preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s9", "child-foreign", None)).await;
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c["status"], "fail");
    assert_eq!(finding_codes(c), vec!["SUB_SEQUENCE_MISSING"]);
}

#[tokio::test]
async fn draft_version_plus_production_version_passes() {
    let srv = spawn_test_server().await;
    let v1 = draft("t1", "child-mixed", json!([noop_step("a")]));
    let v1_id = create_sequence(&srv, "t1", &v1).await;
    set_status(&srv, v1_id, "draft").await;
    let mut v2 = draft("t1", "child-mixed", json!([noop_step("a")]));
    v2["id"] = json!(Uuid::now_v7());
    v2["version"] = json!(2);
    create_sequence(&srv, "t1", &v2).await;
    let report =
        preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s10", "child-mixed", None)).await;
    assert_eq!(check(&report, "sub_sequences_available")["status"], "pass");
}

#[tokio::test]
async fn version_pinned_to_draft_version_warns_despite_production_sibling() {
    let srv = spawn_test_server().await;
    let v1 = draft("t1", "child-pinned", json!([noop_step("a")]));
    create_sequence(&srv, "t1", &v1).await; // v1 production
    let mut v2 = draft("t1", "child-pinned", json!([noop_step("a")]));
    v2["id"] = json!(Uuid::now_v7());
    v2["version"] = json!(2);
    let v2_id = create_sequence(&srv, "t1", &v2).await;
    set_status(&srv, v2_id, "draft").await;
    let report =
        preflight(&srv, "t1", &sub_ref_draft("t1", "pf-s11", "child-pinned", Some(2))).await;
    let c = check(&report, "sub_sequences_available");
    assert_eq!(c["status"], "warning");
    assert_eq!(finding_codes(c), vec!["SUB_SEQUENCE_DRAFT_ONLY"]);
}

// ---------------------------------------------------------------------------
// stored sequence preflight
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stored_sequence_preflight_reports_failures() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-stored1", json!([step("x", "external_thing")]));
    let id = create_sequence(&srv, "t1", &body).await;

    let resp = reqwest::Client::new()
        .get(format!("{}/sequences/{id}/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(report["overall"], "fail");
    assert_eq!(report["sequence_name"], "pf-stored1");
    assert_eq!(check(&report, "handlers_have_workers")["status"], "fail");
}

#[tokio::test]
async fn stored_sequence_preflight_passes_when_runtime_is_ready() {
    let srv = spawn_test_server().await;
    seed_worker(&srv, "external_thing", None, Some("1.0.0"), None, Utc::now()).await;
    let body = draft("t1", "pf-stored2", json!([step("x", "external_thing")]));
    let id = create_sequence(&srv, "t1", &body).await;

    let resp = reqwest::Client::new()
        .get(format!("{}/sequences/{id}/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(report["overall"], "pass", "{report}");
}

#[tokio::test]
async fn stored_preflight_is_tenant_isolated_with_404() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-stored3", json!([noop_step("a")]));
    let id = create_sequence(&srv, "t1", &body).await;

    let resp = reqwest::Client::new()
        .get(format!("{}/sequences/{id}/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "intruder")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn stored_preflight_unknown_id_is_404() {
    let srv = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .get(format!(
            "{}/sequences/{}/preflight",
            srv.v1_url(),
            Uuid::now_v7()
        ))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

// ---------------------------------------------------------------------------
// request validation and tenancy on the draft endpoint
// ---------------------------------------------------------------------------

#[tokio::test]
async fn header_body_tenant_mismatch_is_forbidden() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-mismatch", json!([noop_step("a")]));
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "different-tenant")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::FORBIDDEN);
}

#[tokio::test]
async fn draft_without_header_uses_body_tenant_for_inventory() {
    let srv = spawn_test_server().await;
    seed_credential(&srv, "t9", "body-cred", true, None).await;
    let body = cred_draft("t9", "pf-noheader", "body-cred");
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .json(&body)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let report: Value = resp.json().await.unwrap();
    assert_eq!(check(&report, "credentials_present")["status"], "pass");
}

#[tokio::test]
async fn non_json_body_is_client_error() {
    let srv = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .header("Content-Type", "application/json")
        .body("this is not json {{{")
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "expected 4xx, got {}",
        resp.status()
    );
}

#[tokio::test]
async fn malformed_sequence_json_is_400_or_422() {
    let srv = spawn_test_server().await;
    // Syntactically valid JSON that is not a sequence definition.
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"blocks": 42}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::UNPROCESSABLE_ENTITY,
        "expected 400/422, got {status}"
    );
}

#[tokio::test]
async fn sequence_missing_required_fields_is_400_or_422() {
    let srv = spawn_test_server().await;
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&json!({"name": "incomplete"}))
        .send()
        .await
        .unwrap();
    let status = resp.status();
    assert!(
        status == StatusCode::BAD_REQUEST || status == StatusCode::UNPROCESSABLE_ENTITY,
        "expected 400/422, got {status}"
    );
}

#[tokio::test]
async fn blocks_with_unknown_block_type_is_client_error() {
    let srv = spawn_test_server().await;
    let body = draft(
        "t1",
        "pf-badblock",
        json!([{"type": "no_such_block_type", "id": "a"}]),
    );
    let resp = reqwest::Client::new()
        .post(format!("{}/sequences/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .json(&body)
        .send()
        .await
        .unwrap();
    assert!(
        resp.status().is_client_error(),
        "expected 4xx, got {}",
        resp.status()
    );
}

// ---------------------------------------------------------------------------
// response shape details
// ---------------------------------------------------------------------------

#[tokio::test]
async fn statuses_serialize_snake_case_in_http_response() {
    let srv = spawn_test_server().await;
    let body = draft(
        "t1",
        "pf-snake",
        json!([
            {"type": "step", "id": "a", "handler": "custom_x", "params": {},
             "queue_name": "orphan"}
        ]),
    );
    let report = preflight(&srv, "t1", &body).await;
    assert_eq!(check(&report, "handlers_have_workers")["status"], "fail");
    assert_eq!(check(&report, "queues_consumable")["status"], "warning");
    assert_eq!(check(&report, "definition_valid")["status"], "pass");
    assert_eq!(report["overall"], "fail");
}

#[tokio::test]
async fn findings_carry_severity_confidence_and_observed_at() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-fshape", json!([step("x", "custom_x")]));
    let report = preflight(&srv, "t1", &body).await;
    let f = &check(&report, "handlers_have_workers")["findings"][0];
    assert_eq!(f["code"], "NO_COMPATIBLE_WORKER");
    assert_eq!(f["severity"], "error");
    assert_eq!(f["confidence"], "high");
    assert!(f["observed_at"].as_str().is_some());
    assert!(f["summary"].as_str().unwrap().contains("custom_x"));
}

#[tokio::test]
async fn passing_checks_omit_findings_in_http_response() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-omit", json!([noop_step("a")]));
    let report = preflight(&srv, "t1", &body).await;
    for c in report["checks"].as_array().unwrap() {
        assert!(c.get("findings").is_none(), "clean check has findings: {c}");
    }
}

#[tokio::test]
async fn draft_and_stored_reports_agree_for_the_same_sequence() {
    let srv = spawn_test_server().await;
    let body = draft("t1", "pf-agree", json!([step("x", "custom_x")]));
    let draft_report = preflight(&srv, "t1", &body).await;
    let id = create_sequence(&srv, "t1", &body).await;
    let resp = reqwest::Client::new()
        .get(format!("{}/sequences/{id}/preflight", srv.v1_url()))
        .header("X-Tenant-Id", "t1")
        .send()
        .await
        .unwrap();
    let stored_report: Value = resp.json().await.unwrap();
    assert_eq!(draft_report["overall"], stored_report["overall"]);
    assert_eq!(
        check(&draft_report, "handlers_have_workers")["status"],
        check(&stored_report, "handlers_have_workers")["status"]
    );
}

#[tokio::test]
async fn warning_report_is_distinct_from_fail_in_overall() {
    let srv = spawn_test_server().await;
    // Draft-only sub-sequence → warning; nothing failing.
    let target = draft("t1", "child-warn", json!([noop_step("a")]));
    let id = create_sequence(&srv, "t1", &target).await;
    set_status(&srv, id, "draft").await;
    let report =
        preflight(&srv, "t1", &sub_ref_draft("t1", "pf-warnonly", "child-warn", None)).await;
    assert_eq!(report["overall"], "warning");

    // Same reference but unpublished → fail.
    set_status(&srv, id, "unpublished").await;
    let report2 =
        preflight(&srv, "t1", &sub_ref_draft("t1", "pf-failonly", "child-warn", None)).await;
    assert_eq!(report2["overall"], "fail");
}
